from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from tokenizer import AsyncBatchTokenizer
from reasoning import _get_reasoning_parser
from multimedia import (
    _extract_multimedia_info,
    _strip_multimedia_from_messages,
    _estimate_multimedia_tokens,
    _resolve_image_urls,
)
from utils import (
    _transform_messages_for_template,
    _transform_tools_for_template,
    _get_requested_output_tokens,
    _build_context_length_error,
)

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class TokenGuardResult:
    input_tokens: int
    error: Optional[dict] = None


class TokenGuard:
    def __init__(
        self,
        batch_tokenizer: AsyncBatchTokenizer | None,
        model_max_context: int | None,
        reasoning_type: str,
        reject_multimedia: bool,
        vl_config: dict,
        aiohttp_session: aiohttp.ClientSession | None = None,
    ):
        self._batch_tokenizer = batch_tokenizer
        self._model_max_context = model_max_context
        self._reasoning_parser = _get_reasoning_parser(reasoning_type) if reasoning_type else None
        self._reject_multimedia = reject_multimedia
        self._vl_config = vl_config
        self._vl_strategy = vl_config.get("strategy", "")
        self._session = aiohttp_session

    @property
    def batch_tokenizer(self) -> AsyncBatchTokenizer | None:
        return self._batch_tokenizer

    async def check(self, body: dict, body_bytes: bytes) -> TokenGuardResult:
        chat_flag = "messages" in body

        if chat_flag and self._session:
            messages_list = body.get("messages") or []
            body["messages"] = await _resolve_image_urls(self._session, messages_list)

        if self._reject_multimedia and chat_flag:
            multimedia_items = _extract_multimedia_info(body["messages"])
            if multimedia_items:
                logger.info("Rejected request: multimedia content detected (%d item(s))", len(multimedia_items))
                return TokenGuardResult(
                    input_tokens=0,
                    error={
                        "error": {
                            "message": "Multimedia content (images, videos, or audio) is not supported by this model.",
                            "type": "invalid_request_error",
                        }
                    },
                )

        input_tokens = await self._calculate_input_tokens(body, body_bytes, chat_flag)

        logger.info(
            "Request input_tokens=%d, requested_output_tokens=%s, stream=%s",
            input_tokens,
            body.get("max_tokens") or body.get("max_completion_tokens", 16),
            body.get("stream", False),
        )

        error = self._check_context_length(input_tokens, body)
        return TokenGuardResult(input_tokens=input_tokens, error=error)

    async def _calculate_input_tokens(self, body: dict, body_bytes: bytes, chat_flag: bool) -> int:
        if not chat_flag or self._batch_tokenizer is None:
            return len(body_bytes) // 4

        multimedia_items = _extract_multimedia_info(body.get("messages") or [])
        messages_for_counting = _strip_multimedia_from_messages(body.get("messages") or [])
        messages = _transform_messages_for_template(messages_for_counting)

        if self._reasoning_parser is not None:
            messages = self._reasoning_parser(messages)

        tools = _transform_tools_for_template(body.get("tools"))
        text_tokens = await self._batch_tokenizer.count_tokens(messages, tools)

        if multimedia_items and self._vl_strategy and self._vl_strategy not in ("none", ""):
            multimedia_tokens = _estimate_multimedia_tokens(multimedia_items, self._vl_config)
            input_tokens = text_tokens + multimedia_tokens
            logger.info(
                "VL token estimation: strategy=%s, text_tokens=%d, multimedia_tokens=%d, total=%d",
                self._vl_strategy, text_tokens, multimedia_tokens, input_tokens,
            )
        else:
            input_tokens = text_tokens

        return input_tokens

    def _check_context_length(self, input_tokens: int, body: dict) -> Optional[dict]:
        if self._model_max_context is None:
            return None

        requested_output_tokens = _get_requested_output_tokens(body)
        total_tokens = input_tokens + requested_output_tokens
        if total_tokens > self._model_max_context:
            return {
                "error": {
                    "message": _build_context_length_error(
                        self._model_max_context, requested_output_tokens, input_tokens,
                    ),
                    "type": "invalid_request_error",
                }
            }
        return None
