"""
Tokenizer module for batch tokenization operations.

Provides AsyncBatchTokenizer for efficient batch processing of chat templates
and utilities for resolving model context lengths.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

__all__ = [
    "AsyncBatchTokenizer",
    "_resolve_model_max_context",
    "HAS_TOKENIZER",
]

try:
    from transformers import AutoConfig, AutoTokenizer

    HAS_TOKENIZER = True
except ImportError:
    HAS_TOKENIZER = False

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))


class AsyncBatchTokenizer:
    def __init__(
        self,
        tokenizer,
        max_batch_size: int = 32,
        batch_wait_timeout_s: float = 0.002,
    ):
        self._tokenizer = tokenizer
        self._max_batch_size = max_batch_size
        self._batch_wait_timeout_s = batch_wait_timeout_s
        self._queue: asyncio.Queue = asyncio.Queue()
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._lock = threading.Lock()
        self._running = True
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        self._task = asyncio.create_task(self._process_loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._executor.shutdown(wait=False)

    async def count_tokens(
        self, messages: list[dict], tools: Optional[list[dict]]
    ) -> int:
        future = asyncio.get_running_loop().create_future()
        await self._queue.put((messages, tools, future))
        return await future

    async def _process_loop(self):
        loop = asyncio.get_running_loop()
        while self._running:
            batch = []
            try:
                item = await asyncio.wait_for(
                    self._queue.get(), timeout=self._batch_wait_timeout_s
                )
                batch.append(item)
            except asyncio.TimeoutError:
                pass

            while len(batch) < self._max_batch_size and not self._queue.empty():
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            if not batch:
                continue

            for messages, tools, future in batch:
                try:
                    count = await loop.run_in_executor(
                        self._executor, self._tokenize, messages, tools
                    )
                    future.set_result(count)
                except Exception as e:
                    future.set_exception(e)

    def _tokenize(self, messages: list[dict], tools: Optional[list[dict]]) -> int:
        with self._lock:
            result = self._tokenizer.apply_chat_template(
                conversation=messages,
                tools=tools,
                tokenize=True,
                add_generation_prompt=True,
            )
            if hasattr(result, "input_ids"):
                ids = result["input_ids"]
                return len(ids[0]) if ids and isinstance(ids[0], list) else len(ids)
            return len(result)


def _resolve_model_max_context(model_id: str, tokenizer: Any) -> int | None:
    max_context = None
    try:
        model_config = AutoConfig.from_pretrained(model_id)
        raw_max_context = getattr(model_config, "max_position_embeddings", None)
        if isinstance(raw_max_context, int) and raw_max_context > 0:
            max_context = raw_max_context
    except Exception as e:
        logger.warning(f"Failed to load model config for {model_id}: {e}")

    if max_context is None:
        tokenizer_max_context = getattr(tokenizer, "model_max_length", None)
        if (
            isinstance(tokenizer_max_context, int)
            and tokenizer_max_context > 0
            and tokenizer_max_context < 10_000_000
        ):
            max_context = tokenizer_max_context
    return max_context
