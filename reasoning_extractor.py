"""
reasoning_extractor.py

Extracts <think>...</think> tags from chat completion responses and moves the content
to a separate reasoning_content field, following the DeepSeek-R1 API format.
"""

import logging
import orjson
from typing import AsyncGenerator, Optional, Tuple, List

logger = logging.getLogger(__name__)


class ReasoningExtractor:
    """Stateful processor for extracting <think> tags across streaming chunks."""

    def __init__(self):
        self.inside_think = False
        self.accumulated_reasoning = []

    def process_content_for_streaming(
        self, content: str
    ) -> List[Tuple[Optional[str], Optional[str], Optional[str]]]:
        """Splits output into three parts: before <think>, thinking content, and after </think>."""
        if not content:
            return [(None, None, None)]

        if not self.inside_think:
            think_start = content.find("<think>")
            if think_start == -1:
                return [(content, None, None)]

            text_before = content[:think_start]
            self.inside_think = True
            remaining = content[think_start + len("<think>") :]

            think_end = remaining.find("</think>")
            if think_end == -1:
                reasoning = remaining
                if reasoning:
                    self.accumulated_reasoning.append(reasoning)
                return [
                    (
                        text_before if text_before else None,
                        reasoning if reasoning else None,
                        None,
                    )
                ]

            reasoning = remaining[:think_end]
            if reasoning:
                self.accumulated_reasoning.append(reasoning)
            self.inside_think = False
            text_after = remaining[think_end + len("</think>") :]
            return [
                (
                    text_before if text_before else None,
                    reasoning if reasoning else None,
                    text_after if text_after else None,
                )
            ]
        else:
            think_end = content.find("</think>")
            if think_end == -1:
                reasoning = content
                if reasoning:
                    self.accumulated_reasoning.append(reasoning)
                return [(None, reasoning if reasoning else None, None)]

            reasoning = content[:think_end]
            if reasoning:
                self.accumulated_reasoning.append(reasoning)
            self.inside_think = False
            text_after = content[think_end + len("</think>") :]
            return [
                (
                    None,
                    reasoning if reasoning else None,
                    text_after if text_after else None,
                )
            ]

    def process_content(self, content: str) -> Tuple[str, Optional[str]]:
        """Process content and extract <think> tags for non-streaming mode."""
        if not content:
            return (content, None)

        cleaned_parts = []
        reasoning_parts = []
        pos = 0

        while pos < len(content):
            if not self.inside_think:
                think_start = content.find("<think>", pos)
                if think_start == -1:
                    cleaned_parts.append(content[pos:])
                    break
                else:
                    cleaned_parts.append(content[pos:think_start])
                    self.inside_think = True
                    pos = think_start + len("<think>")
            else:
                think_end = content.find("</think>", pos)
                if think_end == -1:
                    reasoning_text = content[pos:]
                    if reasoning_text:
                        self.accumulated_reasoning.append(reasoning_text)
                        reasoning_parts.append(reasoning_text)
                    break
                else:
                    reasoning_text = content[pos:think_end]
                    if reasoning_text:
                        self.accumulated_reasoning.append(reasoning_text)
                        reasoning_parts.append(reasoning_text)
                    self.inside_think = False
                    pos = think_end + len("</think>")

        cleaned_content = "".join(cleaned_parts)
        reasoning_chunk = "".join(reasoning_parts) if reasoning_parts else None
        return (cleaned_content, reasoning_chunk)

    def get_merged_reasoning(self) -> Optional[str]:
        """Returns all accumulated reasoning merged together."""
        return (
            "".join(self.accumulated_reasoning) if self.accumulated_reasoning else None
        )


def merge_reasoning_to_content(messages: list) -> list:
    """Merge reasoning_content back into content as <think> tags."""
    for message in messages:
        if not isinstance(message, dict):
            continue

        reasoning = message.get("reasoning_content")
        content = message.get("content", "")

        if reasoning:
            message["content"] = f"<think>{reasoning}</think>{content}"
            del message["reasoning_content"]

    return messages


async def transform_sse_stream(
    source_generator: AsyncGenerator[bytes, None], is_streaming: bool
) -> AsyncGenerator[bytes, None]:
    """Transform SSE stream to extract <think> tags into reasoning_content."""
    extractor = ReasoningExtractor()

    try:
        if is_streaming:
            async for chunk in source_generator:
                try:
                    text = chunk.decode("utf-8", errors="replace")
                except Exception as e:
                    logger.warning(f"Failed to decode chunk: {e}")
                    yield chunk
                    continue

                if not text.startswith("data: ") or "data: [DONE]" in text:
                    yield chunk
                    continue

                json_str = text[6:].strip()
                if not json_str:
                    yield chunk
                    continue

                try:
                    data = orjson.loads(json_str)
                except Exception as e:
                    logger.warning(f"Failed to parse JSON from SSE chunk: {e}")
                    yield chunk
                    continue

                choices = data.get("choices", [])
                if not choices:
                    yield chunk
                    continue

                choice = choices[0]
                delta = choice.get("delta", {})
                content = delta.get("content")

                if content is None or content == "":
                    yield chunk
                    continue

                parts = extractor.process_content_for_streaming(content)

                for content_before, reasoning, content_after in parts:
                    if content_before:
                        part_data = orjson.loads(orjson.dumps(data))
                        part_delta = part_data["choices"][0]["delta"]
                        part_delta["content"] = content_before
                        part_delta.pop("reasoning_content", None)
                        yield b"data: " + orjson.dumps(part_data) + b"\n\n"

                    if reasoning:
                        part_data = orjson.loads(orjson.dumps(data))
                        part_delta = part_data["choices"][0]["delta"]
                        part_delta["content"] = None # 显示None，即使没有数据也要 None
                        part_delta["reasoning_content"] = reasoning
                        yield b"data: " + orjson.dumps(part_data) + b"\n\n"

                    if content_after:
                        part_data = orjson.loads(orjson.dumps(data))
                        part_delta = part_data["choices"][0]["delta"]
                        part_delta["content"] = content_after
                        part_delta.pop("reasoning_content", None)
                        yield b"data: " + orjson.dumps(part_data) + b"\n\n"

        else:
            chunks = []
            async for chunk in source_generator:
                chunks.append(chunk)

            full_response = b"".join(chunks)
            try:
                text = full_response.decode("utf-8", errors="replace")
                data = orjson.loads(text)
            except Exception as e:
                logger.warning(f"Failed to parse non-streaming JSON: {e}")
                yield full_response
                return

            choices = data.get("choices", [])
            if not choices:
                yield full_response
                return

            choice = choices[0]
            message = choice.get("message", {})
            content = message.get("content")

            if content is None or content == "":
                yield full_response
                return

            cleaned_content, reasoning_content = extractor.process_content(content)
            message["content"] = cleaned_content
            if reasoning_content:
                message["reasoning_content"] = reasoning_content

            yield orjson.dumps(data)

    except Exception as e:
        logger.error(f"Error in transform_sse_stream: {e}", exc_info=True)
        raise


async def transform_non_sse_response(
    response_bytes: bytes,
) -> bytes:
    """Transform non-SSE response to extract <think> tags into reasoning_content."""
    extractor = ReasoningExtractor()

    try:
        text = response_bytes.decode("utf-8", errors="replace")
        data = orjson.loads(text)
    except Exception as e:
        logger.warning(f"Failed to parse non-SSE JSON: {e}")
        return response_bytes

    choices = data.get("choices", [])
    if not choices:
        return response_bytes

    choice = choices[0]
    message = choice.get("message", {})
    content = message.get("content")

    if content is None or content == "":
        return response_bytes

    cleaned_content, reasoning_content = extractor.process_content(content)
    message["content"] = cleaned_content
    if reasoning_content:
        message["reasoning_content"] = reasoning_content

    return orjson.dumps(data)