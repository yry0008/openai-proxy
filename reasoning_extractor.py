"""
reasoning_extractor.py

Merges reasoning_content from chat completion responses into the content field
wrapped in <think>...</think> tags, following the DeepSeek-R1 "thinking" format.
"""

import logging
import orjson
from typing import AsyncGenerator, Optional, Tuple, List

logger = logging.getLogger(__name__)

def merge_reasoning_to_content(messages: list) -> list:
    """Merge reasoning_content back into content as <think> tags for input messages."""
    # This function remains unchanged as it already implements the "thinking format" for inputs.
    for message in messages:
        if not isinstance(message, dict):
            continue

        reasoning = message.get("reasoning_content")
        content = message.get("content", "")

        if reasoning:
            # Handle case where content might be None
            if content is None:
                content = ""
            message["content"] = f"<think>{reasoning}</think>{content}"
            del message["reasoning_content"]

    return messages


async def transform_sse_stream(
    source_generator: AsyncGenerator[bytes, None], is_streaming: bool
) -> AsyncGenerator[bytes, None]:
    """
    Transform SSE stream to merge reasoning_content into content wrapped in <think> tags.
    """
    # Compatibility handling if called with is_streaming=False (though main.py uses True)
    if not is_streaming:
        chunks = []
        async for chunk in source_generator:
            chunks.append(chunk)
        full_body = b"".join(chunks)
        yield await transform_non_sse_response(full_body)
        return

    has_started_think = False
    has_ended_think = False

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
        
        reasoning = delta.get("reasoning_content")
        content = delta.get("content")
        
        new_content_fragment = ""

        # 1. Handle Reasoning: If reasoning content is present, append it.
        #    If it's the first time we see reasoning, prepend <think>.
        if reasoning:
            if not has_started_think:
                new_content_fragment += "<think>"
                has_started_think = True
            new_content_fragment += reasoning
        
        # 2. Handle Content: If actual content appears, it signals the end of reasoning.
        #    We assume 'content' being non-None (even empty string) marks the transition 
        #    or the body part.
        if content is not None:
            if has_started_think and not has_ended_think:
                new_content_fragment += "</think>"
                has_ended_think = True
            new_content_fragment += content
            
        # 3. Update the delta with the merged content
        if new_content_fragment:
            delta["content"] = new_content_fragment
        
        # Always remove reasoning_content so the client doesn't see the separate field
        if "reasoning_content" in delta:
            del delta["reasoning_content"]
            
        # Re-serialize and yield
        data["choices"][0]["delta"] = delta
        yield b"data: " + orjson.dumps(data) + b"\n\n"


async def transform_non_sse_response(
    response_bytes: bytes,
) -> bytes:
    """Transform non-SSE response to merge reasoning_content into content as <think> tags."""
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
    
    reasoning = message.get("reasoning_content")
    content = message.get("content")

    if reasoning:
        if content is None:
            content = ""
        # Merge logic: <think>...</think> + content
        new_content = f"<think>{reasoning}</think>{content}"
        
        message["content"] = new_content
        del message["reasoning_content"]
    
    return orjson.dumps(data)