"""
reasoning_extractor.py

Extracts <think>...</think> tags from chat completion responses and moves the content
to a separate reasoning_content field, following the DeepSeek-R1 API format.

Note: Assumes <think> and </think> tags are always complete within a single chunk,
but the content between tags may span multiple chunks.
"""

import logging
import orjson
from typing import AsyncGenerator, Optional, Tuple, List

logger = logging.getLogger(__name__)


class ReasoningExtractor:
    """
    Stateful processor for extracting <think> tags across streaming chunks.

    Handles content that spans multiple chunks while tags themselves are complete.
    """

    def __init__(self):
        self.inside_think = False  # Track if currently inside <think> tags
        self.accumulated_reasoning = []  # Collect reasoning content across chunks

    def process_content_for_streaming(self, content: str) -> List[Tuple[Optional[str], Optional[str]]]:
        """
        Process content and extract <think> tags for streaming mode.
        Splits output into multiple parts when tags are encountered.

        Args:
            content: Text content that may contain <think> or </think> tags

        Returns:
            List of (content_part, reasoning_part) tuples.
            Each tuple represents a separate chunk to be sent.
        """
        if not content:
            return [(content, None)]

        results = []
        pos = 0

        while pos < len(content):
            if not self.inside_think:
                # Look for <think> opening tag
                think_start = content.find('<think>', pos)
                if think_start == -1:
                    # No opening tag, rest is normal content
                    text = content[pos:]
                    if text:
                        results.append((text, None))
                    break
                else:
                    # Found opening tag
                    # Add content before tag as separate part
                    text_before = content[pos:think_start]
                    if text_before:
                        results.append((text_before, None))

                    # Enter thinking mode
                    self.inside_think = True
                    pos = think_start + len('<think>')
            else:
                # Inside <think>, look for </think> closing tag
                think_end = content.find('</think>', pos)
                if think_end == -1:
                    # No closing tag in this chunk, accumulate all remaining as reasoning
                    reasoning_text = content[pos:]
                    if reasoning_text:
                        self.accumulated_reasoning.append(reasoning_text)
                        results.append((None, reasoning_text))
                    break
                else:
                    # Found closing tag
                    # Extract reasoning content up to closing tag
                    reasoning_text = content[pos:think_end]
                    if reasoning_text:
                        self.accumulated_reasoning.append(reasoning_text)
                        results.append((None, reasoning_text))

                    # Exit thinking mode
                    self.inside_think = False
                    pos = think_end + len('</think>')

        return results if results else [(None, None)]

    def process_content(self, content: str) -> Tuple[str, Optional[str]]:
        """
        Process content and extract <think> tags.

        Args:
            content: Text content that may contain <think> or </think> tags

        Returns:
            Tuple of (cleaned_content, reasoning_chunk)
            - cleaned_content: Content with <think>...</think> removed
            - reasoning_chunk: All reasoning extracted from this chunk (None if not found)
        """
        if not content:
            return (content, None)

        cleaned_parts = []
        reasoning_parts = []  # Collect all reasoning from this chunk
        pos = 0

        while pos < len(content):
            if not self.inside_think:
                # Look for <think> opening tag
                think_start = content.find('<think>', pos)
                if think_start == -1:
                    # No opening tag, rest is normal content
                    cleaned_parts.append(content[pos:])
                    break
                else:
                    # Found opening tag
                    # Add content before tag to cleaned
                    cleaned_parts.append(content[pos:think_start])
                    # Enter thinking mode
                    self.inside_think = True
                    pos = think_start + len('<think>')
            else:
                # Inside <think>, look for </think> closing tag
                think_end = content.find('</think>', pos)
                if think_end == -1:
                    # No closing tag in this chunk, accumulate all remaining as reasoning
                    reasoning_text = content[pos:]
                    if reasoning_text:
                        self.accumulated_reasoning.append(reasoning_text)
                        reasoning_parts.append(reasoning_text)
                    break
                else:
                    # Found closing tag
                    # Extract reasoning content up to closing tag
                    reasoning_text = content[pos:think_end]
                    if reasoning_text:  # Only add non-empty reasoning
                        self.accumulated_reasoning.append(reasoning_text)
                        reasoning_parts.append(reasoning_text)
                    # Exit thinking mode
                    self.inside_think = False
                    pos = think_end + len('</think>')

        cleaned_content = ''.join(cleaned_parts)
        reasoning_chunk = ''.join(reasoning_parts) if reasoning_parts else None
        return (cleaned_content, reasoning_chunk)

    def get_merged_reasoning(self) -> Optional[str]:
        """
        Returns all accumulated reasoning merged together.

        Returns:
            Complete reasoning content, or None if no reasoning was extracted
        """
        if not self.accumulated_reasoning:
            return None
        return ''.join(self.accumulated_reasoning)


def merge_reasoning_to_content(messages: list) -> list:
    """
    Merge reasoning_content back into content as <think> tags.
    This is the inverse operation of extraction.

    Args:
        messages: List of message objects that may contain reasoning_content

    Returns:
        Modified messages with reasoning_content merged into content
    """
    for message in messages:
        if not isinstance(message, dict):
            continue

        reasoning = message.get('reasoning_content')
        content = message.get('content', '')

        if reasoning:
            # Prepend <think> tags to content
            merged_content = f'<think>{reasoning}</think>{content}'
            message['content'] = merged_content

            # Remove reasoning_content field
            del message['reasoning_content']

    return messages


async def transform_sse_stream(
    source_generator: AsyncGenerator[bytes, None],
    is_streaming: bool
) -> AsyncGenerator[bytes, None]:
    """
    Transform SSE stream to extract <think> tags into reasoning_content.

    For streaming: Yields modified SSE chunks with reasoning_content in delta
    For non-streaming: Accumulates chunks, processes complete JSON, yields result

    Args:
        source_generator: Raw bytes from upstream API
        is_streaming: True for streaming responses, False for non-streaming

    Yields:
        Transformed bytes with <think> content moved to reasoning_content field
    """
    extractor = ReasoningExtractor()

    try:
        if is_streaming:
            # Streaming mode: process SSE chunks
            async for chunk in source_generator:
                # Decode chunk
                try:
                    text = chunk.decode('utf-8', errors='replace')
                except Exception as e:
                    logger.warning(f"Failed to decode chunk: {e}")
                    yield chunk  # Pass through on decode error
                    continue

                # Handle non-SSE data (heartbeats, etc.)
                if not text.startswith('data: '):
                    yield chunk
                    continue

                # Handle [DONE] sentinel
                if 'data: [DONE]' in text:
                    yield chunk
                    continue

                # Extract JSON from SSE data line
                json_str = text[6:].strip()  # Remove 'data: ' prefix
                if not json_str:
                    yield chunk
                    continue

                # Parse JSON
                try:
                    data = orjson.loads(json_str)
                except Exception as e:
                    logger.warning(f"Failed to parse JSON from SSE chunk: {e}")
                    yield chunk  # Pass through on parse error
                    continue

                # Extract content from appropriate location
                choices = data.get('choices', [])
                if not choices:
                    yield chunk  # No choices, pass through
                    continue

                choice = choices[0]
                delta = choice.get('delta', {})
                content = delta.get('content')

                # If no content, pass through unchanged
                if content is None or content == "":
                    yield chunk
                    continue

                # Process <think> tags and split into multiple chunks
                parts = extractor.process_content_for_streaming(content)

                # Generate separate SSE chunks for each part
                for content_part, reasoning_part in parts:
                    # Create a copy of the data for this part
                    part_data = orjson.loads(orjson.dumps(data))  # Deep copy
                    part_delta = part_data['choices'][0]['delta']

                    # Set content and reasoning_content for this part
                    if content_part is not None and content_part != "":
                        part_delta['content'] = content_part
                    else:
                        part_delta.pop('content', None)  # Remove content if empty

                    if reasoning_part:
                        part_delta['reasoning_content'] = reasoning_part
                    else:
                        part_delta.pop('reasoning_content', None)

                    # Re-serialize to SSE format
                    new_json = orjson.dumps(part_data)
                    new_chunk = b'data: ' + new_json + b'\n\n'

                    yield new_chunk

        else:
            # Non-streaming mode: accumulate all chunks, process complete JSON
            chunks = []
            async for chunk in source_generator:
                chunks.append(chunk)

            # Combine all chunks into complete response
            full_response = b''.join(chunks)

            # Decode and parse JSON
            try:
                text = full_response.decode('utf-8', errors='replace')
                data = orjson.loads(text)
            except Exception as e:
                logger.warning(f"Failed to parse non-streaming JSON: {e}")
                yield full_response  # Pass through on parse error
                return

            # Extract content from message
            choices = data.get('choices', [])
            if not choices:
                yield full_response  # No choices, pass through
                return

            choice = choices[0]
            message = choice.get('message', {})
            content = message.get('content')

            # If no content, pass through unchanged
            if content is None or content == "":
                yield full_response
                return

            # Process <think> tags
            cleaned_content, reasoning_content = extractor.process_content(content)

            # Modify JSON
            message['content'] = cleaned_content

            # Add reasoning_content field if reasoning was extracted
            if reasoning_content:
                message['reasoning_content'] = reasoning_content

            # Re-serialize and yield
            new_response = orjson.dumps(data)
            yield new_response

    except Exception as e:
        logger.error(f"Error in transform_sse_stream: {e}", exc_info=True)
        raise
