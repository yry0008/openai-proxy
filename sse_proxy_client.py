"""SSE 代理客户端 — 在 AsyncHttpClient 基础上增加 SSE 事件拆分、格式修复与错误检测。

功能概览
========

1. **SSE 事件拆分**
   - ``_split_sse_events`` 将字节流按 ``\\n\\n`` 分隔符拆分为完整 SSE 事件。
   - ``stream_generator`` 保证每次 yield 恰好一个完整 SSE 事件（以 ``\\n\\n`` 结尾）。

2. **SSE 格式修复**
   - ``_normalize_sse_event`` 确保 ``data:`` 和 ``event:`` 行冒号后有空格。
   - 修复 ``data:{"error":...}`` → ``data: {"error":...}`` 等格式问题。

3. **SSE 错误检测**
   - ``_detect_sse_error`` 识别两类 SSE error：
     - ``data: {"error": {...}}`` — JSON 数据中包含 ``error`` 字段
     - ``event: error`` — SSE event 类型为 ``error``
   - 返回 ``(is_error, error_data, event_type)`` 三元组。

4. **错误策略配置 (SseErrorConfig)**
   - ``first_packet_swallow`` / ``first_packet_retry`` — 控制首包 error 的吞流和抛出行为。
   - ``mid_stream_swallow`` / ``mid_stream_retry`` — 控制非首包 error 的行为。
   - **注意**: ``_worker()`` 不使用这些配置 — 它总是在首包 error 时 raise
     ``SseFirstPacketError``，由调度层决定是否 fallback。SseErrorConfig
     仅影响 ``SseProxyClient.stream_generator()`` 的行为（供不经 _worker 首包验证的旧路径使用）。

5. **心跳保活**
   - ``stream_generator_with_heartbeat`` 包装 ``stream_generator``，在空闲时
     自动注入 ``: keep-alive\\n\\n`` SSE 注释事件，防止客户端/代理超时断开。

与 http_client.py 的关系
========================
``SseProxyClient`` 继承 ``AsyncHttpClient``，重写 ``stream_generator`` 方法
增加 SSE 感知逻辑。其余功能（连接管理、超时、重试、消费者断开保护等）
完全复用 ``AsyncHttpClient`` 的实现。
"""
import asyncio
import json
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Callable

from http_client import (
    AsyncHttpClient,
    HttpErrorWithContent,
    InterChunkTimeoutError,
)
try:
    from src.base.logging import logger
except ImportError:
    try:
        from loguru import logger
    except ImportError:
        import logging
        logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SseFirstPacketError (moved from http_client.py)
# ---------------------------------------------------------------------------

class SseFirstPacketError(Exception):
    """首个 SSE 事件为 error 时抛出。

    当 wait_for_first_data_validation=True 时，_worker() 在验证首包
    发现 SSE error 事件后抛出此异常，通过 startup_future 传播到
    wait_for_upstream_status()，由调度层决策是否 fallback。

    Attributes:
        error_data: error 事件的 JSON 解析数据（如 {"error": {"message": ...}}）
        event_type: SSE event 类型（如 "error"）
        raw_event: 原始 SSE 事件字节
    """

    def __init__(
        self,
        error_data: dict | None = None,
        event_type: str | None = None,
        raw_event: bytes = b"",
    ):
        self.error_data = error_data
        self.event_type = event_type
        self.raw_event = raw_event
        msg = f"SSE first packet error (event_type={event_type})"
        super().__init__(msg)


# ---------------------------------------------------------------------------
# SSE Error Configuration
# ---------------------------------------------------------------------------

@dataclass
class SseErrorConfig:
    """SSE 流错误处理策略配置。

    控制 ``SseProxyClient.stream_generator()`` 遇到 SSE error 事件时的行为：
    - **首包 error** (is_first_event=True):
      - ``first_packet_swallow=True`` → 吞掉 error 事件，不 yield
      - ``first_packet_retry=True`` → raise ``SseFirstPacketError``，触发调度层 fallback
      - 两者均 False → 原样 yield error 事件
    - **非首包 error** (is_first_event=False):
      - ``mid_stream_swallow=True`` → 吞掉 error 事件，不 yield
      - ``mid_stream_retry=True`` → 仅打 warning 日志（流中 error 不做同路由重试）
      - 两者均 False → 原样 yield error 事件

    **重要**: ``AsyncHttpClient._worker()`` 不使用此配置。当 ``wait_for_first_data_validation=True``
    时，``_worker()`` 总是在首包 error 时 raise ``SseFirstPacketError``，由调度层决策。
    此配置仅对不经过 ``_worker()`` 首包验证的代码路径（如 VlmService 旧路径）有效。
    """
    first_packet_swallow: bool = False   # 首包 error: 是否吞流（不 yield）
    first_packet_retry: bool = False     # 首包 error: 是否 raise SseFirstPacketError
    mid_stream_swallow: bool = False     # 非首包 error: 是否吞流（不 yield）
    mid_stream_retry: bool = False       # 非首包 error: 仅打 warning（流中不做同路由重试）


# ---------------------------------------------------------------------------
# SSE Format Normalization
# ---------------------------------------------------------------------------

def _normalize_sse_event(event_bytes: bytes) -> bytes:
    """Ensure every ``data:`` and ``event:`` line has a space after the colon.

    Handles both ``data:{"error":...}`` → ``data: {"error":...}``
    and ``event:error`` → ``event: error``.
    Only processes lines that start with these prefixes but lack the space.
    """
    lines = event_bytes.split(b"\n")
    normalized = []
    for line in lines:
        if line.startswith(b"data:") and not line.startswith(b"data: "):
            line = b"data: " + line[len(b"data:"):]
        elif line.startswith(b"event:") and not line.startswith(b"event: "):
            line = b"event: " + line[len(b"event:"):]
        normalized.append(line)
    return b"\n".join(normalized)


# ---------------------------------------------------------------------------
# SSE Error Detection
# ---------------------------------------------------------------------------

def _detect_sse_error(event_bytes: bytes) -> tuple[bool, dict | None, str | None]:
    """Check if a complete SSE event is an error event.

    Returns:
        (is_error, error_data, event_type)
        - is_error: True if the event represents an error
        - error_data: Parsed JSON dict if data line contains error, else None
        - event_type: "error" if event: error line present, else None
    """
    event_type = None
    error_data = None

    for line in event_bytes.split(b"\n"):
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith(b"event:"):
            event_value = stripped[len(b"event:"):].strip()
            if event_value == b"error":
                event_type = "error"
        elif stripped.startswith(b"data:"):
            data_content = stripped[len(b"data:"):].strip()
            if data_content and data_content != b"[DONE]":
                try:
                    parsed = json.loads(data_content)
                    if isinstance(parsed, dict) and "error" in parsed:
                        error_data = parsed
                except (json.JSONDecodeError, ValueError):
                    pass

    return (event_type is not None or error_data is not None), error_data, event_type


# ---------------------------------------------------------------------------
# SSE Event Splitting
# ---------------------------------------------------------------------------

def _split_sse_events(buffer: bytearray) -> list[bytes]:
    """Split a byte buffer into complete SSE events (delimited by ``\\n\\n``).

    Returns complete events and leaves incomplete data in the buffer.
    """
    events = []
    while b"\n\n" in buffer:
        idx = buffer.index(b"\n\n") + 2
        events.append(bytes(buffer[:idx]))
        del buffer[:idx]
    return events


def _split_first_sse_event(buffer: bytearray) -> tuple[bytes, bytes]:
    """Split the first complete SSE event from a buffer.

    Returns (first_event_bytes, remaining_bytes). If no ``\\n\\n`` boundary
    is found, the entire buffer is treated as the first event.
    """
    if b"\n\n" in buffer:
        idx = buffer.index(b"\n\n") + 2
        return bytes(buffer[:idx]), bytes(buffer[idx:])
    return bytes(buffer), b""


class SseProxyClient(AsyncHttpClient):
    """SSE 感知的 HTTP 客户端，在 AsyncHttpClient 基础上增加：

    1. **事件拆分** — 每次 yield 恰好一个完整 SSE 事件（``\\n\\n`` 分隔）
    2. **格式修复** — ``data:`` / ``event:`` 后自动补空格
    3. **错误检测** — 识别 ``data: {"error":...}`` 和 ``event: error`` 两类 error
    4. **错误策略** — 通过 ``SseErrorConfig`` 控制首包/非首包 error 的吞流/抛出行为
    5. **SSE 首包验证** — 重写 ``_validate_first_chunk`` 缓冲首个 SSE 事件并验证非 error
    """

    # SseFirstPacketError 遇到即停止重试，由调度层决定是否 fallback
    stop_retry_exceptions = (SseFirstPacketError,)

    async def _validate_first_chunk(
        self,
        first_chunk: bytes | None,
        response_status: int,
        ctx: 'RequestContext',
        chunk_iter,
        req: 'RequestWrapper',
        start_time: float,
    ) -> bytes | None:
        """SSE 首包验证 — 缓冲直到完整 SSE 事件到达，验证非 error 后才 resolve startup_future。"""
        if first_chunk is None:
            return None

        _ict = req.inter_chunk_timeout
        sse_buffer = bytearray(first_chunk)
        while b"\n\n" not in sse_buffer:
            try:
                if _ict is not None:
                    more = await asyncio.wait_for(chunk_iter.__anext__(), timeout=_ict)
                else:
                    more = await chunk_iter.__anext__()
                sse_buffer.extend(more)
            except StopAsyncIteration:
                break
            except asyncio.TimeoutError:
                raise InterChunkTimeoutError(
                    f"inter-chunk idle timeout ({_ict}s) exceeded during first-event buffering req_id={ctx.request_id}"
                )

        first_event, remaining_buffer = _split_first_sse_event(sse_buffer)
        first_event = _normalize_sse_event(first_event)
        is_error, error_data, event_type = _detect_sse_error(first_event)

        if is_error:
            exc = SseFirstPacketError(
                error_data=error_data,
                event_type=event_type,
                raw_event=first_event,
            )
            if not ctx.startup_future.done():
                ctx.startup_future.set_exception(exc)
            raise exc

        if not ctx.startup_future.done():
            ctx.startup_future.set_result(response_status)

        return bytes(sse_buffer)

    async def _handle_empty_body(
        self,
        response_status: int,
        ctx: 'RequestContext',
        req: 'RequestWrapper',
    ) -> None:
        """SSE 空 body 处理 — 200+空 body=成功（无 SSE 流）。"""
        if not ctx.startup_future.done():
            ctx.startup_future.set_result(response_status)

    async def stream_generator(
        self,
        request_id: str,
        sse_error_config: SseErrorConfig | None = None,
        chunk_hook: Callable[[bytes], bytes] | None = None,
    ) -> AsyncGenerator[bytes, None]:
        """Yield 完整、格式修复后的 SSE 事件，带错误处理。

        与 ``AsyncHttpClient.stream_generator()`` 的差异:
        - 每次 yield 恰好一个完整 SSE 事件（``\\n\\n`` 分隔），而非原始 chunk
        - 自动修复 ``data:`` / ``event:`` 后缺空格的格式问题
        - 根据 ``SseErrorConfig`` 处理 SSE error 事件（吞流/抛出/原样输出）

        Args:
            request_id: 请求唯一 ID
            sse_error_config: 错误策略配置，为 None 时所有 SSE error 事件原样输出
            chunk_hook: 每个 SSE 事件 yield 前的变换钩子。
                接收原始事件字节，返回修改后的事件字节。
                默认为 None（直接返回原始 chunk，不修改）。

        Yields:
            bytes: 完整 SSE 事件字节（以 ``\\n\\n`` 结尾）

        Raises:
            SseFirstPacketError: 首包 error 且 ``first_packet_retry=True``
        """
        ctx = self.active_requests.get(request_id)
        if not ctx or not ctx.wrapper.is_stream:
            raise ValueError("Invalid ID")

        config = sse_error_config
        _hook = chunk_hook if chunk_hook is not None else lambda c: c
        raw_stream = super().stream_generator(request_id)
        buffer = bytearray()
        is_first_event = True

        async for raw_chunk in raw_stream:
            buffer.extend(raw_chunk)
            for event_bytes in _split_sse_events(buffer):
                event_bytes = _normalize_sse_event(event_bytes)
                is_error, error_data, event_type = _detect_sse_error(event_bytes)

                if is_error and config is not None:
                    if is_first_event:
                        if config.first_packet_retry:
                            raise SseFirstPacketError(
                                error_data=error_data,
                                event_type=event_type,
                                raw_event=event_bytes,
                            )
                        if config.first_packet_swallow:
                            continue
                    else:
                        if config.mid_stream_retry:
                            logger.warning(
                                f"mid-stream SSE error with retry=True, but retry "
                                f"is limited to same endpoint: {event_bytes[:200]!r}"
                            )
                        if config.mid_stream_swallow:
                            continue

                is_first_event = False
                yield _hook(event_bytes)

        # Handle remaining buffer (last event without trailing \n\n)
        remaining = bytes(buffer).strip()
        if remaining:
            event_bytes = _normalize_sse_event(bytes(buffer))
            is_error, error_data, event_type = _detect_sse_error(event_bytes)
            if is_error and config is not None:
                if is_first_event:
                    if config.first_packet_retry:
                        raise SseFirstPacketError(
                            error_data=error_data,
                            event_type=event_type,
                            raw_event=event_bytes,
                        )
                    if config.first_packet_swallow:
                        return
                else:
                    if config.mid_stream_swallow:
                        return
            yield _hook(event_bytes)

    async def stream_generator_with_heartbeat(
        self,
        request_id: str,
        heartbeat_interval: float = 5.0,
        heartbeat_content: bytes = b": keep-alive\n\n",
        sse_error_config: SseErrorConfig | None = None,
        chunk_hook: Callable[[bytes], bytes] | None = None,
    ) -> AsyncGenerator[bytes, None]:
        """包装 ``stream_generator``，在空闲时注入心跳包防止超时断开。

        当 ``stream_generator`` 在 ``heartbeat_interval`` 秒内未产出数据时，
        自动 yield ``heartbeat_content``（默认为 SSE 注释 ``: keep-alive\\n\\n``）。

        Args:
            request_id: 请求唯一 ID
            heartbeat_interval: 心跳间隔（秒），默认 5.0
            heartbeat_content: 心跳包内容，默认 ``b": keep-alive\\n\\n"``
            sse_error_config: 透传给 stream_generator 的错误策略配置
            chunk_hook: 透传给 stream_generator 的 chunk 变换钩子

        Yields:
            bytes: 完整 SSE 事件或心跳包
        """
        source_generator = self.stream_generator(request_id, sse_error_config=sse_error_config, chunk_hook=chunk_hook)
        iterator = source_generator.__aiter__()
        next_item_task = asyncio.create_task(iterator.__anext__())

        try:
            while True:
                try:
                    chunk = await asyncio.wait_for(
                        asyncio.shield(next_item_task),
                        timeout=heartbeat_interval,
                    )
                    yield chunk
                    next_item_task = asyncio.create_task(iterator.__anext__())

                except asyncio.TimeoutError:
                    ctx = self.active_requests.get(request_id)
                    if next_item_task.done():
                        try:
                            result = await next_item_task
                            yield result
                            next_item_task = asyncio.create_task(iterator.__anext__())
                        except StopAsyncIteration:
                            break
                        except Exception as e:
                            raise e
                    else:
                        if ctx and ctx.task.done() and ctx.stream_queue.empty():
                            try:
                                await next_item_task
                            except Exception:
                                pass
                            break
                        yield heartbeat_content

                except StopAsyncIteration:
                    break
                except HttpErrorWithContent:
                    raise
                except SseFirstPacketError:
                    raise

        finally:
            if next_item_task and not next_item_task.done():
                next_item_task.cancel()
                try:
                    await next_item_task
                except asyncio.CancelledError:
                    pass
