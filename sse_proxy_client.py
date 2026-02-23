import asyncio
from typing import AsyncGenerator, AsyncIterator, Optional, Tuple, Coroutine, Any, Callable, Union, Awaitable

from http_client import AsyncHttpClient, HttpErrorWithContent


OnSseFrameCallback = Callable[[str, bytes], Union[None, Awaitable[None]]]


class SseProxyClient(AsyncHttpClient):
    @staticmethod
    def _find_sse_boundary(buffer: bytearray) -> Optional[Tuple[int, int]]:
        for i in range(1, len(buffer)):
            if buffer[i] != 10:
                continue

            if buffer[i - 1] == 10:
                return i + 1, 2

            if i >= 3 and buffer[i - 1] == 13 and buffer[i - 2] == 10 and buffer[i - 3] == 13:
                return i + 1, 4

        return None

    @staticmethod
    def _is_comment_only_frame(frame: bytes) -> bool:
        for line in frame.splitlines():
            if not line:
                continue
            if not line.startswith(b":"):
                return False
        return True

    @staticmethod
    def _drain_complete_frames(buffer: bytearray) -> list[bytes]:
        frames: list[bytes] = []
        while True:
            boundary = SseProxyClient._find_sse_boundary(buffer)
            if not boundary:
                break

            boundary_pos, _ = boundary
            frame = bytes(buffer[:boundary_pos])
            if not SseProxyClient._is_comment_only_frame(frame):
                frames.append(frame)
            del buffer[:boundary_pos]

        return frames

    @staticmethod
    def _next_item(iterator: AsyncIterator[bytes]) -> Coroutine[Any, Any, bytes]:
        async def _read() -> bytes:
            return await iterator.__anext__()

        return _read()

    async def _consume_until_done(
        self,
        iterator: AsyncIterator[bytes],
        request_id: str,
        heartbeat_interval: float,
        heartbeat_content: bytes,
        on_frame_callback: Optional[OnSseFrameCallback] = None,
    ) -> AsyncGenerator[bytes, None]:
        next_item_task = asyncio.create_task(self._next_item(iterator))
        buffer = bytearray()

        try:
            while True:
                try:
                    chunk = await asyncio.wait_for(
                        asyncio.shield(next_item_task), timeout=heartbeat_interval
                    )
                except asyncio.TimeoutError:
                    if buffer:
                        continue

                    ctx = self.active_requests.get(request_id)
                    queue = ctx.stream_queue if ctx else None
                    if queue and ctx and ctx.task and ctx.task.done() and queue.empty():
                        try:
                            await next_item_task
                        except Exception:
                            pass
                        break

                    yield heartbeat_content
                    continue

                except HttpErrorWithContent:
                    raise

                except StopAsyncIteration:
                    if buffer and not self._is_comment_only_frame(bytes(buffer)):
                        frame = bytes(buffer)
                        self._trigger_sse_frame_callback(on_frame_callback, request_id, frame)
                        yield frame
                    break

                except Exception:
                    raise

                if chunk:
                    buffer.extend(chunk)

                next_item_task = asyncio.create_task(self._next_item(iterator))

                for frame in self._drain_complete_frames(buffer):
                    if frame:
                        self._trigger_sse_frame_callback(on_frame_callback, request_id, frame)
                        yield frame
        finally:
            if not next_item_task.done():
                next_item_task.cancel()
                try:
                    await next_item_task
                except asyncio.CancelledError:
                    pass

    def _trigger_sse_frame_callback(
        self,
        callback: Optional[OnSseFrameCallback],
        request_id: str,
        frame: bytes,
    ) -> None:
        if not callback:
            return
        if asyncio.iscoroutinefunction(callback):
            asyncio.create_task(callback(request_id, frame))
            return
        callback(request_id, frame)

    async def stream_generator_with_heartbeat(
        self,
        request_id: str,
        heartbeat_interval: float = 5.0,
        heartbeat_content: bytes = b": keep-alive\n\n",
        on_frame_callback: Optional[OnSseFrameCallback] = None,
    ) -> AsyncGenerator[bytes, None]:
        source_generator = self.stream_generator(request_id)
        async for item in self._consume_until_done(
            source_generator,
            request_id,
            heartbeat_interval,
            heartbeat_content,
            on_frame_callback=on_frame_callback,
        ):
            yield item
