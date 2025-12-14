import asyncio
from typing import Dict, Any, Optional, Union, Callable, Awaitable, AsyncGenerator
from http_client import AsyncHttpClient, HttpErrorWithContent, RequestWrapper, RequestResult, RequestStatus, CancelBehavior
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SseProxyClient(AsyncHttpClient):
    """支持心跳保活的 SSE 客户端"""
    async def stream_generator_with_heartbeat(self, request_id: str, heartbeat_interval: float = 5.0, heartbeat_content: bytes = b": keep-alive\n\n") -> AsyncGenerator[bytes, None]:
        source_generator = super().stream_generator(request_id)
        iterator = source_generator.__aiter__()
        
        next_item_task = asyncio.create_task(iterator.__anext__())

        try:
            while True:
                try:
                    chunk = await asyncio.wait_for(asyncio.shield(next_item_task), timeout=heartbeat_interval)
                    yield chunk
                    next_item_task = asyncio.create_task(iterator.__anext__())

                except asyncio.TimeoutError:
                    ctx = self.active_requests.get(request_id)
                    # 检查任务是否已完成 (虽然超时了但数据可能刚好到，或者出错了)
                    if next_item_task.done():
                        try: 
                            result = await next_item_task 
                            yield result 
                            next_item_task = asyncio.create_task(iterator.__anext__())
                        except StopAsyncIteration: break
                        except Exception as e: raise e 
                    else:
                        # 真正的超时，且任务仍在运行 -> 发送心跳
                        # 检查 Worker 是否已死，避免死循环
                        if ctx and ctx.task.done() and ctx.stream_queue.empty():
                            try: await next_item_task
                            except Exception: pass
                            break
                        yield heartbeat_content

                except StopAsyncIteration: break
                except HttpErrorWithContent: raise
                
        finally:
            # [FIX-2] 清理 Pending Task
            if next_item_task and not next_item_task.done():
                next_item_task.cancel()
                try: await next_item_task
                except asyncio.CancelledError: pass