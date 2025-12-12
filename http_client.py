import asyncio
import uuid
import aiohttp
import logging
from typing import Dict, Any, Optional, Union, Callable, Awaitable, AsyncGenerator
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import time
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import orjson

# ==========================================
# PART 1: AsyncHttpClient 实现 (复制自上一轮最终版本)
# ==========================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 类型定义 ---
class CancelBehavior(Enum):
    DO_NOTHING = 0
    TRIGGER_SUCCESS = 1
    TRIGGER_FAILURE = 2

class RequestStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class RequestResult:
    request_id: str
    status: RequestStatus
    content: Union[str, bytes, None]
    http_code: Optional[int] = None
    error: Optional[Exception] = None

# 回调函数签名
OnStreamStartCallback = Callable[[str, float, Any], Union[None, Awaitable[None]]]
CallbackType = Callable[['RequestResult', Any], Union[None, Awaitable[None]]]

@dataclass
class RequestWrapper:
    url: str
    method: str = "GET"
    params: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None
    data: Any = None
    json: Any = None
    
    user_data: Any = None 
    
    # 回调配置
    on_stream_start: Optional[OnStreamStartCallback] = None
    stream_start_data: Any = None 

    keep_content_in_memory: bool = True 
    max_retries: int = 0
    retry_interval: float = 1.0
    is_stream: bool = False
    retry_on_stream_error: bool = True 
    timeout: int = 60
    cancel_behavior: CancelBehavior = CancelBehavior.TRIGGER_FAILURE
    on_success: Optional[CallbackType] = None
    on_failure: Optional[CallbackType] = None
    extra_options: Dict[str, Any] = field(default_factory=dict)
    def __post_init__(self): self.method = self.method.upper()

class HttpErrorWithContent(Exception):
    def __init__(self, status_code, content, message):
        super().__init__(message)
        self.status_code = status_code
        self.content = content

class RequestContext:
    def __init__(self, request_id: str, wrapper: RequestWrapper):
        self.request_id = request_id
        self.wrapper = wrapper
        self.task: Optional[asyncio.Task] = None
        self.status = RequestStatus.PENDING
        self.created_at = datetime.now()
        self.stream_queue: Optional[asyncio.Queue] = asyncio.Queue(maxsize=1000) if wrapper.is_stream else None
        self.sentinel = object()
        # 用于 server.py 等待连接建立结果
        self.startup_future: asyncio.Future = asyncio.Future()

class AsyncHttpClient:
    def __init__(self):
        self.active_requests: Dict[str, RequestContext] = {}
        self.session: Optional[aiohttp.ClientSession] = None
        self.scheduler = AsyncIOScheduler()
        self.scheduler.add_job(self._garbage_collector, 'interval', seconds=60)
        self.scheduler.start()

    async def get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    def submit(self, request: RequestWrapper) -> str:
        req_id = str(uuid.uuid4())
        ctx = RequestContext(req_id, request)
        task = asyncio.create_task(self._worker(ctx))
        ctx.task = task
        self.active_requests[req_id] = ctx
        return req_id

    async def wait_for_upstream_status(self, request_id: str):
        """等待上游连接建立结果，如果重试中，这里会一直挂起，直到重试成功或彻底失败"""
        ctx = self.active_requests.get(request_id)
        if not ctx: raise ValueError("Invalid Request ID")
        await ctx.startup_future

    async def _worker(self, ctx: RequestContext):
        req = ctx.wrapper
        ctx.status = RequestStatus.RUNNING
        session = await self.get_session()
        
        request_kwargs = {
            'params': req.params,
            'headers': req.headers or {},
            'timeout': aiohttp.ClientTimeout(total=req.timeout),
            **req.extra_options
        }

        if req.json is not None:
            request_kwargs['data'] = orjson.dumps(req.json)
            if 'headers' not in request_kwargs: request_kwargs['headers'] = {}
            request_kwargs['headers']['Content-Type'] = 'application/json'
        elif req.data is not None:
            request_kwargs['data'] = req.data

        total_attempts = req.max_retries + 1
        accumulated_body = bytearray()
        last_http_code, last_error = None, None
        
        for attempt in range(1, total_attempts + 1):
            accumulated_body = bytearray()
            last_http_code, last_error = None, None
            stream_started_successfully = False
            start_time = time.perf_counter()

            try:
                if attempt > 1:
                    logger.info(f"🔄 Retry attempt {attempt}/{total_attempts} for {ctx.request_id}")

                async with session.request(req.method, req.url, **request_kwargs) as response:
                    last_http_code = response.status
                    
                    # --- 1. 错误状态码处理 ---
                    if response.status >= 400:
                        try:
                            error_bytes = await response.read()
                            if req.keep_content_in_memory: accumulated_body.extend(error_bytes)
                        except: error_bytes = b""
                        
                        exc = HttpErrorWithContent(response.status, error_bytes, f"HTTP {response.status}")
                        
                        # 【修复点 1】: 不要在这里 set_exception。
                        # 如果设置了，server.py 会立即收到错误并停止等待，导致重试无效。
                        # 我们只抛出异常，让下面的 except 块处理重试逻辑。
                        raise exc

                    # --- 2. 连接成功 (200 OK) ---
                    # 只有在这里成功了，才通知 Future 成功
                    if not ctx.startup_future.done():
                        ctx.startup_future.set_result(response.status)

                    if req.is_stream:
                        stream_started_successfully = True
                        is_first_chunk = True
                        async for chunk in response.content.iter_any():
                            # 首字回调
                            if is_first_chunk:
                                ttft = time.perf_counter() - start_time
                                self._trigger_stream_start_callback(req.on_stream_start, ctx.request_id, ttft, req.stream_start_data)
                                is_first_chunk = False

                            await ctx.stream_queue.put(chunk)
                            if req.keep_content_in_memory: accumulated_body.extend(chunk)
                        await ctx.stream_queue.put(ctx.sentinel)
                    else:
                        if req.keep_content_in_memory:
                            body_bytes = await response.read()
                            accumulated_body.extend(body_bytes)
                        else: await response.read()

                    ctx.status = RequestStatus.COMPLETED
                    final_content = self._prepare_content(accumulated_body, req.keep_content_in_memory)
                    self._trigger_callback(req.on_success, RequestResult(ctx.request_id, RequestStatus.COMPLETED, final_content, response.status), req.user_data)
                    return 

            except asyncio.CancelledError as e:
                ctx.status = RequestStatus.CANCELLED
                partial = self._prepare_content(accumulated_body, req.keep_content_in_memory)
                if req.is_stream: await ctx.stream_queue.put(e)
                
                # 取消是不可恢复的，必须立即通知 Future
                if not ctx.startup_future.done(): ctx.startup_future.set_exception(e)

                res = RequestResult(ctx.request_id, RequestStatus.CANCELLED, partial, last_http_code, e)
                if req.cancel_behavior == CancelBehavior.TRIGGER_SUCCESS: self._trigger_callback(req.on_success, res, req.user_data)
                elif req.cancel_behavior == CancelBehavior.TRIGGER_FAILURE: self._trigger_callback(req.on_failure, res, req.user_data)
                raise

            except Exception as e:
                last_error = e
                if isinstance(e, HttpErrorWithContent): last_http_code = e.status_code
                
                # 特殊情况：流已经开始但不允许流错误重试 -> 立即失败
                if req.is_stream and stream_started_successfully and not req.retry_on_stream_error:
                    logger.warning(f"Stream interrupted, no retry: {e}")
                    # 【修复点 2】: 决定不再重试了，才通知 Future 失败
                    if not ctx.startup_future.done(): ctx.startup_future.set_exception(e)
                    break 
                
                # 检查重试次数
                if attempt < total_attempts:
                    logger.warning(f"Attempt {attempt} failed: {e}. Retrying in {req.retry_interval}s...")
                    # 正在重试中... Future 保持 Pending 状态，server.py 继续等待
                    await asyncio.sleep(req.retry_interval)
                    continue 
                else: 
                    # 【修复点 3】: 重试次数耗尽，彻底失败，通知 Future
                    logger.error(f"All attempts failed: {e}")
                    if not ctx.startup_future.done(): ctx.startup_future.set_exception(e)
                    break
        
        ctx.status = RequestStatus.FAILED
        if req.is_stream:
            error_to_propagate = last_error if last_error else Exception("Unknown Error in Worker")
            await ctx.stream_queue.put(error_to_propagate)

        # 兜底
        if not ctx.startup_future.done():
            ctx.startup_future.set_exception(last_error or Exception("Worker Failed"))

        final_content = self._prepare_content(accumulated_body, req.keep_content_in_memory)
        self._trigger_callback(req.on_failure, RequestResult(ctx.request_id, RequestStatus.FAILED, final_content, last_http_code, last_error), req.user_data)

    def _prepare_content(self, data: bytearray, keep_memory: bool) -> Union[str, bytes]:
        if not keep_memory: return "[Content Dropped]"
        try: return data.decode('utf-8')
        except: return bytes(data)

    def _trigger_stream_start_callback(self, callback: OnStreamStartCallback, req_id: str, ttft: float, custom_data: Any):
        if not callback: return
        if asyncio.iscoroutinefunction(callback): asyncio.create_task(callback(req_id, ttft, custom_data))
        else: asyncio.get_running_loop().run_in_executor(None, self._run_sync_stream_start_callback, callback, req_id, ttft, custom_data)

    def _run_sync_stream_start_callback(self, callback, req_id, ttft, custom_data):
        try: callback(req_id, ttft, custom_data)
        except Exception: pass

    def _trigger_callback(self, callback: CallbackType, result: RequestResult, user_data: Any):
        if not callback: return
        if asyncio.iscoroutinefunction(callback): asyncio.create_task(callback(result, user_data))
        else: asyncio.get_running_loop().run_in_executor(None, self._run_sync_callback_safely, callback, result, user_data)

    def _run_sync_callback_safely(self, callback, result, user_data):
        try: callback(result, user_data)
        except Exception: pass

    async def cancel_request(self, request_id: str):
        ctx = self.active_requests.get(request_id)
        if ctx and ctx.task and not ctx.task.done():
            ctx.task.cancel()
            return True
        return False

    async def stream_generator(self, request_id: str) -> AsyncGenerator[bytes, None]:
        ctx = self.active_requests.get(request_id)
        if not ctx or not ctx.wrapper.is_stream: raise ValueError("Invalid ID")
        while True:
            data = await ctx.stream_queue.get()
            if isinstance(data, Exception): raise data
            if data is ctx.sentinel: break
            yield data

    async def _garbage_collector(self):
        for k in [k for k, v in self.active_requests.items() if v.task.done()]: del self.active_requests[k]

    async def close(self):
        if self.session: await self.session.close()
        self.scheduler.shutdown()
