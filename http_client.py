import asyncio
import uuid
import aiohttp
import logging
from typing import Dict, Any, Optional, Union, Callable, Awaitable, AsyncGenerator
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import orjson

# ==========================================
# PART 1: AsyncHttpClient 实现 (复制自上一轮最终版本)
# ==========================================

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        self.content = content  # bytes

class RequestContext:
    def __init__(self, request_id: str, wrapper: RequestWrapper):
        self.request_id = request_id
        self.wrapper = wrapper
        self.task: Optional[asyncio.Task] = None
        self.status = RequestStatus.PENDING
        self.created_at = datetime.now()
        self.stream_queue: Optional[asyncio.Queue] = asyncio.Queue(maxsize=1000) if wrapper.is_stream else None
        self.sentinel = object()
        # 【新增】用于同步等待上游连接建立结果的 Future
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

    # 【新增】供外部调用的等待方法
    async def wait_for_upstream_status(self, request_id: str):
        """
        等待上游建立连接并返回 Headers。
        如果上游返回 >= 400，这里会直接抛出 HttpErrorWithContent。
        """
        ctx = self.active_requests.get(request_id)
        if not ctx:
            raise ValueError("Invalid Request ID")
        
        # 等待 Future 结果
        # 如果 _worker 设置了 set_exception，这里会抛出异常
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

            try:
                # 建立连接阶段
                async with session.request(req.method, req.url, **request_kwargs) as response:
                    last_http_code = response.status
                    
                    # --- 1. 错误状态处理 ---
                    if response.status >= 400:
                        try:
                            error_bytes = await response.read()
                            if req.keep_content_in_memory: accumulated_body.extend(error_bytes)
                        except: 
                            error_bytes = b""
                        
                        exc = HttpErrorWithContent(response.status, error_bytes, f"HTTP {response.status}")
                        
                        # 【关键】立即通知等待者：启动失败
                        if not ctx.startup_future.done():
                            ctx.startup_future.set_exception(exc)
                        
                        raise exc

                    # --- 2. 成功建立连接 ---
                    # 【关键】通知等待者：启动成功
                    if not ctx.startup_future.done():
                        ctx.startup_future.set_result(response.status)

                    # --- 3. 处理 Body ---
                    if req.is_stream:
                        stream_started_successfully = True
                        async for chunk in response.content.iter_any():
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
                
                # 如果还没启动完成就取消了，通知等待者
                if not ctx.startup_future.done():
                    ctx.startup_future.set_exception(e)

                res = RequestResult(ctx.request_id, RequestStatus.CANCELLED, partial, last_http_code, e)
                if req.cancel_behavior == CancelBehavior.TRIGGER_SUCCESS: self._trigger_callback(req.on_success, res, req.user_data)
                elif req.cancel_behavior == CancelBehavior.TRIGGER_FAILURE: self._trigger_callback(req.on_failure, res, req.user_data)
                raise

            except Exception as e:
                last_error = e
                if isinstance(e, HttpErrorWithContent): last_http_code = e.status_code
                
                # 如果是在启动阶段出错，且不是重试的中间过程，应该通知 Future
                # 注意：如果是第一次尝试失败，我们可能希望等重试？ 
                # 这里策略简化：如果本次尝试失败，我们暂不 set_exception，除非是最后一次尝试
                # 或者：我们可以让 submit 调用者等待直到第一次成功或者彻底失败。
                
                if req.is_stream and stream_started_successfully and not req.retry_on_stream_error: 
                    # 流中断，不重试，通知失败
                    if not ctx.startup_future.done(): ctx.startup_future.set_exception(e)
                    break 
                
                if attempt < total_attempts:
                    await asyncio.sleep(req.retry_interval)
                    continue 
                else: 
                    # 彻底失败，通知 Future
                    if not ctx.startup_future.done(): ctx.startup_future.set_exception(e)
                    break
        
        ctx.status = RequestStatus.FAILED
        if req.is_stream:
            error_to_propagate = last_error if last_error else Exception("Unknown Error in Worker")
            await ctx.stream_queue.put(error_to_propagate)

        # 兜底：如果循环结束 Future 还没设置（极少见），设为失败
        if not ctx.startup_future.done():
            ctx.startup_future.set_exception(last_error or Exception("Worker Failed"))

        final_content = self._prepare_content(accumulated_body, req.keep_content_in_memory)
        self._trigger_callback(req.on_failure, RequestResult(ctx.request_id, RequestStatus.FAILED, final_content, last_http_code, last_error), req.user_data)

    def _prepare_content(self, data: bytearray, keep_memory: bool) -> Union[str, bytes]:
        if not keep_memory: return "[Content Dropped]"
        try: return data.decode('utf-8')
        except: return bytes(data)

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
