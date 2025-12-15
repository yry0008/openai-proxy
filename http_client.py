import asyncio
import aiohttp
import uuid
import logging
import orjson
import time
from enum import Enum
from typing import Callable, Optional, Dict, Any, AsyncGenerator, Union, Awaitable
from dataclasses import dataclass, field
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 枚举与数据类 ---

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
    json_body: Any = None 

# 回调函数类型定义
OnStreamStartCallback = Callable[[str, float, Any], Union[None, Awaitable[None]]]
CallbackType = Callable[['RequestResult', Any], Union[None, Awaitable[None]]]

@dataclass
class RequestWrapper:
    """
    HTTP 请求封装类
    """
    url: str
    method: str = "GET"
    params: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None
    data: Any = None
    json: Any = None
    
    # 回调相关
    user_data: Any = None 
    on_stream_start: Optional[OnStreamStartCallback] = None
    stream_start_data: Any = None 
    on_success: Optional[CallbackType] = None
    on_failure: Optional[CallbackType] = None
    
    # 行为配置
    keep_content_in_memory: bool = True 
    max_retries: int = 0
    retry_interval: float = 1.0
    is_stream: bool = False
    retry_on_stream_error: bool = True 
    timeout: int = 60
    cancel_behavior: CancelBehavior = CancelBehavior.TRIGGER_FAILURE
    
    extra_options: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self): 
        self.method = self.method.upper()

class HttpErrorWithContent(Exception):
    """携带上游返回的错误 Body 的异常"""
    def __init__(self, status_code: int = 500, content: Union[str, bytes, None] = None, message: str = ""):
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
        self.finished_at: Optional[datetime] = None # 用于 TTL GC
        
        self.stream_queue: Optional[asyncio.Queue] = asyncio.Queue(maxsize=1000) if wrapper.is_stream else None
        self.sentinel = object()
        
        # 两个 Future 用于同步状态
        self.startup_future: asyncio.Future = asyncio.Future() # 连接建立（Header接收）
        self.result_future: asyncio.Future = asyncio.Future()  # 请求彻底完成

# --- 核心客户端 ---

class AsyncHttpClient:
    def __init__(self, result_retention_seconds: int = 300):
        self.active_requests: Dict[str, RequestContext] = {}
        self.session: Optional[aiohttp.ClientSession] = None
        self.result_retention_seconds = result_retention_seconds
        
        self.scheduler = AsyncIOScheduler()
        self.scheduler.add_job(self._garbage_collector, 'interval', seconds=60)
        self.scheduler.start()

    async def get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            # 不设置 json_serialize，我们在 _worker 中手动用 orjson
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
        """等待上游连接建立结果，遇到 400/500 会抛出异常"""
        ctx = self.active_requests.get(request_id)
        if not ctx: raise ValueError("Invalid Request ID")
        await ctx.startup_future

    async def _get_final_result(self, request_id: str) -> RequestResult:
        ctx = self.active_requests.get(request_id)
        if not ctx: raise ValueError(f"Request {request_id} not found or cleaned up")
        return await ctx.result_future

    async def content(self, request_id: str) -> bytes:
        res = await self._get_final_result(request_id)
        if isinstance(res.content, str): return res.content.encode('utf-8')
        return res.content or b""

    async def text(self, request_id: str, encoding: str = 'utf-8', errors: str = 'replace') -> str:
        res = await self._get_final_result(request_id)
        if isinstance(res.content, str): return res.content
        if isinstance(res.content, bytes):
            return res.content.decode(encoding, errors)
        if res.content is None:
            return ""
        return res.content.decode(encoding, errors) if res.content else ""

    async def json(self, request_id: str) -> Any:
        res = await self._get_final_result(request_id)
        if res.json_body is not None:
            return res.json_body
        if res.content:
            return orjson.loads(res.content)
        return None

    def _try_parse_json(self, data: bytearray) -> Any:
        if not data: return None
        try: return orjson.loads(bytes(data))
        except Exception: return None

    async def _worker(self, ctx: RequestContext):
        req = ctx.wrapper
        ctx.status = RequestStatus.RUNNING
        session = await self.get_session()
        
        # [FIX-3] 计算绝对截止时间
        deadline = time.time() + req.timeout

        request_kwargs = {
            'params': req.params,
            'headers': req.headers or {},
            # timeout 将在循环内动态更新
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
        
        try:
            for attempt in range(1, total_attempts + 1):
                # [FIX-3] 检查剩余时间
                remaining_time = deadline - time.time()
                if remaining_time <= 0:
                    last_error = asyncio.TimeoutError("Global timeout exceeded")
                    logger.error(f"❌ Global timeout exceeded for {ctx.request_id}")
                    break
                
                request_kwargs['timeout'] = aiohttp.ClientTimeout(total=remaining_time)

                accumulated_body = bytearray()
                last_http_code, last_error = None, None
                stream_started_successfully = False
                start_time = time.perf_counter()

                try:
                    if attempt > 1:
                        logger.info(f"🔄 Retry attempt {attempt}/{total_attempts} for {ctx.request_id}")

                    async with session.request(req.method, req.url, **request_kwargs) as response:
                        last_http_code = response.status
                        
                        # --- 错误状态码处理 ---
                        if response.status >= 400:
                            try:
                                error_bytes = await response.read()
                                if req.keep_content_in_memory: accumulated_body.extend(error_bytes)
                            except: error_bytes = b""
                            
                            # 抛出异常进入 except 块处理（决定是否重试）
                            raise HttpErrorWithContent(response.status, error_bytes, f"HTTP {response.status}")

                        # --- 连接成功，通知 wait_for_upstream_status ---
                        if not ctx.startup_future.done():
                            ctx.startup_future.set_result(response.status)

                        if req.is_stream:
                            stream_started_successfully = True
                            is_first_chunk = True
                            async for chunk in response.content.iter_any():
                                # 首字回调 TTFT
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

                        # --- 请求成功完成 ---
                        ctx.status = RequestStatus.COMPLETED
                        final_content = self._prepare_content(accumulated_body, req.keep_content_in_memory)
                        parsed_json = self._try_parse_json(accumulated_body) if req.keep_content_in_memory else None
                        
                        result_obj = RequestResult(ctx.request_id, RequestStatus.COMPLETED, final_content, response.status, None, parsed_json)
                        if not ctx.result_future.done(): ctx.result_future.set_result(result_obj)
                        
                        self._trigger_callback(req.on_success, result_obj, req.user_data)
                        return 

                except asyncio.CancelledError as e:
                    ctx.status = RequestStatus.CANCELLED
                    partial = self._prepare_content(accumulated_body, req.keep_content_in_memory)
                    parsed_json = self._try_parse_json(accumulated_body) if req.keep_content_in_memory else None
                    
                    if req.is_stream: await ctx.stream_queue.put(e)
                    if not ctx.startup_future.done(): ctx.startup_future.set_exception(e)
                    
                    result_obj = RequestResult(ctx.request_id, RequestStatus.CANCELLED, partial, last_http_code, e, parsed_json)
                    if not ctx.result_future.done(): ctx.result_future.set_result(result_obj)

                    if req.cancel_behavior == CancelBehavior.TRIGGER_SUCCESS: self._trigger_callback(req.on_success, result_obj, req.user_data)
                    elif req.cancel_behavior == CancelBehavior.TRIGGER_FAILURE: self._trigger_callback(req.on_failure, result_obj, req.user_data)
                    raise

                except Exception as e:
                    last_error = e
                    if isinstance(e, HttpErrorWithContent): last_http_code = e.status_code
                    
                    if req.is_stream and stream_started_successfully and not req.retry_on_stream_error:
                        if not ctx.startup_future.done(): ctx.startup_future.set_exception(e)
                        break 
                    
                    # 检查是否还有重试机会 且 时间足够
                    if attempt < total_attempts and (deadline - time.time() > 0):
                        await asyncio.sleep(req.retry_interval)
                        continue
                    else:
                        if not ctx.startup_future.done(): ctx.startup_future.set_exception(e)
                        break
            
            # --- 最终失败 ---
            ctx.status = RequestStatus.FAILED
            if req.is_stream:
                error_to_propagate = last_error if last_error else Exception("Unknown Error")
                await ctx.stream_queue.put(error_to_propagate)
            
            if not ctx.startup_future.done():
                ctx.startup_future.set_exception(last_error or Exception("Worker Failed"))
            
            final_content = self._prepare_content(accumulated_body, req.keep_content_in_memory)
            parsed_json = self._try_parse_json(accumulated_body) if req.keep_content_in_memory else None
            
            result_obj = RequestResult(ctx.request_id, RequestStatus.FAILED, final_content, last_http_code, last_error, parsed_json)
            
            if not ctx.result_future.done():
                ctx.result_future.set_exception(last_error or Exception("Request Failed"))

            self._trigger_callback(req.on_failure, result_obj, req.user_data)

        finally:
            # TTL GC 关键点：标记结束时间
            if ctx.finished_at is None:
                ctx.finished_at = datetime.now()

    def _prepare_content(self, data: bytearray, keep_memory: bool) -> Union[str, bytes]:
        if not keep_memory: return "[Content Dropped]"
        try: return data.decode('utf-8')
        except: return bytes(data)

    def _trigger_stream_start_callback(self, callback:Union[OnStreamStartCallback, None], req_id:str, ttft:float, custom_data:Any):
        if not callback: return
        if asyncio.iscoroutinefunction(callback): asyncio.create_task(callback(req_id, ttft, custom_data))
        else: asyncio.get_running_loop().run_in_executor(None, lambda: callback(req_id, ttft, custom_data))

    def _trigger_callback(self, callback:Union[CallbackType, None], result:RequestResult, user_data:Any):
        if not callback: return
        if asyncio.iscoroutinefunction(callback): asyncio.create_task(callback(result, user_data))
        else: asyncio.get_running_loop().run_in_executor(None, lambda: callback(result, user_data))

    async def cancel_request(self, request_id: str):
        ctx = self.active_requests.get(request_id)
        if ctx and ctx.task and not ctx.task.done():
            ctx.task.cancel()
            return True
        return False

    async def stream_generator(self, request_id: str) -> AsyncGenerator[bytes, None]:
        ctx = self.active_requests.get(request_id)
        if not ctx or not ctx.wrapper.is_stream: raise ValueError("Invalid ID")
        
        try:
            while True:
                data = await ctx.stream_queue.get()
                if isinstance(data, Exception): raise data
                if data is ctx.sentinel: break
                yield data
        finally:
            # [FIX-1] 消费者断开防护：如果生成器停止但任务还在跑，说明消费者断开，必须取消任务以防死锁
            if ctx.task and not ctx.task.done():
                logger.warning(f"⚠️ Consumer disconnected early for {request_id}, cancelling worker.")
                ctx.task.cancel()
                # 让出控制权以便 loop 执行取消操作
                try: await asyncio.sleep(0.1)
                except: pass

    async def _garbage_collector(self):
        now = datetime.now()
        retention = timedelta(seconds=self.result_retention_seconds)
        keys_to_remove = []
        for req_id, ctx in self.active_requests.items():
            if ctx.task.done() and ctx.finished_at and (now - ctx.finished_at > retention):
                keys_to_remove.append(req_id)
        for key in keys_to_remove:
            if key in self.active_requests: del self.active_requests[key]

    async def close(self):
        # [FIX-4] 关闭时清理所有残余任务
        logger.info("Closing AsyncHttpClient, cancelling all active requests...")
        
        # 1. 收集所有未完成的任务
        pending_tasks = []
        for _, ctx in self.active_requests.items():
            if ctx.task and not ctx.task.done():
                ctx.task.cancel()
                pending_tasks.append(ctx.task)
        
        # 2. 等待它们优雅退出 (它们会捕获 CancelledError 并处理 finally)
        if pending_tasks:
            # return_exceptions=True 防止某个任务报错打断关闭流程
            await asyncio.gather(*pending_tasks, return_exceptions=True)
        
        # 3. 安全关闭 Session
        if self.session: 
            await self.session.close()
        
        self.scheduler.shutdown()