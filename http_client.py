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
from tempfile import SpooledTemporaryFile

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ResponseBuffer = Union[bytearray, SpooledTemporaryFile]

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
    error: Optional[BaseException] = None
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
    max_memory_size: int = 256 * 1024

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

        # [新增] 标记消费者是否已断开
        self.consumer_disconnected: bool = False

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
            # 取消连接数限制：limit=0 表示无限制
            connector = aiohttp.TCPConnector(limit=0, limit_per_host=0)
            self.session = aiohttp.ClientSession(connector=connector)
        return self.session

    def submit(self, request: RequestWrapper) -> str:
        req_id = str(uuid.uuid4())
        ctx = RequestContext(req_id, request)
        task = asyncio.create_task(self._worker(ctx))
        ctx.task = task
        self.active_requests[req_id] = ctx
        return req_id
    
    def is_alive(self, request_id: str) -> bool:
        """
        检查请求是否依然活跃（Pending 或 Running）。
        如果请求已完成、失败、取消或已被 GC 清理，返回 False。
        """
        ctx = self.active_requests.get(request_id)
        
        # 1. 如果上下文不存在（已被 GC 清理），则不活跃
        if not ctx:
            return False
        
        # 2. 如果任务还没创建（处于 Pending），视为活跃
        if ctx.task is None:
            return True
        
        # 3. 检查 asyncio.Task 是否完成
        if ctx.task.done():
            return False
            
        return True

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
        if res.json_body is not None: return res.json_body
        if res.content: return orjson.loads(res.content)
        return None

    def _new_response_buffer(self, req: RequestWrapper) -> Optional[ResponseBuffer]:
        if not req.keep_content_in_memory:
            return None
        return SpooledTemporaryFile(max_size=req.max_memory_size, mode="w+b")

    @staticmethod
    def _append_response_chunk(accumulator: Optional[ResponseBuffer], chunk: bytes) -> None:
        if not accumulator:
            return
        if isinstance(accumulator, bytearray):
            accumulator.extend(chunk)
            return
        accumulator.write(chunk)

    @staticmethod
    def _read_response_buffer(accumulator: Optional[ResponseBuffer]) -> bytes:
        if accumulator is None:
            return b""
        if isinstance(accumulator, bytearray):
            return bytes(accumulator)

        pos = accumulator.tell()
        accumulator.seek(0)
        body = accumulator.read()
        accumulator.seek(pos)
        return body

    @staticmethod
    def _close_response_buffer(accumulator: Optional[ResponseBuffer]) -> None:
        if isinstance(accumulator, SpooledTemporaryFile):
            accumulator.close()

    def _try_parse_json(self, data: Optional[ResponseBuffer]) -> Any:
        body = self._read_response_buffer(data)
        if not body: return None
        try: return orjson.loads(body)
        except Exception: return None

    @staticmethod
    def _prepare_content(data: Optional[ResponseBuffer], keep_memory: bool) -> Union[str, bytes]:
        if not keep_memory: return "[Content Dropped]"
        content = AsyncHttpClient._read_response_buffer(data)
        try: return content.decode('utf-8')
        except: return content


    async def _worker(self, ctx: RequestContext):
        req = ctx.wrapper
        ctx.status = RequestStatus.RUNNING
        session = await self.get_session()
        deadline = time.time() + req.timeout
        
        request_kwargs = {
            'params': req.params, 'headers': req.headers or {}, **req.extra_options
        }
        if req.json is not None:
            request_kwargs['data'] = orjson.dumps(req.json)
            if 'headers' not in request_kwargs: request_kwargs['headers'] = {}
            request_kwargs['headers']['Content-Type'] = 'application/json'
        elif req.data is not None:
            request_kwargs['data'] = req.data

        total_attempts = req.max_retries + 1
        accumulated_body = self._new_response_buffer(req)
        last_http_code, last_error = None, None
        stream_queue = ctx.stream_queue if req.is_stream else None

        # 认证错误的重试计数（401, 403, 521, 523）最多重试2次
        auth_error_retry_count = 0
        # 定义错误码集合
        auth_errors = {401, 403, 521, 523}
        normal_retry_errors = {408, 429, 470, 471, 472, 500, 502, 503, 504, 520, 522, 524}
        
        try:
            for attempt in range(1, total_attempts + 1):
                remaining_time = deadline - time.time()
                if remaining_time <= 0:
                    last_error = asyncio.TimeoutError("Global timeout exceeded")
                    logger.error(f"❌ Global timeout exceeded for {ctx.request_id}")
                    break
                request_kwargs['timeout'] = aiohttp.ClientTimeout(
                    total=remaining_time,
                    connect=35,
                    sock_connect=30,
                )
                if accumulated_body is not None:
                    self._close_response_buffer(accumulated_body)
                accumulated_body = self._new_response_buffer(req)
                last_http_code, last_error = None, None
                stream_started_successfully = False
                start_time = time.perf_counter()

                try:
                    if attempt > 1:
                        logger.info(f"🔄 Retry attempt {attempt}/{total_attempts} for {ctx.request_id}")

                    async with session.request(req.method, req.url, **request_kwargs) as response:
                        last_http_code = response.status
                        
                        # --- 错误状态码处理 ---
                        error_bytes = b""
                        if response.status >= 400:
                            try:
                                error_bytes = await response.read()
                                self._append_response_chunk(accumulated_body, error_bytes)
                            except:
                                pass

                            # 抛出异常进入 except 块处理（决定是否重试）
                            raise HttpErrorWithContent(response.status, error_bytes, f"HTTP {response.status}")

                        # --- 连接成功，通知 wait_for_upstream_status ---
                        if not ctx.startup_future.done():
                            ctx.startup_future.set_result(response.status)

                        if req.is_stream:
                            stream_started_successfully = True
                            stream_queue = ctx.stream_queue
                            is_first_chunk = True
                            async for chunk in response.content.iter_any():
                                if is_first_chunk:
                                    ttft = time.perf_counter() - start_time
                                    self._trigger_stream_start_callback(req.on_stream_start, ctx.request_id, ttft, req.stream_start_data)
                                    is_first_chunk = False

                                # [修改] 仅当消费者未断开时才 push 到队列
                                # 如果已断开，跳过 push 以免阻塞，但继续执行下面的 accumulate 逻辑
                                if stream_queue is not None and not ctx.consumer_disconnected:
                                    await stream_queue.put(chunk)

                                # [修改] 即使断开，依然统计完整数据
                                self._append_response_chunk(accumulated_body, chunk)

                            # 流结束，放入 sentinel (仅当没断开时)
                            if not ctx.consumer_disconnected and stream_queue is not None:
                                await stream_queue.put(ctx.sentinel)
                        else:
                            if req.keep_content_in_memory:
                                body_bytes = await response.read()
                                self._append_response_chunk(accumulated_body, body_bytes)
                            else: await response.read()

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
                    
                    if req.is_stream and not ctx.consumer_disconnected and stream_queue is not None: await stream_queue.put(e)
                    if not ctx.startup_future.done(): ctx.startup_future.set_exception(e)
                    
                    result_obj = RequestResult(ctx.request_id, RequestStatus.CANCELLED, partial, last_http_code, e, parsed_json)
                    if not ctx.result_future.done(): ctx.result_future.set_result(result_obj)

                    if req.cancel_behavior == CancelBehavior.TRIGGER_SUCCESS: self._trigger_callback(req.on_success, result_obj, req.user_data)
                    elif req.cancel_behavior == CancelBehavior.TRIGGER_FAILURE: self._trigger_callback(req.on_failure, result_obj, req.user_data)
                    raise

                except Exception as e:
                    last_error = e
                    should_stop_retry = False
                    is_auth_error = False
                    is_normal_retry_error = False

                    if isinstance(e, HttpErrorWithContent): 
                        last_http_code = e.status_code
                        is_auth_error = e.status_code in auth_errors
                        is_normal_retry_error = e.status_code in normal_retry_errors
                        
                        # 如果不是可重试的错误码，直接停止
                        if not is_auth_error and not is_normal_retry_error:
                            should_stop_retry = True
                    
                    if req.is_stream and stream_started_successfully and not req.retry_on_stream_error:
                        should_stop_retry = True

                    if should_stop_retry:
                        if not ctx.startup_future.done(): ctx.startup_future.set_exception(e)
                        break 
                    
                    # 检查是否还有重试机会 且 时间足够
                    if attempt < total_attempts and (deadline - time.time() > 0):
                        # 针对认证错误的特殊处理
                        if is_auth_error:
                            if auth_error_retry_count >= 2:
                                # 认证错误已重试2次，不再重试
                                logger.warning(f"Auth error retry limit reached (2 times) for {ctx.request_id}")
                                if not ctx.startup_future.done(): ctx.startup_future.set_exception(e)
                                break
                            # 认证错误使用固定间隔，不退坡
                            backoff = req.retry_interval
                            auth_error_retry_count += 1
                            logger.warning(f"Auth error retry {auth_error_retry_count}/2 for {ctx.request_id}. Sleeping {backoff:.2f}s (fixed)")
                        elif is_normal_retry_error:
                            # 普通可重试错误使用指数退避
                            backoff = min(req.retry_interval * (2 ** (attempt - 1)), 10.0)
                            logger.warning(f"Retry {attempt}/{total_attempts} for {ctx.request_id}. Sleeping {backoff:.2f}s (exponential backoff)")
                        else:
                            # 其他错误也使用指数退避
                            backoff = min(req.retry_interval * (2 ** (attempt - 1)), 10.0)
                            logger.warning(f"Retry {attempt}/{total_attempts} for {ctx.request_id}. Sleeping {backoff:.2f}s")
                        
                        await asyncio.sleep(backoff)
                        continue
                    else:
                        if not ctx.startup_future.done(): ctx.startup_future.set_exception(e)
                        break
            
            ctx.status = RequestStatus.FAILED
            if req.is_stream and not ctx.consumer_disconnected and stream_queue is not None:
                error_to_propagate = last_error if last_error else Exception("Unknown Error in Worker")
                await stream_queue.put(error_to_propagate)
            
            if not ctx.startup_future.done():
                ctx.startup_future.set_exception(last_error or Exception("Worker Failed"))
            
            final_content = self._prepare_content(accumulated_body, req.keep_content_in_memory)
            parsed_json = self._try_parse_json(accumulated_body) if req.keep_content_in_memory else None
            
            result_obj = RequestResult(ctx.request_id, RequestStatus.FAILED, final_content, last_http_code, last_error, parsed_json)
            
            if not ctx.result_future.done():
                ctx.result_future.set_exception(last_error or Exception("Request Failed"))

            self._trigger_callback(req.on_failure, result_obj, req.user_data)

        finally:
            self._close_response_buffer(accumulated_body)
            # TTL GC 关键点：标记结束时间
            if ctx.finished_at is None:
                ctx.finished_at = datetime.now()

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
        stream_queue = ctx.stream_queue
        if stream_queue is None: raise ValueError("Stream queue unavailable")

        try:
            while True:
                data = await stream_queue.get()
                if isinstance(data, Exception): raise data
                if data is ctx.sentinel: break
                yield data
        finally:
            # [修改] 消费者断开逻辑
            # 不再 cancel worker，而是标记 disconnected 并清空队列
            logger.info(f"ℹ️ Consumer disconnected for {request_id}. Worker will continue for stats.")
            ctx.consumer_disconnected = True

            # [关键] 必须清空队列，以防 worker 正好卡在 queue.put() 上等待空位
            # worker 在下一轮循环会检查 consumer_disconnected 并停止 put
            while not stream_queue.empty():
                try: stream_queue.get_nowait()
                except: break

    async def _garbage_collector(self):
        now = datetime.now()
        retention = timedelta(seconds=self.result_retention_seconds)
        keys_to_remove = []
        for req_id, ctx in self.active_requests.items():
            if ctx.task is not None and ctx.task.done() and ctx.finished_at and (now - ctx.finished_at > retention):
                keys_to_remove.append(req_id)
        for key in keys_to_remove:
            if key in self.active_requests: del self.active_requests[key]

    async def close(self):
        logger.info("Closing AsyncHttpClient, cancelling active requests...")
        pending_tasks = []
        for _, ctx in self.active_requests.items():
            if ctx.task and not ctx.task.done():
                ctx.task.cancel()
                pending_tasks.append(ctx.task)
        
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)
        
        if self.session: await self.session.close()
        self.scheduler.shutdown()