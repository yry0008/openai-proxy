"""异步 HTTP 客户端 — 支持流式代理、细粒度超时与调度层重试决策。

功能概览
========

1. **细粒度超时策略**
   - ``connect_timeout`` — TCP 连接建立超时
   - ``first_byte_timeout`` — 首字节超时（请求发出 → 收到首个有效数据）
   - ``inter_chunk_timeout`` — chunk 间隔超时（流式两个 chunk 之间）
   - ``timeout`` — 全局 deadline（超出即终止，不再重试）
   每种超时对应独立异常子类（见 ``UpstreamTimeoutError`` 体系），
   调度层可通过 ``isinstance`` 区分超时类型做不同 fallback 决策。

2. **首包验证 (wait_for_first_data_validation)**
   - ``False``（默认）— HTTP Header 到达即 resolve ``startup_future``，
     ``wait_for_upstream_status()`` 在 Header 阶段返回。
   - ``True`` — 延迟到首数据块通过 ``_validate_first_chunk`` 验证后才 resolve；
     子类可重写 ``_validate_first_chunk`` 实现更复杂的验证逻辑。
   - 空 body（first_chunk is None）+ ``True`` → 由 ``_handle_empty_body`` 处理。

3. **调度层重试决策**
   - ``max_retries`` 默认为 0 — HTTP 客户端不做重试，完全由调度层控制。
   - ``SseFirstPacketError`` 等子类特定异常可通过 ``stop_retry_exceptions`` 声明，
     遇到时自动停止重试，由调度层决定是否 fallback。
   - 非首包的流错误原样输出（不吞流、不重试）。

4. **流式回调**
   - ``on_stream_start`` — 首个有效数据到达后触发，携带 TTFT (time to first token)。
   - ``on_success`` / ``on_failure`` — 请求结束后触发。

5. **消费者断开保护**
   - 当流式消费者断开（如客户端关闭连接），``_worker()`` 继续统计流量，
     但停止向 ``stream_queue`` 写入，避免背压阻塞。

6. **请求生命周期管理**
   - ``submit()`` → ``_worker()`` → ``startup_future`` / ``result_future``
   - TTL GC 自动清理已完成的请求上下文。

架构
====
- ``AsyncHttpClient`` — 核心 HTTP 客户端，管理连接池、请求上下文、重试与超时。
  通过模板方法 ``_validate_first_chunk`` / ``_handle_empty_body`` 允许子类自定义首包处理。
  子类可通过 ``stop_retry_exceptions`` 声明遇到即停止重试的异常类型。
- ``SseProxyClient`` — 继承 ``AsyncHttpClient``，重写模板方法实现 SSE 首包验证，
  增加 SSE 事件拆分、格式修复与错误检测。
"""

import asyncio
import aiohttp
import uuid
import orjson
import time
from enum import Enum
from typing import Callable, Optional, Dict, Any, AsyncGenerator, Union, Awaitable
from dataclasses import dataclass, field
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta

try:
    from src.base.logging import logger
except ImportError:
    try:
        from loguru import logger
    except ImportError:
        import logging
        logger = logging.getLogger(__name__)

class CancelBehavior(Enum):
    """请求被取消时触发哪个回调。"""
    DO_NOTHING = 0
    TRIGGER_SUCCESS = 1
    TRIGGER_FAILURE = 2


# --- 超时错误子类（用于 on_failure 回调区分不同超时类型） ---

class UpstreamTimeoutError(asyncio.TimeoutError):
    """上游超时错误基类，继承 asyncio.TimeoutError 以保持向后兼容。

    on_failure 回调的 RequestResult.error 会是以下子类之一，
    调用方可通过 isinstance 区分超时类型做不同决策。
    """
    timeout_type: str = "unknown"

    def __init__(self, msg: str = ""):
        self.timeout_type = self.__class__.timeout_type
        super().__init__(msg)


class ConnectTimeoutError(UpstreamTimeoutError):
    """TCP 连接建立超时"""
    timeout_type: str = "connect"


class FirstByteTimeoutError(UpstreamTimeoutError):
    """首字节超时（请求发出到收到首个有效数据）"""
    timeout_type: str = "first_byte"


class InterChunkTimeoutError(UpstreamTimeoutError):
    """Chunk 间隔超时（流式两个 chunk 之间）"""
    timeout_type: str = "inter_chunk"


class GlobalTimeoutError(UpstreamTimeoutError):
    """全局请求超时（deadline 耗尽）"""
    timeout_type: str = "global"


class EmptyBodyError(Exception):
    """wait_for_first_data_validation=True 时期望 body 数据但收到空响应（非 204）。"""
    pass

class RequestStatus(Enum):
    """请求生命周期状态。"""
    PENDING = "pending"      # 已提交，等待执行
    RUNNING = "running"      # _worker 正在执行
    COMPLETED = "completed"  # 成功完成
    FAILED = "failed"        # 最终失败（重试耗尽或不可重试错误）
    CANCELLED = "cancelled"  # 被 asyncio.Task.cancel() 取消

@dataclass
class RequestResult:
    """请求最终结果，由 ``_worker()`` 填充并通过 ``result_future`` 传递。"""
    request_id: str                    # 请求唯一 ID
    status: RequestStatus              # 最终状态 (COMPLETED / FAILED / CANCELLED)
    content: Union[str, bytes, None]   # 响应体（keep_content_in_memory=False 时为 "[Content Dropped]"）
    http_code: Optional[int] = None    # 最后一次尝试的 HTTP 状态码
    error: Optional[Exception] = None  # 导致失败的异常（如 UpstreamTimeoutError 子类）
    json_body: Any = None              # orjson 解析后的 JSON 对象（仅 keep_content_in_memory 时）

# 回调函数类型定义
OnStreamStartCallback = Callable[[str, float, Any], Union[None, Awaitable[None]]]
CallbackType = Callable[['RequestResult', Any], Union[None, Awaitable[None]]]

@dataclass
class RequestWrapper:
    """HTTP 请求描述，所有字段均由调用方在提交前设定，_worker 不可变。

    超时层级（由外到内）::

        timeout (全局 deadline)
          └─ connect_timeout (TCP 建立超时)
          └─ first_byte_timeout (首字节超时)
          └─ inter_chunk_timeout (chunk 间隔超时)
          └─ sock_read_timeout (底层 socket 读取超时)
    """

    # ── 请求目标 ──
    url: str                                    # 请求 URL
    method: str = "GET"                         # HTTP 方法 (自动转大写)
    params: Optional[Dict[str, Any]] = None     # URL 查询参数
    headers: Optional[Dict[str, str]] = None    # 请求头
    data: Any = None                            # 请求体 (raw bytes / str)
    json: Any = None                            # JSON 请求体 (自动 orjson.dumps + Content-Type)

    # ── 回调 ──
    user_data: Any = None                       # 透传给回调的任意上下文数据
    on_stream_start: Optional[OnStreamStartCallback] = None  # 首个有效数据到达后触发，参数 (req_id, ttft, stream_start_data)
    stream_start_data: Any = None               # 透传给 on_stream_start 的自定义数据
    on_success: Optional[CallbackType] = None   # 请求成功后触发，参数 (RequestResult, user_data)
    on_failure: Optional[CallbackType] = None   # 请求最终失败后触发，参数 (RequestResult, user_data)

    # ── 请求生命周期钩子 ──
    on_request_start: Optional[Callable[[], Awaitable[None]]] = None  # _worker 开始时调用（如 inflight inc）
    on_request_end: Optional[Callable[[], Awaitable[None]]] = None    # _worker 结束时调用（如 inflight dec），无论成功/失败/取消

    # ── 内存控制 ──
    keep_content_in_memory: bool = True          # False: 读取后丢弃 body，content 返回 "[Content Dropped]"
    max_memory_size: int = 100 * 1024 * 1024    # 单次响应最大内存 (字节)，超出抛 ValueError

    # ── 重试（默认 0 = 调度层控制） ──
    max_retries: int = 0                        # 同路由最大重试次数 (不含首次)
    retry_interval: float = 1.0                 # 首次重试间隔 (秒)，指数退避

    # ── 流式配置 ──
    is_stream: bool = False                     # True: 启用流式代理模式 (chunk → stream_queue → stream_generator)
    retry_on_stream_error: bool = True          # True: 流式传输出错仍重试; False: 流开始后出错立即停止

    # ── 超时 ──
    timeout: int = 60                           # 全局 deadline (秒)，超出后不再重试
    connect_timeout: int | float | None = None   # TCP 连接超时 (秒)，默认 35
    first_byte_timeout: int | float | None = None  # 首字节超时 (秒)，None = 不限制
    sock_read_timeout: int | float | None = None   # 底层 socket 读取超时 (秒)，默认 = timeout
    inter_chunk_timeout: int | float | None = None  # 流式 chunk 间隔超时 (秒)，None = 不限制

    # ── 取消行为 ──
    cancel_behavior: CancelBehavior = CancelBehavior.TRIGGER_FAILURE  # 请求被 cancel 时触发哪个回调

    # ── 首包验证 ──
    wait_for_first_data_validation: bool = False  # False: HTTP Header 到达即 resolve startup_future
                                                  # True: 等待首数据块通过 _validate_first_chunk 验证后才 resolve

    # ── 其他 ──
    extra_options: Dict[str, Any] = field(default_factory=dict)  # 透传给 aiohttp request() 的额外参数
    
    def __post_init__(self): 
        self.method = self.method.upper()

class HttpErrorWithContent(Exception):
    """携带上游返回的错误 Body 的异常"""
    def __init__(self, status_code: int = 500, content: Union[str, bytes, None] = None, message: str = "", url: str = ""):
        super().__init__(message)
        self.status_code = status_code
        self.content = content
        self.url = url

class RequestContext:
    """单次请求的运行时上下文，由 ``submit()`` 创建，贯穿 ``_worker()`` 生命周期。

    关键 Future:
        - ``startup_future`` — 连接建立信号。``wait_for_upstream_status()`` await 此 Future。
          ``wait_for_first_data_validation=False``: HTTP Header 到达即 resolve。
          ``wait_for_first_data_validation=True``: 首数据块通过验证后才 resolve。
          若首包验证失败，此 Future 收到子类特定异常（如 ``SseFirstPacketError``）。
        - ``result_future`` — 请求彻底完成信号。成功时 resolve ``RequestResult``，失败时 set_exception。
    """
    def __init__(self, request_id: str, wrapper: RequestWrapper):
        self.request_id: str = request_id               # 请求唯一 ID
        self.wrapper: RequestWrapper = wrapper           # 请求描述（不可变）
        self.task: Optional[asyncio.Task] = None         # _worker 对应的 asyncio.Task
        self.status: RequestStatus = RequestStatus.PENDING  # 当前生命周期状态
        self.created_at: datetime = datetime.now()       # 创建时间
        self.finished_at: Optional[datetime] = None      # 完成时间（用于 TTL GC）

        self.stream_queue: Optional[asyncio.Queue] = asyncio.Queue(maxsize=1000) if wrapper.is_stream else None  # 流式 chunk 队列

        self.consumer_disconnected: bool = False         # 消费者（stream_generator 侧）是否已断开

        self.sentinel: object = object()                 # 流结束标记，stream_queue.put(sentinel) 表示流结束

        self.startup_future: asyncio.Future = asyncio.Future()  # 连接建立信号
        self.result_future: asyncio.Future = asyncio.Future()   # 请求完成信号

# --- 核心客户端 ---

class AsyncHttpClient:
    """异步 HTTP 客户端，管理请求生命周期、重试、超时和流式代理。

    子类可覆写 ``stop_retry_exceptions`` 声明"遇到即停止重试"的异常类型，
    而无需在基类中硬编码对子类异常的 isinstance 判断。

    使用流程::

        client = AsyncHttpClient()
        req_id = client.submit(RequestWrapper(url=..., is_stream=True, ...))
        await client.wait_for_upstream_status(req_id)  # 等待上游连接建立（或首包验证通过）
        async for chunk in client.stream_generator(req_id):  # 消费流式数据
            ...

    非流式::

        req_id = client.submit(RequestWrapper(url=..., is_stream=False, ...))
        await client.wait_for_upstream_status(req_id)
        content = await client.content(req_id)

    参数:
        result_retention_seconds: 已完成请求上下文的 TTL (秒)，GC 每分钟清理一次。
    """

    # 子类可覆写此元组，声明"遇到即停止重试"的异常类型。
    # 例如 SseProxyClient 会加入 SseFirstPacketError。
    stop_retry_exceptions: tuple[type[Exception], ...] = ()

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
        """提交请求并返回 request_id。立即返回，请求在后台 _worker 中执行。

        Returns:
            请求唯一 ID，后续所有操作以此为凭据。
        """
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
        """等待上游连接建立结果。

        行为由 ``RequestWrapper.wait_for_first_data_validation`` 决定：
        - ``False`` (默认): HTTP Header 到达即返回
        - ``True``: 等待首数据块通过验证才返回

        Raises:
            EmptyBodyError: wait_for_first_data_validation=True 且 body 为空（非 204）
            UpstreamTimeoutError: 各类超时（Connect/FirstByte/InterChunk/Global）
            HttpErrorWithContent: 上游返回 4xx/5xx
        """
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

    def _try_parse_json(self, data: bytearray) -> Any:
        if not data: return None
        try: return orjson.loads(bytes(data))
        except Exception: return None

    async def _worker(self, ctx: RequestContext):
        req = ctx.wrapper
        ctx.status = RequestStatus.RUNNING

        if req.on_request_start is not None:
            try:
                await req.on_request_start()
            except Exception:
                pass

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
        accumulated_body = bytearray()
        last_http_code, last_error = None, None
        
        # 网关类鉴权错误的重试计数（521, 523）最多重试2次
        # 注意：401/403 已从此集合移除 — 上游渠道密钥失效后同路由重试无意义，
        # chat completions 场景下应通过 ChatFallbackPolicy 触发跨路由 fallback。
        # 非 chat endpoint 遇到上游 401/403 也应直接失败，避免浪费时间重试已知失效的密钥。
        auth_error_retry_count = 0
        # 定义错误码集合
        # 网关类鉴权错误：Cloudflare 521/523 表示网关不可达或源站拒绝，
        # 这类错误可能是临时性的（如 CDN 节点切换），同路由重试可能恢复。
        auth_errors = {521, 523}
        # 普通可重试错误：上游临时不可用或过载，同路由重试 + 指数退避可能恢复。
        normal_retry_errors = {408, 429, 470, 471, 472, 500, 502, 503, 504, 520, 522, 524}
        
        try:
            for attempt in range(1, total_attempts + 1):
                remaining_time = deadline - time.time()
                if remaining_time <= 0:
                    last_error = GlobalTimeoutError(f"Global timeout exceeded req_id={ctx.request_id}")
                    logger.error(f"❌ 请求全局超时 req_id={ctx.request_id}")
                    break
                _ct = req.connect_timeout
                _srt = req.sock_read_timeout
                request_kwargs['timeout'] = aiohttp.ClientTimeout(
                    total=remaining_time,
                    connect=_ct if _ct is not None else 35,
                    sock_connect=_ct if _ct is not None else 30,
                    sock_read=_srt if _srt is not None else remaining_time,
                )
                if remaining_time < 30:
                    logger.warning(
                        f"⚠️ _worker timeout 预算不足 req_id={ctx.request_id}, "
                        f"attempt={attempt}/{total_attempts}, "
                        f"remaining_time={remaining_time:.1f}s, "
                        f"wrapper.timeout={req.timeout}, "
                        f"http_code={last_http_code}"
                    )
                accumulated_body = bytearray()
                last_http_code, last_error = None, None
                stream_started_successfully = False
                start_time = time.perf_counter()

                try:
                    if attempt > 1:
                        logger.info(f"🔄 同路由重试 {attempt}/{total_attempts} req_id={ctx.request_id}")

                    async with session.request(req.method, req.url, **request_kwargs) as response:
                        last_http_code = response.status
                        
                        # --- 错误状态码处理 ---
                        if response.status >= 400:
                            try:
                                error_bytes = await response.read()
                                if req.keep_content_in_memory: accumulated_body.extend(error_bytes)
                            except: error_bytes = b""
                            
                            # 抛出异常进入 except 块处理（决定是否重试）
                            raise HttpErrorWithContent(response.status, error_bytes, f"HTTP {response.status}", url=str(req.url))

                        # --- 连接成功，通知 wait_for_upstream_status ---
                        # Default (wait_for_first_data_validation=False): resolve immediately
                        # Deferred mode (True): defer resolution until first data chunk validated
                        if not req.wait_for_first_data_validation:
                            if not ctx.startup_future.done():
                                ctx.startup_future.set_result(response.status)

                        if req.is_stream:
                            stream_started_successfully = True
                            chunk_iter = response.content.iter_any().__aiter__()
                            _ict = req.inter_chunk_timeout
                            first_byte_deadline = (
                                start_time + req.first_byte_timeout
                                if req.first_byte_timeout is not None
                                else None
                            )

                            # --- 首字节读取（可选超时） ---
                            first_chunk = None
                            try:
                                if first_byte_deadline is not None:
                                    fb_remaining = first_byte_deadline - time.perf_counter()
                                    if fb_remaining <= 0:
                                        raise FirstByteTimeoutError(
                                            f"first_byte_timeout ({req.first_byte_timeout}s) exceeded req_id={ctx.request_id}"
                                        )
                                    first_chunk = await asyncio.wait_for(
                                        chunk_iter.__anext__(), timeout=fb_remaining
                                    )
                                else:
                                    first_chunk = await chunk_iter.__anext__()
                            except StopAsyncIteration:
                                pass
                            except asyncio.TimeoutError:
                                raise FirstByteTimeoutError(
                                    f"first_byte_timeout ({req.first_byte_timeout}s) exceeded req_id={ctx.request_id}"
                                )

                            # --- First-chunk handling via template methods ---
                            validated_data = await self._validate_first_chunk(
                                first_chunk, response.status, ctx, chunk_iter, req, start_time
                            )
                            if validated_data is not None:
                                ttft = time.perf_counter() - start_time
                                self._trigger_stream_start_callback(req.on_stream_start, ctx.request_id, ttft, req.stream_start_data)
                                if not ctx.consumer_disconnected:
                                    await ctx.stream_queue.put(validated_data)
                                if req.keep_content_in_memory:
                                    accumulated_body.extend(validated_data)
                                    if len(accumulated_body) > req.max_memory_size:
                                        raise ValueError(f"Response too large > {req.max_memory_size} bytes")
                            else:
                                await self._handle_empty_body(response.status, ctx, req)

                            # --- 后续 chunk 读取（inter-chunk idle timeout 保护） ---
                            # _ict already assigned above
                            if _ict is not None:
                                while True:
                                    try:
                                        chunk = await asyncio.wait_for(chunk_iter.__anext__(), timeout=_ict)
                                    except StopAsyncIteration:
                                        break
                                    except asyncio.TimeoutError:
                                        raise InterChunkTimeoutError(
                                            f"inter-chunk idle timeout ({_ict}s) exceeded req_id={ctx.request_id}"
                                        )
                                    if not ctx.consumer_disconnected:
                                        await ctx.stream_queue.put(chunk)
                                    if req.keep_content_in_memory:
                                        accumulated_body.extend(chunk)
                                        if len(accumulated_body) > req.max_memory_size:
                                            raise ValueError(f"Response too large > {req.max_memory_size} bytes")
                            else:
                                async for chunk in chunk_iter:
                                    if not ctx.consumer_disconnected:
                                        await ctx.stream_queue.put(chunk)
                                    if req.keep_content_in_memory:
                                        accumulated_body.extend(chunk)
                                        if len(accumulated_body) > req.max_memory_size:
                                            raise ValueError(f"Response too large > {req.max_memory_size} bytes")

                            if not ctx.consumer_disconnected:
                                await ctx.stream_queue.put(ctx.sentinel)
                        else:
                            if req.keep_content_in_memory:
                                body_bytes = await response.read()
                                accumulated_body.extend(body_bytes)
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
                    
                    if req.is_stream and not ctx.consumer_disconnected: await ctx.stream_queue.put(e)
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

                    # Wrap aiohttp connect-level timeouts as ConnectTimeoutError
                    # so on_failure callbacks can distinguish timeout types.
                    # aiohttp raises asyncio.TimeoutError for connect/sock_connect timeouts.
                    if isinstance(e, asyncio.TimeoutError) and not isinstance(e, UpstreamTimeoutError):
                        if not ctx.startup_future.done():
                            last_error = ConnectTimeoutError(f"connect timeout exceeded req_id={ctx.request_id}")
                            ctx.startup_future.set_exception(last_error)
                        else:
                            last_error = ConnectTimeoutError(f"timeout exceeded req_id={ctx.request_id}")
                        e = last_error

                    if isinstance(e, HttpErrorWithContent): 
                        last_http_code = e.status_code
                        is_auth_error = e.status_code in auth_errors
                        is_normal_retry_error = e.status_code in normal_retry_errors
                        
                        # 如果不是可重试的错误码，直接停止
                        if not is_auth_error and not is_normal_retry_error:
                            should_stop_retry = True
                    
                    if isinstance(e, self.stop_retry_exceptions):
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
                                logger.warning(f"鉴权重试已达上限(2次) req_id={ctx.request_id}, http_code={last_http_code}")
                                if not ctx.startup_future.done(): ctx.startup_future.set_exception(e)
                                break
                            # 认证错误使用固定间隔，不退坡
                            backoff = req.retry_interval
                            auth_error_retry_count += 1
                            logger.warning(f"鉴权错误重试 {auth_error_retry_count}/2 req_id={ctx.request_id}, http_code={last_http_code}, 退避 {backoff:.2f}s")
                        elif is_normal_retry_error:
                            # 普通可重试错误使用指数退避
                            backoff = min(req.retry_interval * (2 ** (attempt - 1)), 10.0)
                            logger.warning(f"同路由重试 {attempt}/{total_attempts} req_id={ctx.request_id}, http_code={last_http_code} ({type(last_error).__name__}), 指数退避 {backoff:.2f}s")
                        else:
                            # 其他错误也使用指数退避
                            backoff = min(req.retry_interval * (2 ** (attempt - 1)), 10.0)
                            logger.warning(f"同路由重试 {attempt}/{total_attempts} req_id={ctx.request_id}, {type(last_error).__name__}: {str(last_error)[:200]}, 退避 {backoff:.2f}s")
                        
                        await asyncio.sleep(backoff)
                        continue
                    else:
                        if not ctx.startup_future.done(): ctx.startup_future.set_exception(e)
                        break
            
            ctx.status = RequestStatus.FAILED
            logger.warning(
                f"❌ 请求最终失败 req_id={ctx.request_id}, 尝试{total_attempts}次, "
                f"http_code={last_http_code}, error={type(last_error).__name__}: {str(last_error)[:200]}"
            )
            if req.is_stream and not ctx.consumer_disconnected:
                error_to_propagate = last_error if last_error else Exception("Unknown Error in Worker")
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
            if req.on_request_end is not None:
                try:
                    await req.on_request_end()
                except Exception:
                    pass
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

    async def _validate_first_chunk(
        self,
        first_chunk: bytes | None,
        response_status: int,
        ctx: 'RequestContext',
        chunk_iter,
        req: 'RequestWrapper',
        start_time: float,
    ) -> bytes | None:
        """首数据块验证 — 默认: 首数据到达即 resolve startup_future。
        子类可重写此方法实现更复杂的验证逻辑（如 SSE 首包验证）。
        Returns: 放入 stream_queue 的字节，或 None 表示空 body。
        """
        if first_chunk is not None:
            if not ctx.startup_future.done():
                ctx.startup_future.set_result(response_status)
            return first_chunk
        return None

    async def _handle_empty_body(
        self,
        response_status: int,
        ctx: 'RequestContext',
        req: 'RequestWrapper',
    ) -> None:
        """空 body 处理 — 默认: 204=成功, 其他=EmptyBodyError。
        子类可重写此方法实现不同语义（如空 body=成功表示无流式响应）。
        """
        if response_status == 204:
            if not ctx.startup_future.done():
                ctx.startup_future.set_result(response_status)
        else:
            if not ctx.startup_future.done():
                ctx.startup_future.set_exception(
                    EmptyBodyError(
                        f"Expected body data but got empty body (status={response_status}) req_id={ctx.request_id}"
                    )
                )

    async def cancel_request(self, request_id: str):
        ctx = self.active_requests.get(request_id)
        if ctx and ctx.task and not ctx.task.done():
            ctx.task.cancel()
            return True
        return False

    async def stream_generator(self, request_id: str) -> AsyncGenerator[bytes, None]:
        """消费流式响应的原始 chunk 迭代器。

        每个 chunk 是上游返回的原始字节片段，可能跨越多个事件或
        包含不完整事件。如需完整事件，请使用子类的 ``stream_generator()``。

        消费者断开时（迭代器被 GC / 异常中断），自动标记 ``consumer_disconnected``,
        _worker 继续统计流量但停止写入 stream_queue。

        Raises:
            Exception: _worker 传播的任何异常（如 UpstreamTimeoutError）
        """
        ctx = self.active_requests.get(request_id)
        if not ctx or not ctx.wrapper.is_stream: raise ValueError("Invalid ID")
        
        try:
            while True:
                data = await ctx.stream_queue.get()
                if isinstance(data, Exception): raise data
                if data is ctx.sentinel: break
                yield data
        finally:
            # [修改] 消费者断开逻辑
            # 不再 cancel worker，而是标记 disconnected 并清空队列
            logger.info(f"ℹ️ 消费者已断开 req_id={request_id}, worker继续统计")
            ctx.consumer_disconnected = True
            
            # [关键] 必须清空队列，以防 worker 正好卡在 queue.put() 上等待空位
            # worker 在下一轮循环会检查 consumer_disconnected 并停止 put
            while not ctx.stream_queue.empty():
                try: ctx.stream_queue.get_nowait()
                except: break

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
        logger.info("正在关闭AsyncHttpClient, 取消活跃请求...")
        pending_tasks = []
        for _, ctx in self.active_requests.items():
            if ctx.task and not ctx.task.done():
                ctx.task.cancel()
                pending_tasks.append(ctx.task)
        
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)
        
        if self.session: await self.session.close()
        self.scheduler.shutdown()