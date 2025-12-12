from contextlib import asynccontextmanager
import aiohttp
import os
import json
from urllib.parse import urlparse, urljoin
from starlette.datastructures import MutableHeaders
from websockets import connect as websocket_connect  # 新增依赖

from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import Response, StreamingResponse, ORJSONResponse
from fastapi.websockets import WebSocket
from http_client import  RequestWrapper, RequestResult, RequestStatus, HttpErrorWithContent
from sse_proxy_client import SseProxyClient
import orjson

import asyncio

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
import dotenv
dotenv.load_dotenv()



TARGET_SERVER = os.getenv("TARGET_SERVER", "https://api.openai.com")
MODEL_NAME = os.getenv("MODEL_NAME", "")
API_KEY = os.getenv("API_KEY", "your_api_key_here")
parsed_target = urlparse(TARGET_SERVER)
TARGET_HOST = parsed_target.netloc
TARGET_WS_SCHEME = "wss" if parsed_target.scheme == "https" else "ws"  # WebSocket协议

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 3600))  # 请求超时时间，单位秒
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", 5.0))
RETRY_INTERVAL = float(os.getenv("RETRY_INTERVAL", 0.5))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))

client_session: aiohttp.ClientSession = None

proxy_client: SseProxyClient = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    await startup()
    yield
    await shutdown()


async def startup():
    """
    应用启动时，创建 aiohttp.ClientSession 实例。
    """
    global client_session
    global proxy_client
    logger.info("Starting up and creating aiohttp.ClientSession...")
    client_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(connect=10))
    proxy_client = SseProxyClient()
    logger.info("SseProxyClient started.")

async def shutdown():
    """
    应用关闭时，优雅地关闭 aiohttp.ClientSession。
    """
    global client_session
    global proxy_client
    if client_session:
        logger.info("Shutting down aiohttp.ClientSession...")
        await client_session.close()
    if proxy_client:
        logger.info("Shutting down SseProxyClient...")
        await proxy_client.close()


app = FastAPI(
    lifespan=lifespan,
)

@app.exception_handler(HttpErrorWithContent)
async def upstream_http_exception_handler(request: Request, exc: HttpErrorWithContent):
    """
    全局异常捕获：将 AsyncHttpClient 抛出的上游错误转为对应的 HTTP 响应
    """
    logger.warning(f"⚠️ Upstream Error {exc.status_code}: {len(exc.content)} bytes")
    return Response(
        content=exc.content,
        status_code=exc.status_code,
        media_type="application/json"
    )

async def pass_ws_request(websocket: WebSocket, path: str):

    # 接受客户端连接
    await websocket.accept()
    
    # 构建目标 WebSocket URL
    target_ws_url = f"{TARGET_WS_SCHEME}://{TARGET_HOST}/{path}"
    if websocket.url.query:
        target_ws_url += f"?{websocket.url.query}"

    # 建立到目标服务器的 WebSocket 连接
    async with websocket_connect(target_ws_url,ssl=False if TARGET_WS_SCHEME == "wss" else None) as target_ws:
        # 双向数据转发
        while True:
            try:
                # 客户端 -> 目标服务器
                data = await websocket.receive()
                if data["type"] == "websocket.receive":
                    await target_ws.send(data["text"] if "text" in data else data["bytes"])
                elif data["type"] == "websocket.disconnect":
                    await target_ws.close()
                    break

                # 目标服务器 -> 客户端
                target_data = await target_ws.recv()
                if isinstance(target_data, str):
                    await websocket.send_text(target_data)
                else:
                    await websocket.send_bytes(target_data)
            except:
                await websocket.close()
                break


# 健康检查端点
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "openai-proxy"}

# WebSocket 代理路由
@app.websocket("/{path:path}")
async def websocket_proxy(websocket: WebSocket, path: str):
    return await pass_ws_request(websocket, path)


async def _check_client_disconnected(response: aiohttp.ClientResponse, raw_request: Request) -> None:
        """检查客户端是否断开连接"""
        while response.closed is False:
            if await raw_request.is_disconnected():
                logger.warning("Client disconnected, stopping stream")
                response.close()
                return
            await asyncio.sleep(1)  # 每秒检查一次
        return


async def stream_generator(response: aiohttp.ClientResponse,raw_request:Request):
    try:
        task = asyncio.create_task(_check_client_disconnected(response, raw_request))
        async for chunk in response.content:
            logger.debug(f"Received chunk: {chunk}")
            if chunk:
                yield chunk
    except (aiohttp.ClientError, ConnectionError, Exception) as e:
        import traceback
        logger.error(traceback.format_exc())
        # 在流式传输过程中如果连接断开，优雅地结束
        logger.error(f"Stream connection error: {e}")
        response.release()
        task.cancel()
        return
    finally:
        task.cancel()
        response.release()

async def on_first_chunk_callback(request_id:str, ttft:float, data:None):
    logger.info(f"First chunk for request {request_id} received in {ttft:.2f} seconds.")

@app.post("/v1/chat/completions")
async def chat_completions(req:dict,request: Request):
    try:
        target_url = TARGET_SERVER.rstrip('/') + '/' + request.url.path.lstrip('/')
        body_bytes = await request.body()
        try:
            body = orjson.loads(body_bytes)
        except orjson.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON")

        auth_header = "Bearer " + API_KEY

        client_wants_stream = body.get("stream", False)
        body["model"] = MODEL_NAME if MODEL_NAME else body.get("model", "")
        if body.get("model") is None or body.get("model") == "":
            raise HTTPException(status_code=400, detail="Model name is required but not provided.")
        
        if 'stream_options' in body:                     
            if 'continuous_usage_stats' in body['stream_options']:      
                del body['stream_options']['continuous_usage_stats']

        req_wrapper = RequestWrapper(
            url=target_url,
            method="POST",
            headers={
                "Authorization": auth_header,
                "Content-Type": "application/json"
            },
            json=body,
            is_stream=True, 
            keep_content_in_memory=False, 
            retry_on_stream_error=False,
            timeout=REQUEST_TIMEOUT,
            retry_interval=RETRY_INTERVAL,
            max_retries=MAX_RETRIES,
            on_stream_start=on_first_chunk_callback,
        )

        # 1. 提交任务
        req_id = proxy_client.submit(req_wrapper)
        logger.info(f"Forwarding request {req_id} (Stream: {client_wants_stream})")

        # 2. 【关键修复】同步等待上游连接建立结果
        # 如果上游返回 401/400/500，这里会直接抛出 HttpErrorWithContent
        # 然后被 @app.exception_handler 捕获，返回正确的错误码给客户端
        await proxy_client.wait_for_upstream_status(req_id)

        # 3. 只有当 wait_for_upstream_status 成功通过（即 Status 200），才建立流式响应
        if client_wants_stream:
            return StreamingResponse(
                proxy_client.stream_generator(
                    req_id 
                ),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no"
                }
            )
        else:
            async def collect_response():
                chunks = []
                async for chunk in proxy_client.stream_generator(req_id):
                    chunks.append(chunk)
                return b"".join(chunks)
            
            full_body = await collect_response()
            return Response(content=full_body, media_type="application/json")

    except HttpErrorWithContent:
        # 显式抛出以触发 handler
        raise
    except Exception as e:
        logger.error(f"Proxy Internal Error: {str(e)}")
        return ORJSONResponse(content={"error": str(e)}, status_code=500)


@app.api_route("/{path:path}", methods=[
    "GET", "POST", "PUT", "DELETE", 
    "PATCH", "HEAD", "OPTIONS"
])
async def reverse_proxy(request: Request, path: str):
    global client_session
    # 构建目标URL
    target_url = TARGET_SERVER.rstrip('/') + '/' + path.lstrip('/')
    # 保留原始查询参数
    if request.url.query:
        target_url += f"?{request.url.query}"

    # 获取请求体
    body = await request.body()
    
    # 如果是POST请求且有body，检查并替换model字段
    if request.method == "POST" and body:
        try:
            # 尝试解析JSON
            json_data = json.loads(body.decode('utf-8'))
            # 如果包含model字段，替换为MODEL_NAME
            if isinstance(json_data, dict) and 'model' in json_data:
                json_data['model'] = MODEL_NAME
                body = json.dumps(json_data).encode('utf-8')
        except (json.JSONDecodeError, UnicodeDecodeError):
            # 如果解析失败，保持原始body不变
            pass
    
    # 准备请求头 - 使用MutableHeaders
    headers = MutableHeaders(request.headers)
    
    # 移除客户端相关头部
    if "host" in headers:
        del headers["host"]
    
    # 设置目标服务器信息
    headers["host"] = TARGET_HOST.split('/')[0].split(':')[0]
    
    # 替换Authorization头为API_KEY
    headers["authorization"] = f"Bearer {API_KEY}"

    if "content-length" in headers:
        del headers["content-length"]
    
    # 转换headers为dict格式
    request_headers = dict(headers)
    
    # 配置超时和连接参数
    timeout = aiohttp.ClientTimeout(total=300, connect=30, sock_read=300)  # 5分钟超时，适用于长时间流式响应

    try:
        # 使用aiohttp发送请求
        response = await client_session.request(
            method=request.method,
            url=target_url,
            headers=request_headers,
            data=body if body else None,
        )
                    
        content_type = response.headers.get('content-type', '')
        if 'text/event-stream' in content_type:
            response.raise_for_status()
            # 流式响应
            return StreamingResponse(
                stream_generator(response,request),
                media_type='text/event-stream'
            )
        else:
            # 普通响应
            content = await response.read()
            
            # 过滤响应头
            response_headers = {
                k: v for k, v in response.headers.items()
                if k.lower() not in ["content-encoding", "transfer-encoding"]
            }
            
            return Response(
                content=content,
                status_code=response.status,
                headers=response_headers
            )
                    
    except aiohttp.ClientError:
        raise HTTPException(502, detail="Bad Gateway")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3280)