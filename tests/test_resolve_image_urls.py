"""Smoke tests for image URL resolution in chat messages.

Verifies that HTTP image URLs in OpenAI chat completion format
are replaced with base64 data URLs by _resolve_image_urls, and that
truncated/corrupt images (downloaded or inline) are rejected with a
400 error instead of being forwarded upstream.
"""
import base64
import io

import aiohttp
import pytest
import pytest_asyncio
from PIL import Image

from multimedia import _resolve_image_urls, _normalize_data_url


def _make_valid_png() -> bytes:
    # 用 PIL 现场生成，确保像素数据完整（网络流传的极简 PNG 未必能通过严格 load() 校验）
    img = Image.new("RGB", (8, 8), color=(0, 128, 255))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


PNG_VALID = _make_valid_png()
PNG_VALID_B64 = base64.b64encode(PNG_VALID).decode()
# 保留完整 PNG 头但截断像素数据，PIL load() 会抛 "broken data stream"
PNG_TRUNCATED = PNG_VALID[: len(PNG_VALID) // 2]


@pytest_asyncio.fixture
async def image_server():
    async def handler(request):
        return aiohttp.web.Response(body=PNG_VALID, content_type="image/png")

    app = aiohttp.web.Application()
    app.router.add_get("/image.png", handler)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    yield f"http://127.0.0.1:{port}/image.png"
    await runner.cleanup()


@pytest_asyncio.fixture
async def broken_image_server():
    async def handler(request):
        return aiohttp.web.Response(body=PNG_TRUNCATED, content_type="image/png")

    app = aiohttp.web.Application()
    app.router.add_get("/broken.png", handler)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    yield f"http://127.0.0.1:{port}/broken.png"
    await runner.cleanup()


@pytest_asyncio.fixture
async def not_found_server():
    async def handler(request):
        return aiohttp.web.Response(status=404)

    app = aiohttp.web.Application()
    app.router.add_get("/missing.png", handler)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    yield f"http://127.0.0.1:{port}/missing.png"
    await runner.cleanup()


@pytest_asyncio.fixture
async def session():
    async with aiohttp.ClientSession() as s:
        yield s


def _image_message(url: str) -> list[dict]:
    return [
        {
            "role": "user",
            "content": [
                {"type": "image_url", "image_url": {"url": url}},
            ],
        }
    ]


class TestResolveImageUrls:
    @pytest.mark.asyncio
    async def test_http_image_url_replaced_with_base64(self, session, image_server):
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "What is this?"},
                    {
                        "type": "image_url",
                        "image_url": {"url": image_server},
                    },
                ],
            }
        ]
        result, err = await _resolve_image_urls(session, messages)
        assert err is None
        url = result[0]["content"][1]["image_url"]["url"]
        assert url.startswith("data:image/png;base64,")

    @pytest.mark.asyncio
    async def test_base64_url_unchanged(self, session):
        url = f"data:image/png;base64,{PNG_VALID_B64}"
        messages = [_image_message(url)[0]]
        result, err = await _resolve_image_urls(session, messages)
        assert err is None
        assert result[0]["content"][0]["image_url"]["url"] == url

    @pytest.mark.asyncio
    async def test_no_images_returns_same_messages(self, session):
        messages = [
            {"role": "user", "content": "Just text"},
            {"role": "assistant", "content": "Reply"},
        ]
        result, err = await _resolve_image_urls(session, messages)
        assert err is None
        assert result == messages

    @pytest.mark.asyncio
    async def test_text_parts_preserved(self, session, image_server):
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Describe this"},
                    {
                        "type": "image_url",
                        "image_url": {"url": image_server},
                    },
                    {"type": "text", "text": "in detail"},
                ],
            }
        ]
        result, err = await _resolve_image_urls(session, messages)
        assert err is None
        assert result[0]["content"][0]["type"] == "text"
        assert result[0]["content"][0]["text"] == "Describe this"
        assert result[0]["content"][2]["type"] == "text"
        assert result[0]["content"][2]["text"] == "in detail"

    @pytest.mark.asyncio
    async def test_multiple_images_in_one_message(self, session, image_server):
        messages = [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": image_server},
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": image_server},
                    },
                ],
            }
        ]
        result, err = await _resolve_image_urls(session, messages)
        assert err is None
        assert result[0]["content"][0]["image_url"]["url"].startswith("data:image/png;base64,")
        assert result[0]["content"][1]["image_url"]["url"].startswith("data:image/png;base64,")

    @pytest.mark.asyncio
    async def test_string_content_unchanged(self, session, image_server):
        messages = [
            {"role": "user", "content": "Hello"},
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": image_server},
                    },
                ],
            },
        ]
        result, err = await _resolve_image_urls(session, messages)
        assert err is None
        assert result[0]["content"] == "Hello"
        assert result[1]["content"][0]["image_url"]["url"].startswith("data:image/png;base64,")

    @pytest.mark.asyncio
    async def test_other_message_keys_preserved(self, session, image_server):
        messages = [
            {
                "role": "user",
                "name": "test_user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": image_server, "detail": "high"},
                    },
                ],
            }
        ]
        result, err = await _resolve_image_urls(session, messages)
        assert err is None
        assert result[0]["role"] == "user"
        assert result[0]["name"] == "test_user"
        assert result[0]["content"][0]["image_url"]["detail"] == "high"


class TestImageValidation:
    @pytest.mark.asyncio
    async def test_truncated_download_rejected(self, session, broken_image_server):
        result, err = await _resolve_image_urls(session, _image_message(broken_image_server))
        assert err is not None
        assert err["error"]["type"] == "invalid_request_error"
        assert "not a valid or complete image" in err["error"]["message"]
        assert broken_image_server in err["error"]["message"]

    @pytest.mark.asyncio
    async def test_valid_download_accepted(self, session, image_server):
        result, err = await _resolve_image_urls(session, _image_message(image_server))
        assert err is None
        assert result[0]["content"][0]["image_url"]["url"].startswith("data:image/png;base64,")

    @pytest.mark.asyncio
    async def test_truncated_inline_data_url_rejected(self, session):
        url = "data:image/png;base64," + base64.b64encode(PNG_TRUNCATED).decode()
        result, err = await _resolve_image_urls(session, _image_message(url))
        assert err is not None
        assert "Inline base64 image" in err["error"]["message"]

    @pytest.mark.asyncio
    async def test_invalid_base64_data_url_rejected(self, session):
        url = "data:image/png;base64,###invalid###"
        result, err = await _resolve_image_urls(session, _image_message(url))
        assert err is not None
        assert "Inline base64 image" in err["error"]["message"]

    @pytest.mark.asyncio
    async def test_non_image_data_url_not_validated(self, session):
        url = "data:application/pdf;base64,JVBERi0xLjQ="
        result, err = await _resolve_image_urls(session, _image_message(url))
        assert err is None
        assert result[0]["content"][0]["image_url"]["url"] == url

    @pytest.mark.asyncio
    async def test_download_404_falls_back_without_error(self, session, not_found_server):
        result, err = await _resolve_image_urls(session, _image_message(not_found_server))
        assert err is None
        assert result[0]["content"][0]["image_url"]["url"] == not_found_server


class TestNormalizeDataUrl:
    def test_standard_url_unchanged(self):
        assert _normalize_data_url("data:image/png;base64,iVBOR") == "data:image/png;base64,iVBOR"

    def test_strip_name_param_with_comma(self):
        url = 'data:image/png; name="a,b.png";base64,iVBOR'
        assert _normalize_data_url(url) == "data:image/png;base64,iVBOR"

    def test_strip_name_param_exact_filename(self):
        url = 'data:image/png; name="2026-07-21 17_22_17-Hello, â¢ - Sub (UNREG).png";base64,iVBOR'
        assert _normalize_data_url(url) == "data:image/png;base64,iVBOR"

    def test_strip_charset_param(self):
        url = "data:image/png; charset=utf-8;base64,iVBOR"
        assert _normalize_data_url(url) == "data:image/png;base64,iVBOR"

    def test_unquoted_name_with_comma(self):
        url = 'data:image/png;name=a,b.png;base64,iVBOR'
        assert _normalize_data_url(url) == "data:image/png;base64,iVBOR"

    def test_non_data_url_unchanged(self):
        assert _normalize_data_url("https://example.com/x.png") == "https://example.com/x.png"

    def test_empty_url_unchanged(self):
        assert _normalize_data_url("") == ""

    def test_jpeg_mime_preserved(self):
        url = 'data:image/jpeg; name="f.jpg";base64,aGVsbG8='
        assert _normalize_data_url(url) == "data:image/jpeg;base64,aGVsbG8="


class TestResolveNormalizesDataUrlInMessages:
    @pytest.mark.asyncio
    async def test_data_url_with_comma_in_filename_normalized(self, session):
        url = f'data:image/png; name="2026-07-21 17_22_17-Hello, â¢ - Sub.png";base64,{PNG_VALID_B64}'
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "What is this?"},
                    {"type": "image_url", "image_url": {"url": url}},
                ],
            }
        ]
        result, err = await _resolve_image_urls(session, messages)
        assert err is None
        assert result[0]["content"][1]["image_url"]["url"] == f"data:image/png;base64,{PNG_VALID_B64}"

    @pytest.mark.asyncio
    async def test_data_url_other_keys_preserved(self, session):
        url = f'data:image/png; name="a,b.png";base64,{PNG_VALID_B64}'
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": url, "detail": "high"}},
                ],
            }
        ]
        result, err = await _resolve_image_urls(session, messages)
        assert err is None
        assert result[0]["content"][0]["image_url"]["url"] == f"data:image/png;base64,{PNG_VALID_B64}"
        assert result[0]["content"][0]["image_url"]["detail"] == "high"
