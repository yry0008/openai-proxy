"""Smoke tests for image URL resolution in chat messages.

Verifies that HTTP image URLs in OpenAI chat completion format
are replaced with base64 data URLs by _resolve_image_urls.
"""
import base64

import aiohttp
import pytest
import pytest_asyncio

from multimedia import _resolve_image_urls, _normalize_data_url


PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVQI12NgAAIABQABNjN9GQAAAABJRUEFTkSuQmCC"
)


@pytest_asyncio.fixture
async def image_server():
    async def handler(request):
        return aiohttp.web.Response(body=PNG_1X1, content_type="image/png")

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
async def session():
    async with aiohttp.ClientSession() as s:
        yield s


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
        result = await _resolve_image_urls(session, messages)
        url = result[0]["content"][1]["image_url"]["url"]
        assert url.startswith("data:image/png;base64,")

    @pytest.mark.asyncio
    async def test_base64_url_unchanged(self, session):
        messages = [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": "data:image/png;base64,iVBOR"},
                    },
                ],
            }
        ]
        result = await _resolve_image_urls(session, messages)
        assert result[0]["content"][0]["image_url"]["url"] == "data:image/png;base64,iVBOR"

    @pytest.mark.asyncio
    async def test_no_images_returns_same_messages(self, session):
        messages = [
            {"role": "user", "content": "Just text"},
            {"role": "assistant", "content": "Reply"},
        ]
        result = await _resolve_image_urls(session, messages)
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
        result = await _resolve_image_urls(session, messages)
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
        result = await _resolve_image_urls(session, messages)
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
        result = await _resolve_image_urls(session, messages)
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
        result = await _resolve_image_urls(session, messages)
        assert result[0]["role"] == "user"
        assert result[0]["name"] == "test_user"
        assert result[0]["content"][0]["image_url"]["detail"] == "high"


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
        url = "data:image/png;name=a,b.png;base64,iVBOR"
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
        url = 'data:image/png; name="2026-07-21 17_22_17-Hello, â¢ - Sub.png";base64,iVBOR'
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "What is this?"},
                    {"type": "image_url", "image_url": {"url": url}},
                ],
            }
        ]
        result = await _resolve_image_urls(session, messages)
        assert result[0]["content"][1]["image_url"]["url"] == "data:image/png;base64,iVBOR"

    @pytest.mark.asyncio
    async def test_data_url_other_keys_preserved(self, session):
        url = 'data:image/png; name="a,b.png";base64,iVBOR'
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": url, "detail": "high"}},
                ],
            }
        ]
        result = await _resolve_image_urls(session, messages)
        assert result[0]["content"][0]["image_url"]["url"] == "data:image/png;base64,iVBOR"
        assert result[0]["content"][0]["image_url"]["detail"] == "high"
