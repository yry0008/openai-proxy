"""Smoke tests for image URL validation in chat messages.

Verifies that image_url parts must carry base64 data URLs: remote
http(s) URLs are rejected with a 400 error, and truncated/corrupt
inline base64 images are rejected instead of being forwarded upstream.
"""
import base64
import io

import pytest
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
PNG_VALID_DATA_URL = f"data:image/png;base64,{PNG_VALID_B64}"
# 保留完整 PNG 头但截断像素数据，PIL load() 会抛 "broken data stream"
PNG_TRUNCATED = PNG_VALID[: len(PNG_VALID) // 2]


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
    async def test_base64_url_unchanged(self):
        messages = _image_message(PNG_VALID_DATA_URL)
        result, err = await _resolve_image_urls(messages)
        assert err is None
        assert result[0]["content"][0]["image_url"]["url"] == PNG_VALID_DATA_URL

    @pytest.mark.asyncio
    async def test_no_images_returns_same_messages(self):
        messages = [
            {"role": "user", "content": "Just text"},
            {"role": "assistant", "content": "Reply"},
        ]
        result, err = await _resolve_image_urls(messages)
        assert err is None
        assert result == messages

    @pytest.mark.asyncio
    async def test_text_parts_preserved(self):
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Describe this"},
                    {
                        "type": "image_url",
                        "image_url": {"url": PNG_VALID_DATA_URL},
                    },
                    {"type": "text", "text": "in detail"},
                ],
            }
        ]
        result, err = await _resolve_image_urls(messages)
        assert err is None
        assert result[0]["content"][0]["type"] == "text"
        assert result[0]["content"][0]["text"] == "Describe this"
        assert result[0]["content"][2]["type"] == "text"
        assert result[0]["content"][2]["text"] == "in detail"

    @pytest.mark.asyncio
    async def test_multiple_images_in_one_message(self):
        messages = [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": PNG_VALID_DATA_URL},
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": PNG_VALID_DATA_URL},
                    },
                ],
            }
        ]
        result, err = await _resolve_image_urls(messages)
        assert err is None
        assert result[0]["content"][0]["image_url"]["url"] == PNG_VALID_DATA_URL
        assert result[0]["content"][1]["image_url"]["url"] == PNG_VALID_DATA_URL

    @pytest.mark.asyncio
    async def test_string_content_unchanged(self):
        messages = [
            {"role": "user", "content": "Hello"},
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": PNG_VALID_DATA_URL},
                    },
                ],
            },
        ]
        result, err = await _resolve_image_urls(messages)
        assert err is None
        assert result[0]["content"] == "Hello"
        assert result[1]["content"][0]["image_url"]["url"] == PNG_VALID_DATA_URL

    @pytest.mark.asyncio
    async def test_other_message_keys_preserved(self):
        messages = [
            {
                "role": "user",
                "name": "test_user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": PNG_VALID_DATA_URL, "detail": "high"},
                    },
                ],
            }
        ]
        result, err = await _resolve_image_urls(messages)
        assert err is None
        assert result[0]["role"] == "user"
        assert result[0]["name"] == "test_user"
        assert result[0]["content"][0]["image_url"]["detail"] == "high"


class TestRemoteImageUrlRejected:
    @pytest.mark.asyncio
    async def test_http_url_rejected(self):
        url = "http://example.com/image.png"
        result, err = await _resolve_image_urls(_image_message(url))
        assert err is not None
        assert err["error"]["type"] == "invalid_request_error"
        assert "Remote image URL is not supported" in err["error"]["message"]
        assert url in err["error"]["message"]

    @pytest.mark.asyncio
    async def test_https_url_rejected(self):
        url = "https://example.com/image.png"
        result, err = await _resolve_image_urls(_image_message(url))
        assert err is not None
        assert err["error"]["type"] == "invalid_request_error"
        assert "Remote image URL is not supported" in err["error"]["message"]
        assert url in err["error"]["message"]

    @pytest.mark.asyncio
    async def test_remote_url_rejected_alongside_valid_base64(self):
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": PNG_VALID_DATA_URL}},
                    {"type": "image_url", "image_url": {"url": "https://example.com/x.png"}},
                ],
            }
        ]
        result, err = await _resolve_image_urls(messages)
        assert err is not None
        assert "Remote image URL is not supported" in err["error"]["message"]


class TestImageValidation:
    @pytest.mark.asyncio
    async def test_valid_base64_accepted(self):
        result, err = await _resolve_image_urls(_image_message(PNG_VALID_DATA_URL))
        assert err is None
        assert result[0]["content"][0]["image_url"]["url"] == PNG_VALID_DATA_URL

    @pytest.mark.asyncio
    async def test_truncated_inline_data_url_rejected(self):
        url = "data:image/png;base64," + base64.b64encode(PNG_TRUNCATED).decode()
        result, err = await _resolve_image_urls(_image_message(url))
        assert err is not None
        assert "Inline base64 image" in err["error"]["message"]

    @pytest.mark.asyncio
    async def test_invalid_base64_data_url_rejected(self):
        url = "data:image/png;base64,###invalid###"
        result, err = await _resolve_image_urls(_image_message(url))
        assert err is not None
        assert "Inline base64 image" in err["error"]["message"]

    @pytest.mark.asyncio
    async def test_non_image_data_url_not_validated(self):
        url = "data:application/pdf;base64,JVBERi0xLjQ="
        result, err = await _resolve_image_urls(_image_message(url))
        assert err is None
        assert result[0]["content"][0]["image_url"]["url"] == url


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
    async def test_data_url_with_comma_in_filename_normalized(self):
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
        result, err = await _resolve_image_urls(messages)
        assert err is None
        assert result[0]["content"][1]["image_url"]["url"] == f"data:image/png;base64,{PNG_VALID_B64}"

    @pytest.mark.asyncio
    async def test_data_url_other_keys_preserved(self):
        url = f'data:image/png; name="a,b.png";base64,{PNG_VALID_B64}'
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": url, "detail": "high"}},
                ],
            }
        ]
        result, err = await _resolve_image_urls(messages)
        assert err is None
        assert result[0]["content"][0]["image_url"]["url"] == f"data:image/png;base64,{PNG_VALID_B64}"
        assert result[0]["content"][0]["image_url"]["detail"] == "high"
