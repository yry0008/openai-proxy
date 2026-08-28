"""Multimedia-related functions for handling images, videos, and audio in messages."""

import base64
import logging
import math
import re
from io import BytesIO
from typing import Any, Optional

try:
    from PIL import Image as _pil_image
    _has_pil = True
except ImportError:
    _pil_image = None
    _has_pil = False

logger = logging.getLogger(__name__)

__all__ = [
    "_is_multimedia_part",
    "_MESSAGE_LEVEL_MULTIMEDIA_KEYS",
    "_strip_multimedia_from_messages",
    "_extract_multimedia_info",
    "_estimate_multimedia_tokens",
    "_resolve_image_urls",
    "_normalize_data_url",
    "_validate_image_bytes",
    "_validate_data_url",
    "_smart_resize",
]


_DATA_URL_RE = re.compile(r"^data:([^;,]+).*;base64,(.*)$", re.DOTALL)


def _normalize_data_url(url: str) -> str:
    if not url or not url.startswith("data:"):
        return url
    m = _DATA_URL_RE.match(url)
    if not m:
        return url
    mime, b64 = m.group(1), m.group(2)
    return f"data:{mime};base64,{b64}"


def _get_image_dimensions(url: str | None) -> tuple[int | None, int | None]:
    if not url or not _has_pil or _pil_image is None or not url.startswith("data:image"):
        return None, None
    try:
        _, b64_data = url.split("base64,", 1)
        raw = base64.b64decode(b64_data)
        with _pil_image.open(BytesIO(raw)) as img:
            return img.size[0], img.size[1]
    except Exception:
        return None, None


def _validate_image_bytes(raw: bytes) -> Optional[str]:
    """Fully decode image bytes to verify integrity.

    Returns an error description for truncated/corrupt/undecodable data,
    None when the image decodes cleanly (or PIL is unavailable).
    """
    if not raw:
        return "empty image data"
    if not _has_pil or _pil_image is None:
        return None
    try:
        with _pil_image.open(BytesIO(raw)) as img:
            img.load()
        return None
    except Exception as e:
        return f"{type(e).__name__}: {e}"


def _validate_data_url(url: str) -> Optional[str]:
    """Validate the base64 payload of a data:image URL decodes to a complete image.

    Non-image or non-base64 data URLs are skipped (returns None).
    """
    m = _DATA_URL_RE.match(url)
    if not m:
        return None
    mime, b64 = m.group(1), m.group(2)
    if not mime.startswith("image/"):
        return None
    try:
        raw = base64.b64decode(b64)
    except Exception as e:
        return f"invalid base64 payload: {e}"
    return _validate_image_bytes(raw)


def _build_invalid_image_error(message: str) -> dict:
    return {"error": {"message": message, "type": "invalid_request_error"}}


def _smart_resize(
    height: int, width: int, factor: int,
    min_pixels: int | None = None, max_pixels: int | None = None,
) -> tuple[int, int]:
    if max_pixels is None:
        max_pixels = 16384 * factor * factor
    if min_pixels is None:
        min_pixels = 4 * factor * factor

    h_bar = max(factor, round(height / factor) * factor)
    w_bar = max(factor, round(width / factor) * factor)

    if h_bar * w_bar > max_pixels:
        beta = math.sqrt((height * width) / max_pixels)
        h_bar = max(factor, math.floor(height / beta / factor) * factor)
        w_bar = max(factor, math.floor(width / beta / factor) * factor)
    elif h_bar * w_bar < min_pixels:
        beta = math.sqrt(min_pixels / (height * width))
        h_bar = max(factor, math.ceil(height * beta / factor) * factor)
        w_bar = max(factor, math.ceil(width * beta / factor) * factor)

    return h_bar, w_bar


def _navit_resize_tokens(
    width: int, height: int,
    patch_size: int = 14,
    merge_kernel_size: int = 2,
    in_patch_limit: int = 16384,
    patch_limit_on_one_side: int = 512,
) -> int:
    s1 = math.sqrt(
        in_patch_limit
        / (max(1.0, width // patch_size) * max(1.0, height // patch_size))
    )
    s2 = patch_limit_on_one_side * patch_size / width
    s3 = patch_limit_on_one_side * patch_size / height
    scale = min(1.0, s1, s2, s3)

    new_w = min(max(1, int(width * scale)), patch_limit_on_one_side * patch_size)
    new_h = min(max(1, int(height * scale)), patch_limit_on_one_side * patch_size)

    factor = merge_kernel_size * patch_size
    pad_w = (factor - new_w % factor) % factor
    pad_h = (factor - new_h % factor) % factor

    token_h = (new_h + pad_h) // factor
    token_w = (new_w + pad_w) // factor
    return token_h * token_w


def _is_multimedia_part(part: Any) -> bool:
    if not isinstance(part, dict):
        return False

    part_type = str(part.get("type") or "").lower()
    if part_type and part_type != "text":
        if any(token in part_type for token in ("image", "video", "audio", "file")):
            return True

    multimedia_keys = {
        "image",
        "images",
        "image_url",
        "input_image",
        "video",
        "videos",
        "video_url",
        "input_video",
        "audio",
        "audios",
        "input_audio",
        "file",
        "files",
        "input_file",
    }
    return any(key in part for key in multimedia_keys)


_MESSAGE_LEVEL_MULTIMEDIA_KEYS = {
    "image",
    "images",
    "image_url",
    "video",
    "videos",
    "video_url",
    "audio",
    "audios",
    "file",
    "files",
    "input_audio",
    "input_image",
    "input_video",
    "input_file",
}


def _strip_multimedia_from_messages(messages: list[dict]) -> list[dict]:
    result = []
    for msg in messages:
        msg = dict(msg)

        for key in _MESSAGE_LEVEL_MULTIMEDIA_KEYS:
            msg.pop(key, None)

        content = msg.get("content")

        if isinstance(content, list):
            text_parts = [p for p in content if not _is_multimedia_part(p)]
            if text_parts:
                if len(text_parts) == 1 and isinstance(text_parts[0], dict) and text_parts[0].get("type") == "text":
                    msg["content"] = text_parts[0].get("text") or ""
                else:
                    msg["content"] = text_parts
            else:
                msg["content"] = ""
        elif isinstance(content, dict):
            if _is_multimedia_part(content):
                msg["content"] = ""

        result.append(msg)
    return result


def _extract_multimedia_info(messages: list[dict]) -> list[dict]:
    items = []
    for msg in messages:
        content = msg.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            part_type = str(part.get("type") or "").lower()
            if part_type == "image_url":
                url = (part.get("image_url") or {}).get("url") or ""
                w, h = _get_image_dimensions(url)
                items.append({"type": "image", "url": url, "width": w, "height": h})
            elif "image" in part_type and part_type != "image_url":
                url = ""
                image_data = part.get("image_url") or part.get("url")
                if isinstance(image_data, dict):
                    url = image_data.get("url") or ""
                elif isinstance(image_data, str):
                    url = image_data
                w, h = _get_image_dimensions(url)
                items.append({"type": "image", "url": url, "width": w, "height": h})
            elif part_type == "video_url":
                url = (part.get("video_url") or {}).get("url") or ""
                items.append({"type": "video", "url": url, "width": None, "height": None, "num_frames": None})
            elif "video" in part_type and part_type != "video_url":
                url = ""
                video_data = part.get("video_url") or part.get("url")
                if isinstance(video_data, dict):
                    url = video_data.get("url") or ""
                elif isinstance(video_data, str):
                    url = video_data
                items.append({"type": "video", "url": url, "width": None, "height": None, "num_frames": None})
    return items


def _estimate_multimedia_tokens(items: list[dict], config: dict) -> int:
    strategy = config.get("strategy", "")
    if not strategy or strategy == "none":
        return 0

    patch_size = config.get("patch_size", 16)
    merge_size = config.get("merge_size", 2)
    temporal_patch_size = config.get("temporal_patch_size", 2)
    max_pixels = config.get("max_pixels", 0)
    image_size = config.get("image_size", 448)
    max_image_tokens = config.get("max_image_tokens", 2048)

    total = 0

    if strategy == "qwen3_vl":
        factor = patch_size * merge_size
        default_max_pixels = 16384 * factor * factor
        default_min_pixels = 4 * factor * factor
        eff_max_pixels = max_pixels if max_pixels else default_max_pixels
        eff_min_pixels = config.get("min_pixels", 0) or default_min_pixels
        min_tokens = 4

        for item in items:
            w = item.get("width")
            h = item.get("height")
            if w and h:
                resized_h, resized_w = _smart_resize(
                    h, w, factor,
                    min_pixels=eff_min_pixels, max_pixels=eff_max_pixels,
                )
                grid_h = resized_h // patch_size
                grid_w = resized_w // patch_size
                if item["type"] == "image":
                    grid_t = 1
                else:
                    num_frames = item.get("num_frames") or 1
                    grid_t = max(math.ceil(num_frames / temporal_patch_size), 1)
                total += (grid_t * grid_h * grid_w) // (merge_size ** 2)
            else:
                total += min_tokens

    elif strategy == "kimi_k25":
        kimi_patch = 14
        kimi_merge = 2
        kimi_in_patch_limit = 16384
        kimi_patch_limit_side = 512
        min_tokens = 1

        for item in items:
            w = item.get("width")
            h = item.get("height")
            if w and h:
                total += _navit_resize_tokens(
                    w, h,
                    patch_size=kimi_patch,
                    merge_kernel_size=kimi_merge,
                    in_patch_limit=kimi_in_patch_limit,
                    patch_limit_on_one_side=kimi_patch_limit_side,
                )
            else:
                total += min_tokens

    elif strategy == "minimax_m3":
        mm_patch = 14
        mm_merge = 2
        mm_factor = mm_patch * mm_merge  # 28
        mm_max_pixels = 451584
        mm_min_pixels = 4 * mm_factor * mm_factor  # 3136
        min_tokens = 1

        for item in items:
            w = item.get("width")
            h = item.get("height")
            if w and h:
                resized_h, resized_w = _smart_resize(
                    h, w, mm_factor,
                    min_pixels=mm_min_pixels, max_pixels=mm_max_pixels,
                )
                grid_h = resized_h // mm_patch
                grid_w = resized_w // mm_patch
                total += (grid_h * grid_w) // (mm_merge ** 2)
            else:
                total += min_tokens

    elif strategy == "glm4v":
        grid_length = image_size // patch_size // 2
        num_image_tokens = grid_length * grid_length + 2
        image_count = sum(1 for item in items if item["type"] == "image")
        total += num_image_tokens * min(image_count, 1)

    elif strategy == "llava_next":
        for item in items:
            total += max_image_tokens

    return total


async def _resolve_image_urls(
    messages: list[dict],
) -> tuple[list[dict], Optional[dict]]:
    """Validate image URLs in messages; only base64 data URLs are accepted.

    Returns (messages, error). error is None when every image is usable;
    otherwise it is a 400 error body to return to the client. Remote
    http(s) image URLs are rejected — clients must send base64 data URLs.
    """
    new_messages = []
    for msg in messages:
        new_msg = dict(msg)
        content = new_msg.get("content")
        if not isinstance(content, list):
            new_messages.append(new_msg)
            continue
        new_content = []
        for part in content:
            if not isinstance(part, dict) or str(part.get("type") or "").lower() != "image_url":
                new_content.append(part)
                continue
            image_url_data = part.get("image_url")
            if not isinstance(image_url_data, dict):
                new_content.append(part)
                continue
            url = image_url_data.get("url") or ""
            if url.startswith("http://") or url.startswith("https://"):
                logger.warning("Rejected remote image URL: %s", url)
                return new_messages, _build_invalid_image_error(
                    f"Remote image URL is not supported ({url}). "
                    "image_url must be a base64 data URL "
                    "(data:image/<type>;base64,<data>)."
                )
            elif url.startswith("data:"):
                normalized = _normalize_data_url(url)
                invalid = _validate_data_url(normalized)
                if invalid is not None:
                    logger.warning(
                        "Rejected inline image data URL (%d chars): %s", len(url), invalid
                    )
                    return new_messages, _build_invalid_image_error(
                        f"Inline base64 image is not a valid or complete image ({invalid}). "
                        "Please check the image data."
                    )
                new_part = dict(part)
                new_part["image_url"] = dict(image_url_data)
                new_part["image_url"]["url"] = normalized
                new_content.append(new_part)
            else:
                new_content.append(part)
        new_msg["content"] = new_content
        new_messages.append(new_msg)

    return new_messages, None
