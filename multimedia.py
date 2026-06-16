"""Multimedia-related functions for handling images, videos, and audio in messages."""

import asyncio
import base64
import logging
import math
import mimetypes
from io import BytesIO
from typing import Any, Optional

import aiohttp

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
    "_download_image_as_base64",
    "_resolve_image_urls",
    "_smart_resize",
]


def _get_image_dimensions(url: str) -> tuple[int | None, int | None]:
    if not _has_pil or _pil_image is None or not url.startswith("data:image"):
        return None, None
    try:
        _, b64_data = url.split("base64,", 1)
        raw = base64.b64decode(b64_data)
        with _pil_image.open(BytesIO(raw)) as img:
            return img.size[0], img.size[1]
    except Exception:
        return None, None


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


def _is_multimedia_part(part: Any) -> bool:
    if not isinstance(part, dict):
        return False

    part_type = str(part.get("type", "")).lower()
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
                    msg["content"] = text_parts[0].get("text", "")
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
            part_type = str(part.get("type", "")).lower()
            if part_type == "image_url":
                url = (part.get("image_url") or {}).get("url", "")
                w, h = _get_image_dimensions(url)
                items.append({"type": "image", "url": url, "width": w, "height": h})
            elif "image" in part_type and part_type != "image_url":
                url = ""
                image_data = part.get("image_url") or part.get("url")
                if isinstance(image_data, dict):
                    url = image_data.get("url", "")
                elif isinstance(image_data, str):
                    url = image_data
                w, h = _get_image_dimensions(url)
                items.append({"type": "image", "url": url, "width": w, "height": h})
            elif part_type == "video_url":
                url = (part.get("video_url") or {}).get("url", "")
                items.append({"type": "video", "url": url, "width": None, "height": None, "num_frames": None})
            elif "video" in part_type and part_type != "video_url":
                url = ""
                video_data = part.get("video_url") or part.get("url")
                if isinstance(video_data, dict):
                    url = video_data.get("url", "")
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
        for item in items:
            total += max_image_tokens

    elif strategy == "glm4v":
        grid_length = image_size // patch_size // 2
        num_image_tokens = grid_length * grid_length + 2
        image_count = sum(1 for item in items if item["type"] == "image")
        total += num_image_tokens * min(image_count, 1)

    elif strategy == "llava_next":
        for item in items:
            total += max_image_tokens

    return total


async def _download_image_as_base64(session: aiohttp.ClientSession, url: str) -> str:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status != 200:
                logger.warning(
                    "Failed to download image from %s: status %d", url, response.status
                )
                return url

            content_type = response.headers.get("content-type", "")
            if not content_type or not content_type.startswith("image/"):
                guessed, _ = mimetypes.guess_type(url)
                if guessed and guessed.startswith("image/"):
                    content_type = guessed
                else:
                    content_type = "image/png"

            content = await response.read()
            ext = content_type.split("/")[-1] if "/" in content_type else "png"
            b64 = base64.b64encode(content).decode("utf-8")
            return f"data:image/{ext};base64,{b64}"
    except Exception as e:
        logger.warning("Failed to download image from %s: %s", url, e)
        return url


async def _resolve_image_urls(session: aiohttp.ClientSession, messages: list[dict]) -> list[dict]:
    download_tasks = []
    location_to_idx = {}

    for msg_idx, msg in enumerate(messages):
        content = msg.get("content")
        if not isinstance(content, list):
            continue
        for part_idx, part in enumerate(content):
            if not isinstance(part, dict):
                continue
            if str(part.get("type", "")).lower() == "image_url":
                image_url_data = part.get("image_url")
                if isinstance(image_url_data, dict):
                    url = image_url_data.get("url", "")
                    if url.startswith("http://") or url.startswith("https://"):
                        task_idx = len(download_tasks)
                        download_tasks.append(_download_image_as_base64(session, url))
                        location_to_idx[(msg_idx, part_idx)] = task_idx

    if not download_tasks:
        return messages

    results = await asyncio.gather(*download_tasks)

    new_messages = []
    for msg_idx, msg in enumerate(messages):
        new_msg = dict(msg)
        content = new_msg.get("content")
        if isinstance(content, list):
            new_content = []
            for part_idx, part in enumerate(content):
                key = (msg_idx, part_idx)
                if key in location_to_idx:
                    new_part = dict(part) if isinstance(part, dict) else part
                    if isinstance(new_part, dict):
                        new_part["image_url"] = dict(
                            new_part.get("image_url") or {}
                        )
                        new_part["image_url"]["url"] = results[
                            location_to_idx[key]
                        ]
                    new_content.append(new_part)
                else:
                    new_content.append(part)
            new_msg["content"] = new_content
        new_messages.append(new_msg)

    return new_messages
