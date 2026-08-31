"""Multimedia-related functions for handling images, videos, and audio in messages."""

import asyncio
import base64
import logging
import math
import re
import tempfile
from io import BytesIO
from pathlib import Path
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
    "_extract_video_frames",
    "_convert_video_urls_to_images",
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


def _decode_video_data_url(url: str) -> tuple[bytes | None, str | None]:
    if not isinstance(url, str) or not url.startswith("data:"):
        return None, "video_url must be a base64 data URL."

    match = _DATA_URL_RE.match(url)
    if not match:
        return None, "video_url must be a base64 data URL (data:video/<type>;base64,<data>)."

    mime, b64_data = match.groups()
    if not mime.lower().startswith("video/"):
        return None, f"Unsupported video data URL MIME type: {mime}."
    try:
        raw = base64.b64decode(b64_data, validate=True)
    except Exception as e:
        return None, f"invalid base64 payload: {e}"
    if not raw:
        return None, "empty video data"
    return raw, None


async def _extract_video_frames(
    video_url: str,
    *,
    ffmpeg_bin: str = "ffmpeg",
    fps: float = 1.0,
    max_frames: int | None = 120,
    max_dimension: int = 768,
    jpeg_quality: int = 5,
    timeout: float = 60.0,
) -> tuple[list[str], str | None]:
    """Extract JPEG data URLs from a base64 video URL at a fixed frame rate."""
    video_bytes, decode_error = _decode_video_data_url(video_url)
    if decode_error is not None:
        return [], decode_error
    if fps <= 0:
        return [], "Video frame rate must be greater than zero."
    if max_frames is not None and max_frames <= 0:
        return [], "Video max_frames must be greater than zero."
    if max_dimension <= 0:
        return [], "Video max_dimension must be greater than zero."

    filter_graph = (
        f"fps={fps:g},"
        f"scale={max_dimension}:{max_dimension}:force_original_aspect_ratio=decrease"
    )

    with tempfile.TemporaryDirectory(prefix="openai-proxy-video-") as temp_dir:
        # MP4 files may require seeking to their trailing moov atom. A regular
        # temporary file is therefore more compatible than feeding bytes via
        # stdin/pipe:0.
        input_path = Path(temp_dir) / "input.video"
        await asyncio.to_thread(input_path.write_bytes, video_bytes)
        output_pattern = str(Path(temp_dir) / "frame-%06d.jpg")
        command = [
            ffmpeg_bin,
            "-hide_banner",
            "-loglevel", "error",
            "-y",
            "-threads", "1",
            "-i", str(input_path),
            "-map", "0:v:0",
            "-an",
            "-vf", filter_graph,
            "-q:v", str(jpeg_quality),
        ]
        if max_frames is not None:
            command.extend(["-frames:v", str(max_frames)])
        command.extend(["-f", "image2", output_pattern])

        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            return [], f"FFmpeg executable was not found: {ffmpeg_bin}"
        except OSError as e:
            return [], f"Failed to start FFmpeg: {e}"

        try:
            _, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            if process.returncode is None:
                process.kill()
            await process.wait()
            return [], f"Video frame extraction timed out after {timeout:g} seconds."
        except Exception as e:
            if process.returncode is None:
                process.kill()
            await process.wait()
            return [], f"Video frame extraction failed: {e}"

        if process.returncode != 0:
            detail = stderr.decode("utf-8", errors="replace").strip()
            return [], f"FFmpeg could not decode the video{': ' + detail if detail else '.'}"

        frame_paths = sorted(Path(temp_dir).glob("frame-*.jpg"))
        if not frame_paths:
            return [], "FFmpeg produced no video frames."

        frames = []
        for frame_path in frame_paths:
            frame_bytes = frame_path.read_bytes()
            invalid = _validate_image_bytes(frame_bytes)
            if invalid is not None:
                return [], f"FFmpeg produced an invalid image frame ({invalid})."
            frame_b64 = base64.b64encode(frame_bytes).decode("ascii")
            frames.append(f"data:image/jpeg;base64,{frame_b64}")
        return frames, None


async def _convert_video_urls_to_images(
    messages: list[dict],
    *,
    ffmpeg_bin: str = "ffmpeg",
    fps: float = 1.0,
    max_frames: int | None = 120,
    max_dimension: int = 768,
    jpeg_quality: int = 5,
    timeout: float = 60.0,
) -> tuple[list[dict], Optional[dict]]:
    """Replace data URL video parts with one image part per extracted frame."""
    new_messages = []
    for msg in messages:
        new_msg = dict(msg)
        content = new_msg.get("content")
        if not isinstance(content, list):
            new_messages.append(new_msg)
            continue

        new_content = []
        for part in content:
            if not isinstance(part, dict) or str(part.get("type") or "").lower() != "video_url":
                new_content.append(part)
                continue

            video_url_data = part.get("video_url")
            if not isinstance(video_url_data, dict):
                new_content.append(part)
                continue
            url = video_url_data.get("url") or ""
            if not isinstance(url, str) or not url.startswith("data:"):
                new_content.append(part)
                continue

            frame_urls, extraction_error = await _extract_video_frames(
                url,
                ffmpeg_bin=ffmpeg_bin,
                fps=fps,
                max_frames=max_frames,
                max_dimension=max_dimension,
                jpeg_quality=jpeg_quality,
                timeout=timeout,
            )
            if extraction_error is not None:
                logger.warning("Failed to extract video frames: %s", extraction_error)
                return new_messages, _build_invalid_image_error(
                    f"Video could not be converted to image frames ({extraction_error})."
                )
            new_content.extend(
                {"type": "image_url", "image_url": {"url": frame_url}}
                for frame_url in frame_urls
            )

        new_msg["content"] = new_content
        new_messages.append(new_msg)

    return new_messages, None


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
    """Validate image and video URLs in message content.

    Returns (messages, error). error is None when every media part is usable;
    otherwise it is a 400 error body to return to the client. Remote
    http(s) image and video URLs are rejected — clients must send data URLs.
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
            if not isinstance(part, dict):
                new_content.append(part)
                continue

            part_type = str(part.get("type") or "").lower()
            if part_type == "image_url":
                url_field = "image_url"
                media_name = "image"
            elif part_type == "video_url":
                url_field = "video_url"
                media_name = "video"
            else:
                new_content.append(part)
                continue

            media_url_data = part.get(url_field)
            if not isinstance(media_url_data, dict):
                new_content.append(part)
                continue
            url = media_url_data.get("url") or ""
            if isinstance(url, str) and url.lower().startswith(("http://", "https://")):
                logger.warning("Rejected remote %s URL: %s", media_name, url)
                return new_messages, _build_invalid_image_error(
                    f"Remote {media_name} URL is not supported ({url}). "
                    f"{url_field} must be a base64 data URL "
                    f"(data:{media_name}/<type>;base64,<data>)."
                )
            elif isinstance(url, str) and url.startswith("data:"):
                normalized = _normalize_data_url(url)
                invalid = _validate_data_url(normalized) if media_name == "image" else None
                if invalid is not None:
                    logger.warning(
                        "Rejected inline image data URL (%d chars): %s", len(url), invalid
                    )
                    return new_messages, _build_invalid_image_error(
                        f"Inline base64 image is not a valid or complete image ({invalid}). "
                        "Please check the image data."
                    )
                new_part = dict(part)
                new_part[url_field] = dict(media_url_data)
                new_part[url_field]["url"] = normalized
                new_content.append(new_part)
            else:
                new_content.append(part)
        new_msg["content"] = new_content
        new_messages.append(new_msg)

    return new_messages, None
