"""Smoke tests for qwen3_vl image token estimation.

Verifies that _estimate_multimedia_tokens produces correct token counts
matching the official Qwen3-VL smart_resize formula.
"""
import base64
import io

from PIL import Image

from multimedia import _estimate_multimedia_tokens, _smart_resize, _get_image_dimensions


CONFIG = {
    "strategy": "qwen3_vl",
    "patch_size": 16,
    "merge_size": 2,
    "temporal_patch_size": 2,
    "min_pixels": 4 * 32 * 32,
    "max_pixels": 16384 * 32 * 32,
}

FACTOR = 16 * 2  # 32


def _make_data_uri(width: int, height: int) -> str:
    img = Image.new("RGB", (width, height), color=(255, 0, 0))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    b64 = base64.b64encode(buf.getvalue()).decode()
    return f"data:image/png;base64,{b64}"


def _expected_tokens(width: int, height: int) -> int:
    resized_h, resized_w = _smart_resize(
        height, width, FACTOR,
        min_pixels=CONFIG["min_pixels"], max_pixels=CONFIG["max_pixels"],
    )
    grid_h = resized_h // CONFIG["patch_size"]
    grid_w = resized_w // CONFIG["patch_size"]
    return (grid_h * grid_w) // (CONFIG["merge_size"] ** 2)


class TestSmartResize:
    def test_dimensions_divisible_by_factor(self):
        for w, h in [(100, 100), (1080, 1080), (1920, 1080), (32, 32)]:
            rh, rw = _smart_resize(h, w, FACTOR)
            assert rh % FACTOR == 0, f"height {rh} not divisible by {FACTOR}"
            assert rw % FACTOR == 0, f"width {rw} not divisible by {FACTOR}"

    def test_small_image_enforced_to_min_pixels(self):
        rh, rw = _smart_resize(1, 1, FACTOR, min_pixels=CONFIG["min_pixels"])
        assert rh * rw >= CONFIG["min_pixels"]

    def test_large_image_capped_to_max_pixels(self):
        rh, rw = _smart_resize(10000, 10000, FACTOR, max_pixels=CONFIG["max_pixels"])
        assert rh * rw <= CONFIG["max_pixels"] + FACTOR * FACTOR

    def test_aspect_ratio_preserved(self):
        rh, rw = _smart_resize(200, 100, FACTOR)
        original_ratio = 100 / 200
        resized_ratio = rw / rh
        assert abs(original_ratio - resized_ratio) < 0.2


class TestGetImageDimensions:
    def test_reads_dimensions_from_data_uri(self):
        uri = _make_data_uri(640, 480)
        w, h = _get_image_dimensions(uri)
        assert w == 640
        assert h == 480

    def test_returns_none_for_http_url(self):
        w, h = _get_image_dimensions("https://example.com/image.png")
        assert w is None
        assert h is None

    def test_returns_none_for_empty(self):
        w, h = _get_image_dimensions("")
        assert w is None
        assert h is None


class TestEstimateMultimediaTokens:
    def test_known_image_dimensions(self):
        uri = _make_data_uri(640, 480)
        items = [{"type": "image", "url": uri, "width": 640, "height": 480}]
        result = _estimate_multimedia_tokens(items, CONFIG)
        expected = _expected_tokens(640, 480)
        assert result == expected
        assert result > 0

    def test_1080x1080_produces_reasonable_tokens(self):
        uri = _make_data_uri(1080, 1080)
        items = [{"type": "image", "url": uri, "width": 1080, "height": 1080}]
        result = _estimate_multimedia_tokens(items, CONFIG)
        assert result > 100, f"Expected >100 tokens for 1080x1080, got {result}"

    def test_unknown_dimensions_uses_min_fallback(self):
        items = [{"type": "image", "url": "https://example.com/img.png", "width": None, "height": None}]
        result = _estimate_multimedia_tokens(items, CONFIG)
        assert result == 4

    def test_multiple_images_summed(self):
        uri1 = _make_data_uri(640, 480)
        uri2 = _make_data_uri(320, 240)
        items = [
            {"type": "image", "url": uri1, "width": 640, "height": 480},
            {"type": "image", "url": uri2, "width": 320, "height": 240},
        ]
        result = _estimate_multimedia_tokens(items, CONFIG)
        expected = _expected_tokens(640, 480) + _expected_tokens(320, 240)
        assert result == expected

    def test_no_items_returns_zero(self):
        assert _estimate_multimedia_tokens([], CONFIG) == 0

    def test_data_uri_image_dimensions_extracted_correctly(self):
        uri = _make_data_uri(640, 480)
        w, h = _get_image_dimensions(uri)
        items = [{"type": "image", "url": uri, "width": w, "height": h}]
        result = _estimate_multimedia_tokens(items, CONFIG)
        assert result == _expected_tokens(640, 480)
