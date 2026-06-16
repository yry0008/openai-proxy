"""Smoke tests for Kimi K2.5 NaViT image token estimation.

Verifies that _estimate_multimedia_tokens and _navit_resize_tokens produce
correct token counts matching the official Kimi K2.5 NaViT resize formula.
"""
import base64
import io

from PIL import Image

from multimedia import _estimate_multimedia_tokens, _navit_resize_tokens


KIMI_CONFIG = {"strategy": "kimi_k25"}


def _make_data_uri(width: int, height: int) -> str:
    img = Image.new("RGB", (width, height), color=(0, 128, 255))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    b64 = base64.b64encode(buf.getvalue()).decode()
    return f"data:image/png;base64,{b64}"


class TestNavitResizeTokens:
    def test_small_image_produces_few_tokens(self):
        result = _navit_resize_tokens(224, 224)
        assert result > 0
        assert result <= 100, f"Expected <100 tokens for 224x224, got {result}"

    def test_large_image_produces_many_tokens(self):
        result = _navit_resize_tokens(1024, 1024)
        assert result > 1000, f"Expected >1000 tokens for 1024x1024, got {result}"

    def test_huge_image_capped_by_in_patch_limit(self):
        result = _navit_resize_tokens(10000, 10000)
        assert result > 0
        assert result < 20000

    def test_tall_narrow_image_handled(self):
        result = _navit_resize_tokens(100, 2000)
        assert result > 0

    def test_tokens_scale_with_resolution(self):
        small = _navit_resize_tokens(256, 256)
        medium = _navit_resize_tokens(512, 512)
        large = _navit_resize_tokens(1024, 1024)
        assert small < medium < large

    def test_min_image_size(self):
        result = _navit_resize_tokens(1, 1)
        assert result >= 1

    def test_extreme_aspect_ratio_capped(self):
        result = _navit_resize_tokens(10, 10000)
        assert result > 0
        assert result < 50000

    def test_patch_limit_on_one_side_enforced(self):
        result = _navit_resize_tokens(100000, 1)
        assert result > 0


class TestEstimateKimiK25Tokens:
    def test_known_dimensions(self):
        items = [{"type": "image", "url": "", "width": 1024, "height": 768}]
        result = _estimate_multimedia_tokens(items, KIMI_CONFIG)
        expected = _navit_resize_tokens(1024, 768)
        assert result == expected
        assert result > 0

    def test_unknown_dimensions_uses_min_fallback(self):
        items = [{"type": "image", "url": "https://example.com/img.png", "width": None, "height": None}]
        result = _estimate_multimedia_tokens(items, KIMI_CONFIG)
        assert result == 1

    def test_multiple_images_summed(self):
        items = [
            {"type": "image", "url": "", "width": 640, "height": 480},
            {"type": "image", "url": "", "width": 320, "height": 240},
        ]
        result = _estimate_multimedia_tokens(items, KIMI_CONFIG)
        expected = _navit_resize_tokens(640, 480) + _navit_resize_tokens(320, 240)
        assert result == expected

    def test_no_items_returns_zero(self):
        assert _estimate_multimedia_tokens([], KIMI_CONFIG) == 0

    def test_not_flat_2048(self):
        items = [{"type": "image", "url": "", "width": 224, "height": 224}]
        result = _estimate_multimedia_tokens(items, KIMI_CONFIG)
        assert result != 2048, "Should not be flat 2048 anymore"

    def test_data_uri_dimensions_extracted(self):
        uri = _make_data_uri(512, 512)
        from multimedia import _get_image_dimensions
        w, h = _get_image_dimensions(uri)
        items = [{"type": "image", "url": uri, "width": w, "height": h}]
        result = _estimate_multimedia_tokens(items, KIMI_CONFIG)
        assert result == _navit_resize_tokens(512, 512)
