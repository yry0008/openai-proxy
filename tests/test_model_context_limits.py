"""Tests for per-model context size limits in TokenGuard.

Verifies that MODEL_CONTEXT_LIMITS-style mappings are enforced for the
client-requested model name, with fallback to the global model_max_context.
"""
import asyncio

from main import _parse_model_context_limits
from token_guard import TokenGuard


def _make_guard(model_max_context=None, model_context_limits=None) -> TokenGuard:
    return TokenGuard(
        batch_tokenizer=None,
        model_max_context=model_max_context,
        reasoning_type="",
        reject_multimedia=False,
        vl_config={},
        model_context_limits=model_context_limits,
    )


def _check(guard: TokenGuard, model: str, input_tokens: int, max_tokens: int = 16):
    body = {"messages": [{"role": "user", "content": "hi"}], "max_tokens": max_tokens}
    return asyncio.run(guard.check(body, b"x" * (input_tokens * 4), model=model))


class TestParseModelContextLimits:
    def test_empty_returns_empty(self):
        assert _parse_model_context_limits("") == {}
        assert _parse_model_context_limits("   ") == {}

    def test_valid_mapping(self):
        raw = '{"gpt-4o": 128000, "kimi-k2": "65536"}'
        assert _parse_model_context_limits(raw) == {"gpt-4o": 128000, "kimi-k2": 65536}

    def test_invalid_json_returns_empty(self):
        assert _parse_model_context_limits("{not json") == {}

    def test_invalid_values_skipped(self):
        raw = '{"a": -1, "b": 0, "c": "xyz", "d": 1024}'
        assert _parse_model_context_limits(raw) == {"d": 1024}


class TestModelContextLimits:
    def test_model_specific_limit_enforced(self):
        guard = _make_guard(model_context_limits={"gpt-4o": 1000})
        result = _check(guard, "gpt-4o", input_tokens=1000, max_tokens=16)
        assert result.error is not None
        assert "1000" in result.error["error"]["message"]

    def test_model_specific_limit_allows_within(self):
        guard = _make_guard(model_context_limits={"gpt-4o": 1000})
        result = _check(guard, "gpt-4o", input_tokens=900, max_tokens=16)
        assert result.error is None

    def test_model_specific_limit_without_global(self):
        guard = _make_guard(model_max_context=None, model_context_limits={"gpt-4o": 100})
        result = _check(guard, "gpt-4o", input_tokens=200)
        assert result.error is not None

    def test_unlisted_model_falls_back_to_global(self):
        guard = _make_guard(model_max_context=500, model_context_limits={"gpt-4o": 100000})
        result = _check(guard, "other-model", input_tokens=600)
        assert result.error is not None
        assert "500" in result.error["error"]["message"]

    def test_unlisted_model_no_global_no_check(self):
        guard = _make_guard(model_max_context=None, model_context_limits={"gpt-4o": 100})
        result = _check(guard, "other-model", input_tokens=999999)
        assert result.error is None

    def test_no_model_falls_back_to_global(self):
        guard = _make_guard(model_max_context=100, model_context_limits={"gpt-4o": 100000})
        result = _check(guard, "", input_tokens=200)
        assert result.error is not None
        assert "100" in result.error["error"]["message"]

    def test_model_specific_limit_overrides_global(self):
        guard = _make_guard(model_max_context=100, model_context_limits={"gpt-4o": 100000})
        result = _check(guard, "gpt-4o", input_tokens=200)
        assert result.error is None
