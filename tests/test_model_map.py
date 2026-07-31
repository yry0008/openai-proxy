"""Tests for model_map.load_model_map — MODEL_MAP env var parsing."""
from model_map import load_model_map


class TestLoadModelMap:
    def test_valid_map(self):
        raw = '{"claude-sonnet-4-5":"claude-sonnet-4-5-20250929"}'
        assert load_model_map(raw) == {"claude-sonnet-4-5": "claude-sonnet-4-5-20250929"}

    def test_multiple_entries(self):
        raw = '{"a":"a-1","b":"b-2"}'
        assert load_model_map(raw) == {"a": "a-1", "b": "b-2"}

    def test_empty_string(self):
        assert load_model_map("") == {}

    def test_whitespace_only(self):
        assert load_model_map("   ") == {}

    def test_invalid_json(self):
        assert load_model_map("{not json}") == {}

    def test_non_dict_json(self):
        assert load_model_map('["a","b"]') == {}
        assert load_model_map('"just a string"') == {}
        assert load_model_map("123") == {}

    def test_lookup_semantics(self):
        """命中映射时返回上游模型，未命中时返回原模型。"""
        m = load_model_map('{"claude-sonnet-4-5":"claude-sonnet-4-5-20250929"}')
        assert m.get("claude-sonnet-4-5", "claude-sonnet-4-5") == "claude-sonnet-4-5-20250929"
        assert m.get("gpt-4o", "gpt-4o") == "gpt-4o"
