"""Tests for _make_model_replace_hook — SSE model field replacement.

Uses the same logic as main.py to verify correctness independently,
without importing main.py (which has import-time side effects).
"""
import orjson


def _make_model_replace_hook(original_model: str):
    """创建 SSE chunk hook，将上游响应中顶层 model 字段替换为客户端请求的原始模型名称。"""
    def hook(event: bytes) -> bytes:
        lines = event.split(b"\n")
        result = []
        for line in lines:
            if not line.startswith(b"data: "):
                result.append(line)
                continue
            payload = line[6:]
            if payload.strip() == b"[DONE]":
                result.append(line)
                continue
            try:
                data = orjson.loads(payload)
                if isinstance(data, dict) and "model" in data:
                    data["model"] = original_model
                    result.append(b"data: " + orjson.dumps(data))
                    continue
            except (orjson.JSONDecodeError, ValueError):
                pass
            result.append(line)
        return b"\n".join(result)
    return hook


class TestModelReplaceHook:
    """SSE event model field replacement tests."""

    # -- Basic replacement --

    def test_basic_replacement(self):
        hook = _make_model_replace_hook("gpt-4o")
        event = (
            b'data: {"id":"chatcmpl-xxx","model":"gpt-4o-2024-0521",'
            b'"object":"chat.completion.chunk"}\n\n'
        )
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == "gpt-4o"

    def test_spaces_around_colon(self):
        hook = _make_model_replace_hook("gpt-4o")
        event = b'data: {"id":"xxx", "model" : "gpt-4o-2024-0521"}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == "gpt-4o"

    # -- No-match scenarios --

    def test_done_event_unchanged(self):
        hook = _make_model_replace_hook("gpt-4o")
        event = b"data: [DONE]\n\n"
        result = hook(event)
        assert result == event

    def test_no_model_field(self):
        hook = _make_model_replace_hook("gpt-4o")
        event = b'data: {"id":"chatcmpl-xxx","object":"chat.completion.chunk"}\n\n'
        result = hook(event)
        assert result == event

    def test_event_line_without_data(self):
        hook = _make_model_replace_hook("gpt-4o")
        event = b"event: ping\n\n"
        result = hook(event)
        assert result == event

    # -- Structure preservation --

    def test_event_line_preserved(self):
        hook = _make_model_replace_hook("gpt-4o")
        event = (
            b"event: message_start\n"
            b'data: {"model":"gpt-4o-2024","id":"chatcmpl-xxx"}\n\n'
        )
        result = hook(event)
        lines = result.split(b"\n")
        assert lines[0] == b"event: message_start"
        data = orjson.loads(lines[1].split(b"data: ")[1])
        assert data["model"] == "gpt-4o"

    def test_preserves_trailing_newlines(self):
        hook = _make_model_replace_hook("gpt-4o")
        event = b'data: {"model":"gpt-4o-2024"}\n\n'
        result = hook(event)
        assert result.endswith(b"\n\n")

    # -- Nested model: only top-level replaced --

    def test_nested_model_after_toplevel(self):
        """Nested 'model' key appears AFTER top-level 'model' — only top-level replaced."""
        hook = _make_model_replace_hook("gpt-4o")
        event = b'data: {"model":"gpt-4o-2024","nested":{"model":"internal-v1"}}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == "gpt-4o"
        assert data["nested"]["model"] == "internal-v1"

    def test_nested_model_before_toplevel(self):
        """Nested 'model' key appears BEFORE top-level 'model' — only top-level replaced."""
        hook = _make_model_replace_hook("gpt-4o")
        event = b'data: {"nested":{"model":"internal-v1"},"model":"gpt-4o-2024"}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == "gpt-4o"
        assert data["nested"]["model"] == "internal-v1"

    # -- Escaped content should NOT be replaced --

    def test_escaped_model_in_content_not_replaced(self):
        """Content string containing escaped \"model\":\"fake\" should not be replaced."""
        hook = _make_model_replace_hook("gpt-4o")
        event = (
            b'data: {"model":"gpt-4o-2024","choices":[{"delta":'
            b'{"content":"check \\"model\\":\\"fake\\" out"}}]}\n\n'
        )
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == "gpt-4o"
        assert data["choices"][0]["delta"]["content"] == 'check "model":"fake" out'

    def test_function_arguments_with_nested_model(self):
        """Function arguments containing nested model string should not be replaced."""
        hook = _make_model_replace_hook("gpt-4o")
        event = b'data: {"model":"gpt-4o-2024","choices":[{"delta":{"function":{"arguments":"{\\"model\\":\\"nested\\"}"}}}]}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == "gpt-4o"
        assert data["choices"][0]["delta"]["function"]["arguments"] == '{"model":"nested"}'

    # -- Edge cases --

    def test_empty_model_value(self):
        hook = _make_model_replace_hook("gpt-4o")
        event = b'data: {"model":""}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == "gpt-4o"

    def test_model_value_with_escaped_quote(self):
        """Model value containing escaped quote should be fully matched and replaced."""
        hook = _make_model_replace_hook("new-model")
        event = b'data: {"model":"test\\"model"}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == "new-model"

    def test_unicode_model_name(self):
        hook = _make_model_replace_hook("模型名")
        event = b'data: {"model":"gpt-4o-2024"}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == "模型名"

    def test_model_with_special_chars(self):
        hook = _make_model_replace_hook('my"model')
        event = b'data: {"model":"gpt-4o"}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == 'my"model'

    def test_multiline_sse_event(self):
        """Event with event: line + data: line, both preserved correctly."""
        hook = _make_model_replace_hook("gpt-4o")
        event = (
            b"event: message_start\n"
            b"data: {\"model\":\"gpt-4o-2024\",\"id\":\"abc\"}\n"
            b"\n"
        )
        result = hook(event)
        lines = result.split(b"\n")
        assert lines[0] == b"event: message_start"
        data = orjson.loads(lines[1].split(b"data: ")[1])
        assert data["model"] == "gpt-4o"

    def test_incomplete_json_passthrough(self):
        """Incomplete JSON on data: line should pass through unchanged."""
        hook = _make_model_replace_hook("gpt-4o")
        event = b'data: {"model":"gpt-4o-20\n\n'
        result = hook(event)
        assert result == event

    def test_multiple_data_lines(self):
        """Multiple data: lines in one event — each with independent model replacement."""
        hook = _make_model_replace_hook("gpt-4o")
        event = (
            b"data: {\"model\":\"gpt-4o-2024\",\"id\":\"a\"}\n"
            b"data: {\"model\":\"gpt-4o-mini\",\"id\":\"b\"}\n\n"
        )
        result = hook(event)
        lines = result.split(b"\n")
        data_a = orjson.loads(lines[0].split(b"data: ")[1])
        data_b = orjson.loads(lines[1].split(b"data: ")[1])
        assert data_a["model"] == "gpt-4o"
        assert data_b["model"] == "gpt-4o"
