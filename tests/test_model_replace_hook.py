"""Tests for _make_model_replace_hook — SSE id/model field replacement.

Uses the same logic as main.py to verify correctness independently,
without importing main.py (which has import-time side effects).
"""
import orjson


def _make_model_replace_hook(original_model: str, response_id: str, is_vllm: bool = False):
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
                modified = False
                if isinstance(data, dict):
                    if "id" in data:
                        data["id"] = response_id
                        modified = True
                    if original_model and "model" in data:
                        data["model"] = original_model
                        modified = True
                    if is_vllm:
                        choices = data.get("choices")
                        if isinstance(choices, list):
                            for choice in choices:
                                delta = choice.get("delta") if isinstance(choice, dict) else None
                                if isinstance(delta, dict) and "reasoning" in delta and "reasoning_content" not in delta:
                                    delta["reasoning_content"] = delta.pop("reasoning")
                                    modified = True
                if modified:
                    result.append(b"data: " + orjson.dumps(data))
                    continue
            except (orjson.JSONDecodeError, ValueError):
                pass
            result.append(line)
        return b"\n".join(result)
    return hook


class TestModelReplaceHook:
    """SSE event id/model field replacement tests."""

    # -- Basic replacement --

    def test_basic_replacement(self):
        hook = _make_model_replace_hook("gpt-4o", "resp-001")
        event = (
            b'data: {"id":"chatcmpl-xxx","model":"gpt-4o-2024-0521",'
            b'"object":"chat.completion.chunk"}\n\n'
        )
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == "gpt-4o"
        assert data["id"] == "resp-001"

    def test_spaces_around_colon(self):
        hook = _make_model_replace_hook("gpt-4o", "resp-001")
        event = b'data: {"id":"xxx", "model" : "gpt-4o-2024-0521"}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == "gpt-4o"
        assert data["id"] == "resp-001"

    # -- No-match scenarios --

    def test_done_event_unchanged(self):
        hook = _make_model_replace_hook("gpt-4o", "resp-001")
        event = b"data: [DONE]\n\n"
        result = hook(event)
        assert result == event

    def test_no_id_no_model_field(self):
        hook = _make_model_replace_hook("gpt-4o", "resp-001")
        event = b'data: {"object":"chat.completion.chunk"}\n\n'
        result = hook(event)
        assert result == event

    def test_event_line_without_data(self):
        hook = _make_model_replace_hook("gpt-4o", "resp-001")
        event = b"event: ping\n\n"
        result = hook(event)
        assert result == event

    # -- Structure preservation --

    def test_event_line_preserved(self):
        hook = _make_model_replace_hook("gpt-4o", "resp-001")
        event = (
            b"event: message_start\n"
            b'data: {"model":"gpt-4o-2024","id":"chatcmpl-xxx"}\n\n'
        )
        result = hook(event)
        lines = result.split(b"\n")
        assert lines[0] == b"event: message_start"
        data = orjson.loads(lines[1].split(b"data: ")[1])
        assert data["model"] == "gpt-4o"
        assert data["id"] == "resp-001"

    def test_preserves_trailing_newlines(self):
        hook = _make_model_replace_hook("gpt-4o", "resp-001")
        event = b'data: {"model":"gpt-4o-2024"}\n\n'
        result = hook(event)
        assert result.endswith(b"\n\n")

    # -- Nested model: only top-level replaced --

    def test_nested_model_after_toplevel(self):
        """Nested 'model' key appears AFTER top-level 'model' — only top-level replaced."""
        hook = _make_model_replace_hook("gpt-4o", "resp-001")
        event = b'data: {"model":"gpt-4o-2024","nested":{"model":"internal-v1"}}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == "gpt-4o"
        assert data["nested"]["model"] == "internal-v1"

    def test_nested_model_before_toplevel(self):
        """Nested 'model' key appears BEFORE top-level 'model' — only top-level replaced."""
        hook = _make_model_replace_hook("gpt-4o", "resp-001")
        event = b'data: {"nested":{"model":"internal-v1"},"model":"gpt-4o-2024"}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == "gpt-4o"
        assert data["nested"]["model"] == "internal-v1"

    # -- Escaped content should NOT be replaced --

    def test_escaped_model_in_content_not_replaced(self):
        """Content string containing escaped \"model\":\"fake\" should not be replaced."""
        hook = _make_model_replace_hook("gpt-4o", "resp-001")
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
        hook = _make_model_replace_hook("gpt-4o", "resp-001")
        event = b'data: {"model":"gpt-4o-2024","choices":[{"delta":{"function":{"arguments":"{\\"model\\":\\"nested\\"}"}}}]}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == "gpt-4o"
        assert data["choices"][0]["delta"]["function"]["arguments"] == '{"model":"nested"}'

    # -- Edge cases --

    def test_empty_model_value(self):
        hook = _make_model_replace_hook("gpt-4o", "resp-001")
        event = b'data: {"model":""}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == "gpt-4o"

    def test_model_value_with_escaped_quote(self):
        """Model value containing escaped quote should be fully matched and replaced."""
        hook = _make_model_replace_hook("new-model", "resp-001")
        event = b'data: {"model":"test\\"model"}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == "new-model"

    def test_unicode_model_name(self):
        hook = _make_model_replace_hook("模型名", "resp-001")
        event = b'data: {"model":"gpt-4o-2024"}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == "模型名"

    def test_model_with_special_chars(self):
        hook = _make_model_replace_hook('my"model', "resp-001")
        event = b'data: {"model":"gpt-4o"}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["model"] == 'my"model'

    def test_multiline_sse_event(self):
        """Event with event: line + data: line, both preserved correctly."""
        hook = _make_model_replace_hook("gpt-4o", "resp-001")
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
        assert data["id"] == "resp-001"

    def test_incomplete_json_passthrough(self):
        """Incomplete JSON on data: line should pass through unchanged."""
        hook = _make_model_replace_hook("gpt-4o", "resp-001")
        event = b'data: {"model":"gpt-4o-20\n\n'
        result = hook(event)
        assert result == event

    def test_multiple_data_lines(self):
        """Multiple data: lines in one event — each with independent replacement."""
        hook = _make_model_replace_hook("gpt-4o", "resp-001")
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
        assert data_a["id"] == "resp-001"
        assert data_b["id"] == "resp-001"

    # -- Response id replacement --

    def test_id_replaced_even_without_model(self):
        """id field is replaced even when the chunk has no model field."""
        hook = _make_model_replace_hook("gpt-4o", "resp-001")
        event = b'data: {"id":"chatcmpl-xxx","object":"chat.completion.chunk"}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["id"] == "resp-001"

    def test_id_replaced_with_empty_original_model(self):
        """id field is replaced even when original_model is empty."""
        hook = _make_model_replace_hook("", "resp-001")
        event = b'data: {"id":"chatcmpl-xxx","model":"upstream-v1"}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        assert data["id"] == "resp-001"
        assert data["model"] == "upstream-v1"

    def test_model_not_replaced_when_original_model_empty(self):
        """With empty original_model and no id, model field passes through unchanged."""
        hook = _make_model_replace_hook("", "resp-001")
        event = b'data: {"model":"upstream-v1"}\n\n'
        result = hook(event)
        assert result == event

    # -- vLLM reasoning field conversion --

    def test_vllm_reasoning_converted(self):
        """is_vllm=True converts delta.reasoning to delta.reasoning_content."""
        hook = _make_model_replace_hook("gpt-4o", "resp-001", is_vllm=True)
        event = b'data: {"id":"x","choices":[{"delta":{"reasoning":"think"}}]}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        delta = data["choices"][0]["delta"]
        assert delta["reasoning_content"] == "think"
        assert "reasoning" not in delta

    def test_vllm_reasoning_content_not_overwritten(self):
        """Existing reasoning_content is kept when both fields present."""
        hook = _make_model_replace_hook("gpt-4o", "resp-001", is_vllm=True)
        event = b'data: {"id":"x","choices":[{"delta":{"reasoning":"a","reasoning_content":"b"}}]}\n\n'
        result = hook(event)
        data = orjson.loads(result.split(b"data: ")[1].strip())
        delta = data["choices"][0]["delta"]
        assert delta["reasoning_content"] == "b"
        assert delta["reasoning"] == "a"
