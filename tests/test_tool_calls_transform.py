"""Smoke tests for tool_calls and message transformation edge cases.

Verifies _transform_messages_for_template and _transform_tools_for_template
handle non-standard tool formats (built-in search, string function, etc.)
without crashing.
"""
import json

from utils import _transform_messages_for_template, _transform_tools_for_template


class TestTransformToolsForTemplate:
    def test_standard_function_tool(self):
        tools = [
            {"type": "function", "function": {"name": "get_weather", "description": "Get weather", "parameters": {}}}
        ]
        result = _transform_tools_for_template(tools)
        assert result == [{"name": "get_weather", "description": "Get weather", "parameters": {}}]

    def test_web_search_tool_passes_through(self):
        tools = [
            {"type": "web_search", "web_search": {"enable": True, "search_mode": "performance_first"}}
        ]
        result = _transform_tools_for_template(tools)
        assert result == tools

    def test_mixed_tools(self):
        tools = [
            {"type": "web_search", "web_search": {"enable": True}},
            {"type": "function", "function": {"name": "calc", "parameters": {}}},
        ]
        result = _transform_tools_for_template(tools)
        assert result[0] == tools[0]
        assert result[1] == {"name": "calc", "parameters": {}}

    def test_none_tools(self):
        assert _transform_tools_for_template(None) is None
        assert _transform_tools_for_template([]) is None

    def test_function_none_values_stripped(self):
        tools = [
            {"type": "function", "function": {"name": "x", "description": None, "parameters": {}}}
        ]
        result = _transform_tools_for_template(tools)
        assert result == [{"name": "x", "parameters": {}}]


class TestTransformMessagesToolCalls:
    def test_standard_tool_calls(self):
        messages = [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "get_weather", "arguments": json.dumps({"city": "NYC"})},
                    }
                ],
            }
        ]
        result = _transform_messages_for_template(messages)
        tc = result[0]["tool_calls"][0]
        assert tc["function"]["name"] == "get_weather"
        assert tc["function"]["arguments"] == {"city": "NYC"}

    def test_tool_calls_no_function_key(self):
        messages = [
            {
                "role": "assistant",
                "content": "Searching...",
                "tool_calls": [
                    {"id": "call_1", "type": "web_search", "web_search": {"query": "weather"}}
                ],
            }
        ]
        result = _transform_messages_for_template(messages)
        assert result[0]["tool_calls"][0] == {"id": "call_1", "type": "web_search", "web_search": {"query": "weather"}}

    def test_tool_calls_function_is_string(self):
        messages = [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {"id": "call_1", "type": "function", "function": "web_search"}
                ],
            }
        ]
        result = _transform_messages_for_template(messages)
        assert result[0]["tool_calls"][0] == {"id": "call_1", "type": "function", "function": "web_search"}

    def test_tool_calls_function_is_none(self):
        messages = [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {"id": "call_1", "type": "function", "function": None}
                ],
            }
        ]
        result = _transform_messages_for_template(messages)
        assert result[0]["tool_calls"][0] == {"id": "call_1", "type": "function", "function": None}

    def test_mixed_tool_calls_types(self):
        messages = [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {"id": "call_1", "type": "web_search", "web_search": {"query": "news"}},
                    {"id": "call_2", "type": "function", "function": {"name": "summarize", "arguments": "{}"}},
                    {"id": "call_3", "type": "function", "function": "raw_string"},
                ],
            }
        ]
        result = _transform_messages_for_template(messages)
        calls = result[0]["tool_calls"]
        assert calls[0]["type"] == "web_search"
        assert calls[1]["function"]["name"] == "summarize"
        assert calls[1]["function"]["arguments"] == {}
        assert calls[2]["function"] == "raw_string"

    def test_tool_result_message(self):
        messages = [
            {"role": "tool", "tool_call_id": "call_1", "content": "Sunny 25C"}
        ]
        result = _transform_messages_for_template(messages)
        assert result[0]["role"] == "tool"
        assert result[0]["tool_call_id"] == "call_1"
        assert result[0]["content"] == "Sunny 25C"

    def test_tool_calls_invalid_arguments_json(self):
        messages = [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "x", "arguments": "not valid json{"},
                    }
                ],
            }
        ]
        result = _transform_messages_for_template(messages)
        tc = result[0]["tool_calls"][0]
        assert tc["function"]["arguments"] == {}

    def test_tool_calls_missing_arguments_key(self):
        messages = [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {"id": "call_1", "type": "function", "function": {"name": "x"}}
                ],
            }
        ]
        result = _transform_messages_for_template(messages)
        tc = result[0]["tool_calls"][0]
        assert tc["function"]["arguments"] == {}

    def test_reasoning_content_preserved(self):
        messages = [
            {
                "role": "assistant",
                "content": "Answer",
                "reasoning_content": "Thinking...",
                "tool_calls": [
                    {"id": "c1", "type": "function", "function": {"name": "f", "arguments": "{}"}}
                ],
            }
        ]
        result = _transform_messages_for_template(messages)
        assert result[0]["reasoning_content"] == "Thinking..."

    def test_empty_tool_calls_list(self):
        messages = [{"role": "assistant", "content": "Hi", "tool_calls": []}]
        result = _transform_messages_for_template(messages)
        assert result[0]["tool_calls"] == []

    def test_no_tool_calls_key(self):
        messages = [{"role": "user", "content": "Hello"}]
        result = _transform_messages_for_template(messages)
        assert "tool_calls" not in result[0]

    def test_arguments_already_dict(self):
        messages = [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "c1",
                        "type": "function",
                        "function": {"name": "f", "arguments": {"key": "val"}},
                    }
                ],
            }
        ]
        result = _transform_messages_for_template(messages)
        assert result[0]["tool_calls"][0]["function"]["arguments"] == {"key": "val"}


class TestTransformToolsNonDictFunction:
    def test_function_str_skipped(self):
        tools = [{"type": "function", "function": "get_weather"}]
        result = _transform_tools_for_template(tools)
        assert result == []

    def test_function_int_skipped(self):
        tools = [{"type": "function", "function": 123}]
        result = _transform_tools_for_template(tools)
        assert result == []

    def test_function_list_skipped(self):
        tools = [{"type": "function", "function": ["a"]}]
        result = _transform_tools_for_template(tools)
        assert result == []

    def test_function_none_preserved(self):
        tools = [{"type": "function", "function": None}]
        result = _transform_tools_for_template(tools)
        assert result == tools

    def test_non_dict_tool_entry_skipped(self):
        tools = ["just_a_string", {"type": "function", "function": {"name": "f", "parameters": {}}}]
        result = _transform_tools_for_template(tools)
        assert result == [{"name": "f", "parameters": {}}]

    def test_malicious_function_str_does_not_leak(self):
        tools = [
            {"type": "function", "function": "bad"},
            {"type": "function", "function": {"name": "good", "parameters": {}}},
        ]
        result = _transform_tools_for_template(tools)
        assert result == [{"name": "good", "parameters": {}}]


class TestTransformMessagesNonDictArguments:
    def test_arguments_json_list_becomes_empty_dict(self):
        messages = [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {"id": "c1", "type": "function",
                     "function": {"name": "f", "arguments": "[1,2]"}}
                ],
            }
        ]
        result = _transform_messages_for_template(messages)
        assert result[0]["tool_calls"][0]["function"]["arguments"] == {}

    def test_arguments_json_int_becomes_empty_dict(self):
        messages = [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {"id": "c1", "type": "function",
                     "function": {"name": "f", "arguments": "42"}}
                ],
            }
        ]
        result = _transform_messages_for_template(messages)
        assert result[0]["tool_calls"][0]["function"]["arguments"] == {}

    def test_arguments_none_becomes_empty_dict(self):
        messages = [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {"id": "c1", "type": "function",
                     "function": {"name": "f", "arguments": None}}
                ],
            }
        ]
        result = _transform_messages_for_template(messages)
        assert result[0]["tool_calls"][0]["function"]["arguments"] == {}

    def test_arguments_json_dict_still_parsed(self):
        messages = [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {"id": "c1", "type": "function",
                     "function": {"name": "f", "arguments": '{"a": 1}'}}
                ],
            }
        ]
        result = _transform_messages_for_template(messages)
        assert result[0]["tool_calls"][0]["function"]["arguments"] == {"a": 1}
