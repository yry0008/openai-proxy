import json


__all__ = [
    "_transform_messages_for_template",
    "_transform_tools_for_template",
    "_get_requested_output_tokens",
    "_build_context_length_error",
]


def _transform_messages_for_template(messages: list[dict]) -> list[dict]:
    result = []
    for msg in messages:
        d = {"role": msg.get("role", "user"), "content": msg.get("content") or ""}
        if "name" in msg:
            d["name"] = msg["name"]
        if "tool_call_id" in msg:
            d["tool_call_id"] = msg["tool_call_id"]
        if "tool_calls" in msg:
            transformed_calls = []
            for tc in msg["tool_calls"]:
                func = tc.get("function")
                if not isinstance(func, dict):
                    transformed_calls.append(tc)
                    continue
                try:
                    parsed_args = json.loads(func["arguments"])
                except (json.JSONDecodeError, TypeError, KeyError):
                    parsed_args = func.get("arguments")
                transformed_calls.append(
                    {
                        "type": "function",
                        "id": tc.get("id"),
                        "function": {
                            "name": func.get("name", ""),
                            "arguments": parsed_args,
                        },
                    }
                )
            d["tool_calls"] = transformed_calls
        if "reasoning_content" in msg:
            d["reasoning_content"] = msg["reasoning_content"]
        result.append(d)
    return result


def _transform_tools_for_template(tools: list[dict] | None) -> list[dict] | None:
    if not tools:
        return None
    result = []
    for t in tools:
        func = t.get("function")
        if isinstance(func, dict):
            result.append({k: v for k, v in func.items() if v is not None})
        else:
            result.append(t)
    return result


def _get_requested_output_tokens(req_data: dict) -> int:
    requested_output_tokens = req_data.get("max_tokens")
    if requested_output_tokens is None:
        requested_output_tokens = req_data.get("max_completion_tokens", 16)
    if requested_output_tokens is None:
        requested_output_tokens = 16

    try:
        return max(0, int(requested_output_tokens))
    except (TypeError, ValueError):
        return 16


def _build_context_length_error(
    model_max_context: int,
    requested_output_tokens: int,
    input_tokens: int,
) -> str:
    total_tokens = input_tokens + requested_output_tokens
    return (
        f"This model's maximum context length is {model_max_context} tokens. "
        f"However, you requested {requested_output_tokens} output tokens and your prompt contains at least "
        f"{input_tokens} input tokens, for a total of at least {total_tokens} tokens. "
        "Please reduce the length of the input prompt or the number of requested output tokens."
    )
