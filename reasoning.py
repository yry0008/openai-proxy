"""Reasoning parser logic for handling model reasoning content."""

from typing import Callable

THINK_START = "<think>"
THINK_END = "</think>"


def _get_reasoning_parser(reasoning_type: str) -> Callable[[list[dict]], list[dict]]:
    if reasoning_type == "":
        return _reasoning_passthrough
    if reasoning_type == "qwen":
        return _reasoning_discard
    if reasoning_type in ("kimi_k2", "minimax_m2", "glm"):
        return _reasoning_merge
    raise ValueError(f"Unknown REASONING_TYPE: {reasoning_type}")


def _reasoning_passthrough(messages: list[dict]) -> list[dict]:
    return messages


def _reasoning_discard(messages: list[dict]) -> list[dict]:
    return [
        {k: v for k, v in msg.items() if k != "reasoning_content"} for msg in messages
    ]


def _reasoning_merge(messages: list[dict]) -> list[dict]:
    import json

    result = []
    for msg in messages:
        msg = dict(msg)
        reasoning_content = msg.pop("reasoning_content", None)
        if reasoning_content and msg.get("role") == "assistant":
            original = msg.get("content", "") or ""
            if not isinstance(original, str):
                original = json.dumps(original, ensure_ascii=False)
            if original:
                msg["content"] = (
                    f"{THINK_START}{reasoning_content}{THINK_END}\n{original}"
                )
            else:
                msg["content"] = f"{THINK_START}{reasoning_content}{THINK_END}"
        result.append(msg)
    return result


__all__ = [
    "THINK_START",
    "THINK_END",
    "_get_reasoning_parser",
    "_reasoning_passthrough",
    "_reasoning_discard",
    "_reasoning_merge",
]
