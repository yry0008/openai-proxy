"""MODEL_MAP 环境变量解析。

MODEL_MAP 是一个 JSON 对象，例如::

    MODEL_MAP={"claude-sonnet-4-5":"claude-sonnet-4-5-20250929"}

表示客户端请求 claude-sonnet-4-5 时，实际转发给上游的模型为
claude-sonnet-4-5-20250929，但返回给客户端的响应中 model 字段
仍为客户端请求的原始名称（由 main.py 中的响应重写逻辑保证）。
"""

import json
import logging

logger = logging.getLogger(__name__)


def load_model_map(raw: str) -> dict:
    """解析 MODEL_MAP 环境变量为 dict。任何非法输入都返回空 dict。"""
    if not raw or not raw.strip():
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning(f"MODEL_MAP 不是合法 JSON，已忽略: {e}")
        return {}
    if not isinstance(data, dict):
        logger.warning("MODEL_MAP 必须是 JSON 对象，已忽略")
        return {}
    return data
