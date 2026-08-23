"""Shared JSON-safe value coercion used across stages and tool loops.

Any non-primitive, non-container value is stringified so the result can be
serialized by ``json.dumps(..., default=str)`` callers without surprise.
"""

from __future__ import annotations

from typing import Any


def json_safe(value: Any) -> Any:
    """Recursively coerce a value into JSON-serializable primitives."""

    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
    return str(value)
