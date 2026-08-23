"""JSON coercion kept local to the independent agent package."""

from __future__ import annotations

import json
from typing import Any


def json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
    return str(value)


def serialize_context(value: object) -> str:
    """Serialize a value exactly as it is written to Agent context."""

    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


__all__ = ["json_safe", "serialize_context"]
