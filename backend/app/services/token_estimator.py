from __future__ import annotations

import json
from typing import Any


def _normalize_content_length(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, str):
        return len(value)
    try:
        return len(json.dumps(value, ensure_ascii=False))
    except Exception:
        return len(str(value))


def estimate_text_tokens(text: str | None) -> int:
    content = str(text or "")
    if not content:
        return 0
    return max(1, (len(content) + 3) // 4)


def estimate_messages_tokens(messages: list[dict[str, Any]]) -> int:
    total_chars = 0
    structural_overhead = 0

    for message in messages:
        if not isinstance(message, dict):
            continue
        total_chars += _normalize_content_length(message.get("content"))
        total_chars += _normalize_content_length(message.get("tool_calls"))
        total_chars += len(str(message.get("role") or ""))
        structural_overhead += 16

    return max(0, ((total_chars + structural_overhead) + 3) // 4)
