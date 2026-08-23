"""Stable JSON projection for model-facing business payloads."""

from __future__ import annotations

import json


def serialize_context(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


__all__ = ["serialize_context"]
