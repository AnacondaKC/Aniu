"""Tool side-effect levels used for stage authorization."""

from __future__ import annotations

from enum import StrEnum


class SideEffectLevel(StrEnum):
    NONE = "none"
    READ = "read"
    WRITE = "write"


def coerce_side_effect_level(value: object | None) -> SideEffectLevel:
    if value is None:
        return SideEffectLevel.READ
    text = str(value).strip().lower()
    try:
        return SideEffectLevel(text)
    except ValueError:
        return SideEffectLevel.READ


__all__ = ["SideEffectLevel", "coerce_side_effect_level"]
