"""Trading domain value objects and validators."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from enum import Enum

_SYMBOL_PATTERN = re.compile(r"^\d{6}$")


def utc_now() -> datetime:
    """Return the current UTC time."""

    return datetime.now(tz=UTC)


def ensure_non_empty_str(value: str, field_name: str) -> str:
    """Ensure a string field is present after trimming whitespace."""

    trimmed = value.strip()
    if not trimmed:
        raise ValueError(f"{field_name} must not be empty")
    return trimmed


def ensure_positive_int(value: int, field_name: str) -> int:
    """Ensure an integer field is positive."""

    if isinstance(value, bool) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def coerce_enum[EnumT: Enum](
    value: EnumT | str,
    enum_cls: type[EnumT],
    field_name: str,
) -> EnumT:
    """Accept enum members and raw string values."""

    if isinstance(value, enum_cls):
        return value

    try:
        return enum_cls(value)
    except ValueError as exc:
        allowed_values = ", ".join(member.value for member in enum_cls)
        raise ValueError(f"{field_name} must be one of: {allowed_values}") from exc


def validate_symbol(symbol: str) -> str:
    """Validate a 6-digit A-share symbol."""

    normalized = ensure_non_empty_str(symbol, "symbol")
    if not _SYMBOL_PATTERN.fullmatch(normalized):
        raise ValueError("symbol must be a 6-digit stock code")
    return normalized


def validate_quantity(quantity: int) -> int:
    """Validate A-share board lot quantity."""

    ensure_positive_int(quantity, "quantity")
    if quantity < 100 or quantity % 100 != 0:
        raise ValueError("quantity must be at least 100 and a multiple of 100")
    return quantity
