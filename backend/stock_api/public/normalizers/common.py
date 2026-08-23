"""Shared helpers for public stock-data normalizers."""

from __future__ import annotations

import html
import math
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from backend.stock_api.public.contracts import normalize_index_symbol, normalize_symbol
from backend.stock_api.public.errors import NoStockData, UpstreamUnavailable

JsonRecord = dict[str, object]


@dataclass(frozen=True, slots=True)
class NormalizedData:
    data: dict[str, object]
    degraded: bool = False
    warnings: tuple[str, ...] = ()


def as_record(value: object) -> JsonRecord | None:
    return value if isinstance(value, dict) else None


def text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value).strip()
    return ""


def number(value: object) -> float | int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return value if math.isfinite(value) else None
    if not isinstance(value, str):
        return None
    candidate = value.strip().replace(",", "")
    if not candidate or candidate in {"-", "--", "N/A", "null"}:
        return None
    try:
        parsed = float(candidate)
    except ValueError:
        return None
    if not math.isfinite(parsed):
        return None
    return int(parsed) if parsed.is_integer() else parsed


def first_value(row: JsonRecord, *keys: str) -> object | None:
    for key in keys:
        value = row.get(key)
        if value is not None and text(value):
            return value
    return None


def first_text(row: JsonRecord, *keys: str) -> str:
    for key in keys:
        value = text(row.get(key))
        if value:
            return value
    return ""


def first_number(row: JsonRecord, *keys: str) -> float | int | None:
    for key in keys:
        value = number(row.get(key))
        if value is not None:
            return value
    return None


def find_rows(value: object, *, depth: int = 0) -> list[JsonRecord] | None:
    """Find the first homogeneous record array in a bounded response tree."""

    if depth > 6:
        return None
    if isinstance(value, list):
        records = [item for item in value if isinstance(item, dict)]
        if records and len(records) == len(value):
            return records
        for item in value:
            nested = find_rows(item, depth=depth + 1)
            if nested is not None:
                return nested
        return None
    if isinstance(value, dict):
        for item in value.values():
            nested = find_rows(item, depth=depth + 1)
            if nested is not None:
                return nested
    return None


def _canonical_market_symbol(value: str) -> str | None:
    for normalize in (normalize_symbol, normalize_index_symbol):
        try:
            return normalize(value)
        except Exception:
            continue
    return None


def canonical_symbol(value: object, *, market: object | None = None) -> str | None:
    raw = text(value).lower()
    if re.fullmatch(r"\d{6}\.(?:sh|sz)", raw):
        return _canonical_market_symbol(raw.upper())
    if raw.startswith(("sh", "sz")) and len(raw) == 8:
        code, exchange = raw[2:], raw[:2].upper()
        return _canonical_market_symbol(f"{code}.{exchange}")
    if not re.fullmatch(r"\d{6}", raw):
        return None
    market_text = text(market)
    if market_text in {"1", "SH", "sh"}:
        exchange = "SH"
    elif market_text in {"0", "SZ", "sz"}:
        exchange = "SZ"
    elif raw.startswith(("600", "601", "603", "605", "688")):
        exchange = "SH"
    else:
        exchange = "SZ"
    return _canonical_market_symbol(f"{raw}.{exchange}")


def sampled[T](items: Sequence[T], limit: int) -> tuple[list[T], bool]:
    values = list(items)
    if len(values) <= limit:
        return values, False
    if limit == 1:
        return [values[-1]], True
    last = len(values) - 1
    return [values[round(index * last / (limit - 1))] for index in range(limit)], True


def require_items(items: Sequence[object], label: str) -> None:
    if not items:
        raise NoStockData(f"{label}没有可用记录。")


def clean_text(value: object, maximum: int | None) -> tuple[str, bool]:
    normalized = re.sub(r"\s+", " ", text(value)).strip()
    normalized = redact_absolute_urls(normalized)
    if maximum is None:
        return normalized, False
    return (
        (normalized, False)
        if len(normalized) <= maximum
        else (normalized[:maximum], True)
    )


def clean_html(value: object, maximum: int) -> tuple[str, bool]:
    source = str(value)
    source = re.sub(r"<script\b[^>]*>[\s\S]*?</script>", " ", source, flags=re.I)
    source = re.sub(r"<style\b[^>]*>[\s\S]*?</style>", " ", source, flags=re.I)
    source = re.sub(r"<[^>]+>", " ", source)
    return clean_text(html.unescape(source), maximum)


def normalize_date(value: object) -> str | None:
    """Normalize provider date/datetime strings to an ISO calendar date."""

    value_text = text(value).replace("/", "-")
    if not value_text:
        return None
    if re.fullmatch(r"\d{8}", value_text):
        value_text = f"{value_text[:4]}-{value_text[4:6]}-{value_text[6:8]}"
    value_text = value_text.split("T", 1)[0].split(" ", 1)[0]
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value_text):
        return None
    try:
        return datetime.strptime(value_text, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def normalize_datetime(value: object) -> str | None:
    """Normalize provider datetimes, treating naive text as Asia/Shanghai."""

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if not math.isfinite(value):
            return None
        timestamp = value / 1000 if abs(value) >= 100_000_000_000 else value
        try:
            return datetime.fromtimestamp(timestamp, tz=UTC).isoformat(
                timespec="seconds"
            )
        except (OverflowError, OSError, ValueError):
            return None
    value_text = text(value).replace("/", "-")
    if not value_text:
        return None
    if re.fullmatch(r"\d{10}|\d{13}", value_text):
        timestamp = float(value_text)
        if len(value_text) == 13:
            timestamp /= 1000
        try:
            return datetime.fromtimestamp(timestamp, tz=UTC).isoformat(
                timespec="seconds"
            )
        except (OverflowError, OSError, ValueError):
            return None
    if re.fullmatch(r"\d{8}", value_text):
        value_text = f"{value_text[:4]}-{value_text[4:6]}-{value_text[6:8]}"
    try:
        parsed = datetime.fromisoformat(value_text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    return parsed.isoformat(timespec="seconds")


def require_valid_items(
    items: Sequence[JsonRecord],
    label: str,
    required_fields: tuple[str, ...],
) -> tuple[list[JsonRecord], int]:
    """Keep valid rows and distinguish empty data from invalid payloads."""

    if not items:
        raise NoStockData(f"{label}没有可用记录。")
    valid = [
        item
        for item in items
        if all(
            item.get(field) is not None and item.get(field) != ""
            for field in required_fields
        )
    ]
    if not valid:
        raise UpstreamUnavailable(f"{label}响应中的关键字段无效。")
    return valid, len(items) - len(valid)


def redact_absolute_urls(value: str) -> str:
    """Remove upstream links from model-facing text while keeping prose."""

    return re.sub(r"https?://[^\s)]+", "[链接已省略]", value)


__all__ = [
    "JsonRecord",
    "NormalizedData",
    "as_record",
    "canonical_symbol",
    "clean_html",
    "clean_text",
    "find_rows",
    "first_number",
    "first_text",
    "first_value",
    "redact_absolute_urls",
    "require_valid_items",
    "normalize_date",
    "normalize_datetime",
    "number",
    "require_items",
    "sampled",
    "text",
]
