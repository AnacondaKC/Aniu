"""Shared MX integration models and serialization utilities."""

from __future__ import annotations

import asyncio
import math
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal
from uuid import uuid4

from backend.business.shared.stock_api_source import (
    STOCK_API_SOURCE_UNKNOWN,
    StockApiCallSource,
    StockApiProvider,
    record_stock_api_call,
)

type StockApiErrorCategory = Literal[
    "timeout",
    "network",
    "rate_limited",
    "upstream_http",
    "invalid_response",
    "business_failure",
    "cancelled",
    "unknown",
]


@dataclass(frozen=True, slots=True)
class StockApiCall:
    """Sanitized metadata for one upstream StockApi request."""

    provider: StockApiProvider
    operation_id: str
    endpoint: str
    method: str
    parameters: object
    status: str
    status_code: int | None
    duration_ms: int
    response_size_bytes: int | None = None
    response_characters: int | None = None
    error_category: StockApiErrorCategory | None = None
    error_message: str | None = None
    source: StockApiCallSource = STOCK_API_SOURCE_UNKNOWN
    interface_name: str | None = None
    interface_identifier: str | None = None
    call_id: str = field(default_factory=lambda: uuid4().hex)


StockApiCallLogger = Callable[[StockApiCall], Awaitable[None]]


async def emit_stock_api_call_log(
    logger: StockApiCallLogger | None,
    call: StockApiCall,
) -> None:
    """Persist call metadata without masking the upstream request outcome."""

    record_stock_api_call(call)
    if logger is None:
        return
    try:
        await logger(call)
    except Exception:
        return


def active_exception_message() -> str | None:
    """Describe an exception still propagating through a finally block."""

    active_exception = sys.exc_info()[1]
    if active_exception is None:
        return None
    if isinstance(active_exception, asyncio.CancelledError):
        return "request cancelled"
    return str(active_exception) or type(active_exception).__name__


def dataframe_payload(value: object) -> dict[str, object]:
    """Convert a pandas-like DataFrame without importing pandas in core installs."""

    columns = getattr(value, "columns", None)
    itertuples = getattr(value, "itertuples", None)
    if columns is None or not callable(itertuples):
        raise TypeError("provider result is not a DataFrame-like object")
    return {
        "columns": [str(column) for column in columns],
        "rows": [
            [json_safe(cell) for cell in row]
            for row in itertuples(index=False, name=None)
        ],
    }


def json_safe(value: object) -> object:
    """Convert provider values to JSON-safe primitives for tools and logs."""

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [json_safe(item) for item in value]
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return json_safe(item())
        except (TypeError, ValueError):
            pass
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return str(isoformat())
        except (TypeError, ValueError):
            pass
    if hasattr(value, "columns") and hasattr(value, "itertuples"):
        return dataframe_payload(value)
    return str(value)


__all__ = [
    "StockApiErrorCategory",
    "StockApiCallLogger",
    "StockApiProvider",
    "active_exception_message",
    "dataframe_payload",
    "emit_stock_api_call_log",
    "json_safe",
]
