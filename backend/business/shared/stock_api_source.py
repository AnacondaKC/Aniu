"""Task-local source markers and provider types for stock-data upstream requests."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Literal

StockApiCallSource = Literal["overview_refresh", "run", "unknown"]
StockApiProvider = Literal["mx", "eastmoney", "tencent", "sina"]
STOCK_API_PROVIDERS: tuple[StockApiProvider, ...] = (
    "mx",
    "eastmoney",
    "tencent",
    "sina",
)
PUBLIC_STOCK_OPERATION_IDS = frozenset(
    {
        "quote.snapshot",
        "chart.kline",
        "chart.intraday",
        "ranking.stocks",
        "ranking.sectors",
        "money_flow.stock_history",
        "money_flow.stock_intraday",
        "money_flow.sector",
        "money_flow.connect",
        "fundamentals.financials",
        "fundamentals.shareholders",
        "fundamentals.valuation",
        "fundamentals.industry_comparison",
        "fundamentals.operating_indicators",
        "research.market_reports",
        "research.stock_reports",
        "research.forecast",
        "research.ratings",
        "news.feed",
        "news.stock_news",
        "news.announcements",
        "news.search",
    }
)
PUBLIC_STOCK_UNKNOWN_OPERATION_ID = "public.request"


@dataclass(frozen=True, slots=True)
class StockApiToolContext:
    run_id: int
    stage_name: str
    tool_call_id: str
    tool_name: str


STOCK_API_SOURCE_OVERVIEW_REFRESH: StockApiCallSource = "overview_refresh"
STOCK_API_SOURCE_RUN: StockApiCallSource = "run"
STOCK_API_SOURCE_UNKNOWN: StockApiCallSource = "unknown"

_current_stock_api_source: ContextVar[StockApiCallSource] = ContextVar(
    "current_stock_api_source",
    default=STOCK_API_SOURCE_UNKNOWN,
)
_current_stock_api_tool_context: ContextVar[StockApiToolContext | None] = ContextVar(
    "current_stock_api_tool_context",
    default=None,
)
_current_stock_api_call_records: ContextVar[list[dict[str, Any]] | None] = ContextVar(
    "current_stock_api_call_records",
    default=None,
)


def normalize_public_stock_operation_id(value: str) -> str:
    """Keep public traces and logs on the documented business-operation contract."""

    return (
        value
        if value in PUBLIC_STOCK_OPERATION_IDS
        else PUBLIC_STOCK_UNKNOWN_OPERATION_ID
    )


@contextmanager
def stock_api_tool_context(
    *,
    run_id: int,
    stage_name: str,
    tool_call_id: str,
    tool_name: str,
) -> Iterator[list[dict[str, Any]]]:
    context_token = _current_stock_api_tool_context.set(
        StockApiToolContext(
            run_id=run_id,
            stage_name=stage_name,
            tool_call_id=tool_call_id,
            tool_name=tool_name,
        )
    )
    records: list[dict[str, Any]] = []
    records_token = _current_stock_api_call_records.set(records)
    try:
        yield records
    finally:
        _current_stock_api_call_records.reset(records_token)
        _current_stock_api_tool_context.reset(context_token)


def record_stock_api_call(call: object) -> None:
    records = _current_stock_api_call_records.get()
    context = _current_stock_api_tool_context.get()
    if records is None or context is None:
        return
    records.append(
        {
            "call_id": str(getattr(call, "call_id", "")),
            "run_id": context.run_id,
            "stage_name": context.stage_name,
            "tool_call_id": context.tool_call_id,
            "tool_name": context.tool_name,
            "provider": str(getattr(call, "provider", "mx")),
            "operation_id": str(getattr(call, "operation_id", "")),
            "interface_name": str(getattr(call, "interface_name", None) or ""),
            "interface_identifier": str(
                getattr(call, "interface_identifier", None) or ""
            ),
            "parameters": getattr(call, "parameters", None),
            "status": str(getattr(call, "status", "failed")),
            "duration_ms": int(getattr(call, "duration_ms", 0) or 0),
            "response_characters": getattr(call, "response_characters", None),
            "error_category": getattr(call, "error_category", None),
            "error_message": getattr(call, "error_message", None),
        }
    )


def current_stock_api_source() -> StockApiCallSource:
    return _current_stock_api_source.get()


@contextmanager
def stock_api_source(source: StockApiCallSource) -> Iterator[None]:
    token = _current_stock_api_source.set(source)
    try:
        yield
    finally:
        _current_stock_api_source.reset(token)


__all__ = [
    "PUBLIC_STOCK_OPERATION_IDS",
    "PUBLIC_STOCK_UNKNOWN_OPERATION_ID",
    "STOCK_API_PROVIDERS",
    "STOCK_API_SOURCE_OVERVIEW_REFRESH",
    "STOCK_API_SOURCE_RUN",
    "STOCK_API_SOURCE_UNKNOWN",
    "StockApiCallSource",
    "StockApiProvider",
    "StockApiToolContext",
    "current_stock_api_source",
    "normalize_public_stock_operation_id",
    "record_stock_api_call",
    "stock_api_source",
    "stock_api_tool_context",
]
