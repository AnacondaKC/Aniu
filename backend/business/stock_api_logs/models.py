"""Business models for user-visible data-interface tool invocation logs."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime

from backend.business.stock_api_logs.catalog import StockApiToolSource


@dataclass(frozen=True, slots=True)
class StockApiToolCall:
    """Final outcome of one Agent call to a directory tool."""

    tool_source: StockApiToolSource
    tool_id: str
    parameters: object
    status: str
    duration_ms: int
    response_characters: int | None = None
    error_category: str | None = None
    error_message: str | None = None


StockApiToolCallLogger = Callable[[StockApiToolCall], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class StockApiCallLog:
    id: int
    tool_source: StockApiToolSource
    tool_id: str
    parameters: object
    status: str
    duration_ms: int
    response_characters: int | None
    error_category: str | None
    error_message: str | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class StockApiCallLogSummary:
    total_calls: int
    success_calls: int
    failed_calls: int
    average_duration_ms: int
