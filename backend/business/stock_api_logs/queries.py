"""Queries for data-interface tool invocation log browsing."""

from __future__ import annotations

from dataclasses import dataclass

from backend.business.stock_api_logs.catalog import StockApiToolSource


@dataclass(frozen=True, slots=True)
class ListStockApiLogsQuery:
    limit: int = 50
    offset: int = 0
    tool_source: StockApiToolSource | None = None
    tool_id: str | None = None
    status: str | None = None
