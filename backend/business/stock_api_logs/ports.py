"""Ports for data-interface tool invocation log persistence."""

from __future__ import annotations

from typing import Protocol

from backend.business.stock_api_logs.catalog import StockApiToolSource
from backend.business.stock_api_logs.models import (
    StockApiCallLog,
    StockApiCallLogSummary,
)


class StockApiCallLogRepositoryPort(Protocol):
    async def list(
        self,
        *,
        limit: int,
        offset: int,
        tool_source: StockApiToolSource | None = None,
        tool_id: str | None = None,
        status: str | None = None,
    ) -> list[StockApiCallLog]: ...

    async def summarize(
        self,
        *,
        tool_source: StockApiToolSource | None = None,
        tool_id: str | None = None,
        status: str | None = None,
    ) -> StockApiCallLogSummary: ...
