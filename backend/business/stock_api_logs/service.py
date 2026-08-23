"""StockApi call-log browsing service."""

from __future__ import annotations

from backend.business.stock_api_logs.dto import (
    StockApiCallLogPageDTO,
    to_log_dto,
    to_summary_dto,
)
from backend.business.stock_api_logs.ports import StockApiCallLogRepositoryPort
from backend.business.stock_api_logs.queries import ListStockApiLogsQuery


class StockApiLogService:
    def __init__(self, repository: StockApiCallLogRepositoryPort) -> None:
        self._repository = repository

    async def list_logs(self, query: ListStockApiLogsQuery) -> StockApiCallLogPageDTO:
        items = await self._repository.list(
            limit=query.limit,
            offset=query.offset,
            tool_source=query.tool_source,
            tool_id=query.tool_id,
            status=query.status,
        )
        summary = await self._repository.summarize(
            tool_source=query.tool_source,
            tool_id=query.tool_id,
            status=query.status,
        )
        return StockApiCallLogPageDTO(
            items=tuple(to_log_dto(item) for item in items),
            total=summary.total_calls,
            summary=to_summary_dto(summary),
        )
