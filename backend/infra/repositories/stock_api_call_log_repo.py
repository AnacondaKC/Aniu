"""Persistence adapter for user-visible data-interface tool invocation logs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.business.stock_api_logs.catalog import (
    STOCK_API_TOOL_SOURCES,
    StockApiToolSource,
    stock_api_tool_descriptor_by_id,
)
from backend.business.stock_api_logs.models import (
    StockApiCallLog,
    StockApiCallLogSummary,
)
from backend.infra.db.models import StockApiCallLogModel

_AGENT_TOOL_CALL_LOG_SOURCE = "agent_tool"


@dataclass(frozen=True, slots=True)
class StockApiCallLogRecord:
    tool_source: StockApiToolSource
    tool_id: str
    parameters: object
    status: str
    duration_ms: int
    response_characters: int | None = None
    error_category: str | None = None
    error_message: str | None = None


class StockApiCallLogRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def append(self, record: StockApiCallLogRecord) -> None:
        if stock_api_tool_descriptor_by_id(record.tool_source, record.tool_id) is None:
            raise ValueError("unsupported data-interface tool log")
        log = StockApiCallLogModel(
            source=_AGENT_TOOL_CALL_LOG_SOURCE,
            provider=record.tool_source,
            operation_id=record.tool_id,
            parameters_json=json.dumps(
                record.parameters,
                ensure_ascii=False,
                separators=(",", ":"),
                default=str,
            ),
            status=record.status,
            duration_ms=max(0, record.duration_ms),
            response_characters=(
                max(0, record.response_characters)
                if record.response_characters is not None
                else None
            ),
            error_category=(
                (record.error_category or "unknown")
                if record.status == "failed"
                else None
            ),
            error_message=record.error_message,
        )
        self._session.add(log)
        await self._session.flush()

    async def list(
        self,
        *,
        limit: int,
        offset: int,
        tool_source: StockApiToolSource | None = None,
        tool_id: str | None = None,
        status: str | None = None,
    ) -> list[StockApiCallLog]:
        statement = select(StockApiCallLogModel).where(
            StockApiCallLogModel.source == _AGENT_TOOL_CALL_LOG_SOURCE
        )
        if tool_source is not None:
            statement = statement.where(StockApiCallLogModel.provider == tool_source)
        else:
            statement = statement.where(
                StockApiCallLogModel.provider.in_(STOCK_API_TOOL_SOURCES)
            )
        if tool_id is not None:
            statement = statement.where(StockApiCallLogModel.operation_id == tool_id)
        if status is not None:
            statement = statement.where(StockApiCallLogModel.status == status)
        statement = statement.order_by(StockApiCallLogModel.id.desc())
        statement = statement.limit(limit).offset(offset)
        rows = list((await self._session.scalars(statement)).all())
        return [_to_model(row) for row in rows]

    async def summarize(
        self,
        *,
        tool_source: StockApiToolSource | None = None,
        tool_id: str | None = None,
        status: str | None = None,
    ) -> StockApiCallLogSummary:
        filters = [StockApiCallLogModel.source == _AGENT_TOOL_CALL_LOG_SOURCE]
        if tool_source is not None:
            filters.append(StockApiCallLogModel.provider == tool_source)
        else:
            filters.append(StockApiCallLogModel.provider.in_(STOCK_API_TOOL_SOURCES))
        if tool_id is not None:
            filters.append(StockApiCallLogModel.operation_id == tool_id)
        if status is not None:
            filters.append(StockApiCallLogModel.status == status)
        statement = select(
            func.count(StockApiCallLogModel.id),
            func.sum(case((StockApiCallLogModel.status == "success", 1), else_=0)),
            func.avg(StockApiCallLogModel.duration_ms),
        ).where(*filters)
        total, success, average = (await self._session.execute(statement)).one()
        total_count = int(total or 0)
        success_count = int(success or 0)
        return StockApiCallLogSummary(
            total_calls=total_count,
            success_calls=success_count,
            failed_calls=max(0, total_count - success_count),
            average_duration_ms=round(float(average or 0)),
        )


def _to_model(row: StockApiCallLogModel) -> StockApiCallLog:
    try:
        parameters = json.loads(row.parameters_json)
    except (TypeError, ValueError):
        parameters = row.parameters_json
    created_at = datetime.fromisoformat(row.created_at)
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)
    return StockApiCallLog(
        id=row.id,
        tool_source=cast(StockApiToolSource, row.provider),
        tool_id=row.operation_id,
        parameters=parameters,
        status=row.status,
        duration_ms=row.duration_ms,
        response_characters=row.response_characters,
        error_category=row.error_category,
        error_message=row.error_message,
        created_at=created_at,
    )


__all__ = ["StockApiCallLogRecord", "StockApiCallLogRepository"]
