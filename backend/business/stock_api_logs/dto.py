"""API-facing DTOs for data-interface tool invocation logs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from backend.business.stock_api_logs.catalog import (
    StockApiToolSource,
    stock_api_tool_descriptor_by_id,
)
from backend.business.stock_api_logs.models import (
    StockApiCallLog,
    StockApiCallLogSummary,
)


@dataclass(frozen=True, slots=True)
class StockApiCallLogDTO:
    id: int
    tool_source: StockApiToolSource
    tool_id: str
    tool_name: str
    parameters: object
    status: str
    duration_ms: int
    response_characters: int | None
    error_category: str | None
    error_message: str | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class StockApiCallLogSummaryDTO:
    total_calls: int
    success_calls: int
    failed_calls: int
    average_duration_ms: int


@dataclass(frozen=True, slots=True)
class StockApiCallLogPageDTO:
    items: tuple[StockApiCallLogDTO, ...]
    total: int
    summary: StockApiCallLogSummaryDTO


def to_log_dto(item: StockApiCallLog) -> StockApiCallLogDTO:
    descriptor = stock_api_tool_descriptor_by_id(item.tool_source, item.tool_id)
    return StockApiCallLogDTO(
        id=item.id,
        tool_source=item.tool_source,
        tool_id=item.tool_id,
        tool_name=descriptor.tool_name if descriptor is not None else item.tool_id,
        parameters=item.parameters,
        status=item.status,
        duration_ms=item.duration_ms,
        response_characters=item.response_characters,
        error_category=item.error_category,
        error_message=item.error_message,
        created_at=item.created_at,
    )


def to_summary_dto(item: StockApiCallLogSummary) -> StockApiCallLogSummaryDTO:
    return StockApiCallLogSummaryDTO(
        total_calls=item.total_calls,
        success_calls=item.success_calls,
        failed_calls=item.failed_calls,
        average_duration_ms=item.average_duration_ms,
    )
