"""Persistence tests for data-interface tool invocation logs."""

from __future__ import annotations

import pytest

from backend.business.stock_api_logs.models import StockApiCallLogSummary
from backend.infra.repositories.stock_api_call_log_repo import (
    StockApiCallLogRecord,
    StockApiCallLogRepository,
)


@pytest.mark.asyncio
async def test_tool_call_logs_can_be_filtered_and_summarized(session) -> None:
    repository = StockApiCallLogRepository(session)
    await repository.append(
        StockApiCallLogRecord(
            tool_source="mx",
            tool_id="data",
            parameters={"query": "贵州茅台行情"},
            status="success",
            duration_ms=25,
            response_characters=1_234,
        )
    )
    await repository.append(
        StockApiCallLogRecord(
            tool_source="public",
            tool_id="stock_quote",
            parameters={"symbols": ["600519.SH"]},
            status="failed",
            duration_ms=50,
            error_message="公开数据暂时不可用。",
        )
    )
    await session.commit()

    items = await repository.list(
        limit=10,
        offset=0,
        tool_source="mx",
        tool_id="data",
    )
    summary = await repository.summarize()

    assert len(items) == 1
    assert items[0].tool_source == "mx"
    assert items[0].tool_id == "data"
    assert items[0].parameters == {"query": "贵州茅台行情"}
    assert items[0].response_characters == 1_234
    assert summary == StockApiCallLogSummary(
        total_calls=2,
        success_calls=1,
        failed_calls=1,
        average_duration_ms=38,
    )


@pytest.mark.asyncio
async def test_tool_call_log_rejects_a_non_catalog_tool(session) -> None:
    repository = StockApiCallLogRepository(session)

    with pytest.raises(ValueError, match="unsupported data-interface tool log"):
        await repository.append(
            StockApiCallLogRecord(
                tool_source="public",
                tool_id="quote.snapshot",
                parameters={},
                status="success",
                duration_ms=1,
            )
        )
