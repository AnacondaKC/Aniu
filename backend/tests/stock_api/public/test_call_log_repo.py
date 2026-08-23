"""Persistence contract for public data-interface tool invocation logs."""

from __future__ import annotations

import pytest

from backend.infra.repositories.stock_api_call_log_repo import (
    StockApiCallLogRecord,
    StockApiCallLogRepository,
)


@pytest.mark.asyncio
async def test_public_tool_call_log_is_persisted_and_filterable(session) -> None:
    repository = StockApiCallLogRepository(session)
    await repository.append(
        StockApiCallLogRecord(
            tool_source="public",
            tool_id="stock_quote",
            parameters={"symbols": ["600519.SH"]},
            status="success",
            duration_ms=20,
        )
    )
    await session.commit()

    items = await repository.list(
        limit=10,
        offset=0,
        tool_source="public",
        tool_id="stock_quote",
    )

    assert len(items) == 1
    assert items[0].tool_source == "public"
    assert items[0].tool_id == "stock_quote"
    assert items[0].parameters == {"symbols": ["600519.SH"]}
