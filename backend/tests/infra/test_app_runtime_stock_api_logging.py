"""Regression tests for process-local data-interface tool log persistence."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.bootstrap.runtime import AppRuntime
from backend.bootstrap.runtime_config import RuntimeConfig
from backend.business.stock_api_logs.models import StockApiToolCall
from backend.infra.db.models import StockApiCallLogModel
from backend.infra.repositories.stock_api_call_log_repo import (
    StockApiCallLogRecord,
    StockApiCallLogRepository,
)


@pytest.mark.asyncio
async def test_runtime_serializes_concurrent_tool_call_log_writes(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = AppRuntime(RuntimeConfig())
    runtime.session_factory = session_factory

    active_writes = 0
    peak_writes = 0
    original_append = StockApiCallLogRepository.append

    async def tracked_append(
        self: StockApiCallLogRepository,
        record: StockApiCallLogRecord,
    ) -> None:
        nonlocal active_writes, peak_writes
        active_writes += 1
        peak_writes = max(peak_writes, active_writes)
        try:
            await asyncio.sleep(0)
            await original_append(self, record)
        finally:
            active_writes -= 1

    monkeypatch.setattr(StockApiCallLogRepository, "append", tracked_append)

    calls = [
        StockApiToolCall(
            tool_source="public",
            tool_id="stock_quote",
            parameters={"symbols": [f"600{index:03d}.SH"]},
            status="success",
            duration_ms=1,
        )
        for index in range(20)
    ]
    await asyncio.gather(*(runtime._record_stock_api_tool_call(call) for call in calls))

    async with session_factory() as session:
        count = await session.scalar(
            select(func.count()).select_from(StockApiCallLogModel)
        )

    assert peak_writes == 1
    assert count == len(calls)
