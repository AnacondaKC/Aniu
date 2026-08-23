"""Tests for extracting daily Run reports for the Dream Agent."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from backend.business.shared.enums import RunStatus
from backend.infra.db.models import StrategyRunModel
from backend.infra.repositories.run_repo import RunRepository


@pytest.mark.asyncio
async def test_completed_report_query_uses_shanghai_completion_date(session) -> None:
    session.add(
        StrategyRunModel(
            id=20260818101,
            trigger_source="manual",
            status=RunStatus.COMPLETED.value,
            current_state="Completed",
            snapshot_json={},
            trace_json={
                "stages": [
                    {
                        "stage_id": "Run",
                        "key": "run",
                        "steps": [
                            {
                                "type": "result",
                                "content": "## 当日运行报告",
                            }
                        ],
                    }
                ]
            },
            started_at="2026-08-18T14:50:00+00:00",
            completed_at="2026-08-18T15:10:00+00:00",
        )
    )
    await session.commit()

    reports = await RunRepository(session).list_completed_reports(
        datetime(2026, 8, 18, tzinfo=UTC).date()
    )

    assert len(reports) == 1
    assert reports[0].run_id == 20260818101
    assert reports[0].content == "## 当日运行报告"
