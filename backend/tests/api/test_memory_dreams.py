"""API tests for nightly memory dream queries."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from httpx import AsyncClient

from backend.business.dreams import DreamStatus, MemoryDream
from backend.business.dreams.service import DreamService
from backend.infra.repositories.memory_dream_repo import MemoryDreamRepository
from backend.main import app


class NoopDreamAgent:
    async def run(self, dream: MemoryDream) -> str:
        return f"整理 {dream.target_date.isoformat()}"


@pytest.mark.asyncio
async def test_manual_memory_dream_run_creates_and_queues_previous_day_task(
    api_client: AsyncClient,
) -> None:
    response = await api_client.post("/api/aniu/memory-dreams/run")

    assert response.status_code == 202
    body = response.json()
    expected_date = datetime.now(tz=UTC).astimezone(
        ZoneInfo("Asia/Shanghai")
    ).date() - timedelta(days=1)
    assert body["target_date"] == expected_date.isoformat()
    assert body["status"] == DreamStatus.PENDING.value
    assert app.state.runtime.dream_worker.submitted == [body["task_id"]]


@pytest.mark.asyncio
async def test_memory_dream_list_and_detail_roundtrip(
    api_client: AsyncClient,
    session,
) -> None:
    service = DreamService(
        MemoryDreamRepository(session),
        NoopDreamAgent(),
        committer=session,
    )
    first = await service.create_or_get(date(2026, 8, 18))
    second = await service.create_or_get(date(2026, 8, 19))
    await session.commit()

    listed = await api_client.get("/api/aniu/memory-dreams")
    assert listed.status_code == 200
    body = listed.json()
    assert body["total"] == 2
    assert [item["target_date"] for item in body["items"]] == [
        "2026-08-19",
        "2026-08-18",
    ]
    assert body["latest"]["task_id"] == second.task_id

    paged = await api_client.get("/api/aniu/memory-dreams?limit=1&offset=1")
    assert paged.status_code == 200
    paged_body = paged.json()
    assert paged_body["items"][0]["task_id"] == first.task_id
    assert paged_body["latest"]["task_id"] == second.task_id

    detail = await api_client.get(f"/api/aniu/memory-dreams/{first.task_id}")
    assert detail.status_code == 200
    detail_body = detail.json()
    assert detail_body["dream"]["task_id"] == first.task_id
    assert detail_body["dream"]["status"] == DreamStatus.PENDING.value
    assert detail_body["activity_total"] == 0
    assert detail_body["activities"] == []

    deleted = await api_client.delete(f"/api/aniu/memory-dreams/{first.task_id}")
    assert deleted.status_code == 204

    remaining = await api_client.get("/api/aniu/memory-dreams")
    assert remaining.status_code == 200
    assert remaining.json()["total"] == 1
    assert remaining.json()["items"][0]["task_id"] == second.task_id

    missing = await api_client.get(f"/api/aniu/memory-dreams/{first.task_id}")
    assert missing.status_code == 404

    missing_delete = await api_client.delete(f"/api/aniu/memory-dreams/{first.task_id}")
    assert missing_delete.status_code == 404
