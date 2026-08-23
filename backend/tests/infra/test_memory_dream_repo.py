"""Persistence tests for nightly memory dream tasks."""

from __future__ import annotations

from datetime import date

import pytest

from backend.business.dreams import DreamStatus
from backend.business.dreams.service import DreamService
from backend.infra.repositories.memory_dream_repo import MemoryDreamRepository
from backend.infra.repositories.task_lease_repo import TaskLeaseRepository


class NoopAgent:
    async def run(self, dream):
        return f"整理 {dream.target_date.isoformat()}"


@pytest.mark.asyncio
async def test_memory_dream_repository_persists_one_task_per_date(
    session,
) -> None:
    service = DreamService(
        MemoryDreamRepository(session),
        NoopAgent(),
        committer=session,
    )

    first = await service.create_or_get(date(2026, 8, 18))
    await session.commit()
    second = await service.create_or_get(date(2026, 8, 18))

    assert first.task_id == second.task_id
    assert first.task_id == 20260818401
    assert second.status is DreamStatus.PENDING


@pytest.mark.asyncio
async def test_memory_dream_repository_lists_running_tasks(session) -> None:
    repository = MemoryDreamRepository(session)
    service = DreamService(repository, NoopAgent(), committer=session)
    dream = await service.create_or_get(date(2026, 8, 19))
    dream.start()
    await repository.save(dream)
    await session.commit()

    running = await repository.list_running()

    assert [item.task_id for item in running] == [dream.task_id]


@pytest.mark.asyncio
async def test_fenced_save_requires_active_owner_and_expected_status(session) -> None:
    repository = MemoryDreamRepository(session)
    service = DreamService(repository, NoopAgent(), committer=session)
    dream = await service.create_or_get(date(2026, 8, 20))
    await session.commit()

    lease_repository = TaskLeaseRepository(session)
    assert await lease_repository.try_acquire(
        lease_key=f"memory-dream:{dream.task_id}",
        owner_id="worker-a",
        lease_seconds=60,
    )
    await session.commit()

    running = await repository.get_by_id(dream.task_id)
    assert running is not None
    running.start()
    assert not await repository.save_fenced(
        running,
        lease_key=f"memory-dream:{dream.task_id}",
        owner_id="worker-b",
        expected_status=DreamStatus.PENDING,
    )
    assert await repository.save_fenced(
        running,
        lease_key=f"memory-dream:{dream.task_id}",
        owner_id="worker-a",
        expected_status=DreamStatus.PENDING,
    )
    await session.commit()

    stored = await repository.get_by_id(dream.task_id)
    assert stored is not None
    assert stored.status is DreamStatus.RUNNING

    stored.complete("已完成")
    assert not await repository.save_fenced(
        stored,
        lease_key=f"memory-dream:{dream.task_id}",
        owner_id="worker-a",
        expected_status=DreamStatus.PENDING,
    )
