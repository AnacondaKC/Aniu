"""Tests for the independent memory dream application service."""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime

import pytest

from backend.business.dreams import DreamStatus, MemoryDream
from backend.business.dreams.service import DreamService


class InMemoryDreamRepository:
    def __init__(self) -> None:
        self.items: dict[int, MemoryDream] = {}
        self.next_id = 1

    async def next_task_id(self, reference_date: date) -> int:
        del reference_date
        task_id = self.next_id
        self.next_id += 1
        return task_id

    async def get_by_id(self, task_id: int) -> MemoryDream | None:
        return self.items.get(task_id)

    async def delete(self, task_id: int) -> bool:
        return self.items.pop(task_id, None) is not None

    async def get_by_date(self, target_date: date) -> MemoryDream | None:
        return next(
            (item for item in self.items.values() if item.target_date == target_date),
            None,
        )

    async def add(self, dream: MemoryDream) -> MemoryDream:
        self.items[dream.task_id] = dream
        return dream

    async def save(self, dream: MemoryDream) -> MemoryDream:
        self.items[dream.task_id] = dream
        return dream

    async def list_pending(self, *, limit: int = 256) -> list[MemoryDream]:
        return [
            item for item in self.items.values() if item.status is DreamStatus.PENDING
        ][:limit]

    async def list_running(self) -> list[MemoryDream]:
        return [
            item for item in self.items.values() if item.status is DreamStatus.RUNNING
        ]


class Committer:
    def __init__(self) -> None:
        self.commits = 0

    async def commit(self) -> None:
        self.commits += 1


@pytest.mark.asyncio
async def test_create_or_get_is_idempotent_for_one_date() -> None:
    repository = InMemoryDreamRepository()
    agent = FakeAgent()
    service = DreamService(repository, agent, committer=Committer())

    first = await service.create_or_get(date(2026, 8, 18))
    second = await service.create_or_get(date(2026, 8, 18))

    assert first.task_id == second.task_id
    assert len(repository.items) == 1


@pytest.mark.asyncio
async def test_delete_removes_existing_dream_and_reports_missing_records() -> None:
    repository = InMemoryDreamRepository()
    committer = Committer()
    service = DreamService(repository, FakeAgent(), committer=committer)
    dream = await service.create_or_get(date(2026, 8, 18))

    assert await service.delete(dream.task_id)
    assert await service.get_by_id(dream.task_id) is None
    assert not await service.delete(dream.task_id)


@pytest.mark.asyncio
async def test_prepare_manual_run_requeues_terminal_previous_day_dream() -> None:
    repository = InMemoryDreamRepository()
    service = DreamService(repository, FakeAgent(), committer=Committer())
    dream = await service.create_or_get(date(2026, 8, 18))
    dream.start()
    dream.complete("旧结果")
    await repository.save(dream)

    prepared = await service.prepare_manual_run(
        now=datetime(2026, 8, 19, 12, tzinfo=UTC)
    )

    assert prepared.task_id == dream.task_id
    assert prepared.status is DreamStatus.PENDING
    assert prepared.result is None
    assert prepared.started_at is None
    assert prepared.completed_at is None


@pytest.mark.asyncio
async def test_failed_dream_keeps_partial_task_state_and_does_not_retry() -> None:
    repository = InMemoryDreamRepository()
    agent = FakeAgent(error=RuntimeError("模型超时"))
    service = DreamService(repository, agent, committer=Committer())
    dream = await service.create_or_get(date(2026, 8, 18))

    failed = await service.execute(dream.task_id)
    skipped = await service.execute(dream.task_id)

    assert failed is not None
    assert failed.status is DreamStatus.FAILED
    assert failed.failure_reason == "模型超时"
    assert skipped is failed
    assert agent.calls == 1


@pytest.mark.asyncio
async def test_cancelled_dream_is_failed_without_automatic_retry() -> None:
    repository = InMemoryDreamRepository()
    service = DreamService(repository, CancelledAgent(), committer=Committer())
    dream = await service.create_or_get(date(2026, 8, 18))

    with pytest.raises(asyncio.CancelledError):
        await service.execute(dream.task_id)

    assert dream.status is DreamStatus.FAILED
    assert dream.failure_reason == "梦境任务被进程取消，未自动重试，请确认后手动重试"
    assert dream.started_at is not None
    assert dream.completed_at is not None


class CancelledAgent:
    async def run(self, dream: MemoryDream) -> str:
        del dream
        raise asyncio.CancelledError


class FakeAgent:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls = 0

    async def run(self, dream: MemoryDream) -> str:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return f"已整理 {dream.target_date.isoformat()}"
