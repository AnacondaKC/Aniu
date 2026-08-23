"""Regression tests for shared scheduler and worker leases."""

from __future__ import annotations

import asyncio
from datetime import date

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.infra.lease_guard import LeaseLostError
from backend.infra.repositories.task_lease_repo import TaskLeaseRepository
from backend.infra.scheduler.job_runner import JobRunner
from backend.infra.workers.memory_dream_worker import (
    DREAM_QUEUE_MAXSIZE,
    MemoryDreamWorker,
)


@pytest.mark.asyncio
async def test_task_lease_allows_one_owner_and_reclaims_after_expiry(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as first_session:
        first = TaskLeaseRepository(first_session)
        assert await first.try_acquire(
            lease_key="test:lease", owner_id="first", lease_seconds=60
        )
        await first_session.commit()

    async with session_factory() as second_session:
        second = TaskLeaseRepository(second_session)
        assert not await second.try_acquire(
            lease_key="test:lease", owner_id="second", lease_seconds=60
        )
        await second_session.rollback()

    async with session_factory() as reclaiming_session:
        reclaiming = TaskLeaseRepository(reclaiming_session)
        assert await reclaiming.try_acquire(
            lease_key="test:expired", owner_id="first", lease_seconds=-1
        )
        await reclaiming_session.commit()
        assert await reclaiming.try_acquire(
            lease_key="test:expired", owner_id="second", lease_seconds=60
        )
        await reclaiming_session.commit()


@pytest.mark.asyncio
async def test_scheduler_handlers_are_single_owner_across_runner_instances(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def handler(_target_date: date, _lease_check) -> None:
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()

    first = JobRunner(session_factory, memory_dream_handler=handler)
    second = JobRunner(session_factory, memory_dream_handler=handler)
    first_task = asyncio.create_task(first.trigger_memory_dream())
    second_task = asyncio.create_task(second.trigger_memory_dream())

    await asyncio.wait_for(started.wait(), timeout=2)
    await asyncio.sleep(0.05)
    assert calls == 1

    release.set()
    await asyncio.gather(first_task, second_task)


@pytest.mark.asyncio
async def test_same_runner_serializes_same_scheduler_lease_key(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    active = 0
    max_active = 0

    async def handler(_target_date: date, _lease_check) -> None:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        started.set()
        await release.wait()
        active -= 1

    runner = JobRunner(session_factory, memory_dream_handler=handler)
    first_task = asyncio.create_task(runner.trigger_memory_dream())
    second_task = asyncio.create_task(runner.trigger_memory_dream())

    await asyncio.wait_for(started.wait(), timeout=2)
    await asyncio.sleep(0.05)
    assert max_active == 1

    release.set()
    await asyncio.gather(first_task, second_task)


@pytest.mark.asyncio
async def test_different_scheduler_lease_keys_can_run_concurrently(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    started_count = 0

    async def handler(_lease_check) -> None:
        nonlocal started_count
        started_count += 1
        if started_count == 2:
            started.set()
        await release.wait()

    runner = JobRunner(session_factory)
    first_task = asyncio.create_task(
        runner._run_as_leader(handler, lease_key="test:scheduler:a", task_name="test-a")
    )
    second_task = asyncio.create_task(
        runner._run_as_leader(handler, lease_key="test:scheduler:b", task_name="test-b")
    )

    await asyncio.wait_for(started.wait(), timeout=2)
    release.set()
    await asyncio.gather(first_task, second_task)
    assert started_count == 2


@pytest.mark.asyncio
async def test_scheduler_cancels_handler_when_lease_is_lost(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def handler(_target_date: date, _lease_check) -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    runner = JobRunner(session_factory, memory_dream_handler=handler)

    async def lose_lease(
        _lease_key: str,
        _owner_id: str,
        _stop: asyncio.Event,
        lease_lost: asyncio.Event,
    ) -> None:
        await started.wait()
        lease_lost.set()

    monkeypatch.setattr(runner, "_renew_leader_lease", lose_lease)

    with pytest.raises(LeaseLostError, match="scheduler-handler"):
        await runner.trigger_memory_dream()

    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_memory_dream_cancels_execution_when_lease_is_lost(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    class BlockingService:
        async def execute(self, _task_id: int, *, execution_fence=None) -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

    worker = MemoryDreamWorker(
        session_factory=session_factory,
        service_factory=lambda _session: BlockingService(),
    )

    async def lose_lease(
        _lease_key: str,
        _owner_id: str,
        _stop: asyncio.Event,
        lease_lost: asyncio.Event,
    ) -> None:
        await started.wait()
        lease_lost.set()

    monkeypatch.setattr(worker, "_renew_lease", lose_lease)

    with pytest.raises(LeaseLostError, match="memory-dream"):
        await worker._execute(123)

    assert cancelled.is_set()


def test_memory_dream_wake_queue_is_bounded_and_deduplicated(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    def unexpected_service_factory(_session: AsyncSession):
        raise AssertionError("service factory must not run while enqueueing")

    worker = MemoryDreamWorker(
        session_factory=session_factory,
        service_factory=unexpected_service_factory,
    )

    assert worker._enqueue(1)
    assert not worker._enqueue(1)
    for task_id in range(2, DREAM_QUEUE_MAXSIZE + 1):
        assert worker._enqueue(task_id)

    assert worker._queue.qsize() == DREAM_QUEUE_MAXSIZE
    assert not worker._enqueue(DREAM_QUEUE_MAXSIZE + 1)
