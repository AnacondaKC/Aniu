"""Tests for startup orphan RUNNING-run recovery."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from backend.bootstrap.app_factory import _recover_stale_run_jobs
from backend.business.runs import StrategyRun, StrategySnapshot
from backend.business.runs.job import RunJobStatus
from backend.business.shared.enums import RunStatus, TriggerSource
from backend.infra.repositories import RunJobRepository, RunRepository


def _make_run(run_id: int, *, status: RunStatus) -> StrategyRun:
    completed_at = (
        datetime(2026, 5, 8, 10, tzinfo=UTC)
        if status is not RunStatus.RUNNING
        else None
    )
    return StrategyRun(
        run_id=run_id,
        trigger_source=TriggerSource.MANUAL,
        schedule_id=None,
        snapshot=StrategySnapshot(
            prompt_version="v1",
            risk_rules_version="risk-v1",
        ),
        status=status,
        completed_at=completed_at,
    )


@pytest.mark.asyncio
async def test_recover_orphan_running_runs_without_active_job(
    session_factory,
) -> None:
    async with session_factory() as session:
        repo = RunRepository(session)
        await repo.add(_make_run(202605081, status=RunStatus.RUNNING))
        await repo.add(_make_run(202605082, status=RunStatus.COMPLETED))
        await repo.add(_make_run(202605083, status=RunStatus.RUNNING))
        # No job rows for the RUNNING runs → orphans on boot.
        await session.commit()

    await _recover_stale_run_jobs(session_factory)

    async with session_factory() as session:
        repo = RunRepository(session)
        assert (await repo.get_by_id(202605081)).status is RunStatus.FAILED
        assert (await repo.get_by_id(202605082)).status is RunStatus.COMPLETED
        assert (await repo.get_by_id(202605083)).status is RunStatus.FAILED
        assert await repo.get_running_run() is None


@pytest.mark.asyncio
async def test_recover_leaves_active_leased_job_running(session_factory) -> None:
    async with session_factory() as session:
        repo = RunRepository(session)
        jobs = RunJobRepository(session)
        await repo.add(_make_run(202605091, status=RunStatus.RUNNING))
        await jobs.create_pending(202605091)
        # Simulate leased job still active (lease not expired).
        from datetime import timedelta

        from sqlalchemy import update

        from backend.business.runs.job import RunJobStatus
        from backend.infra.db.models import RunJobModel

        future = (datetime.now(tz=UTC) + timedelta(seconds=30)).isoformat()
        await session.execute(
            update(RunJobModel)
            .where(RunJobModel.run_id == 202605091)
            .values(
                status=RunJobStatus.LEASED.value,
                worker_id="other-worker",
                lease_expires_at=future,
                active_guard=1,
            )
        )
        await session.commit()

    await _recover_stale_run_jobs(session_factory)

    async with session_factory() as session:
        repo = RunRepository(session)
        assert (await repo.get_by_id(202605091)).status is RunStatus.RUNNING


@pytest.mark.asyncio
async def test_recover_interrupts_expired_leased_job_without_replaying(
    session_factory,
) -> None:
    run_id = 202605092
    async with session_factory() as session:
        runs = RunRepository(session)
        jobs = RunJobRepository(session)
        await runs.add(_make_run(run_id, status=RunStatus.RUNNING))
        await jobs.create_pending(run_id)
        await session.commit()
        claimed = await jobs.claim_next(worker_id="old-worker", lease_seconds=-1)
        assert claimed is not None
        await session.commit()

    await _recover_stale_run_jobs(session_factory)

    async with session_factory() as session:
        run = await RunRepository(session).get_by_id(run_id)
        job = await RunJobRepository(session).get_by_run_id(run_id)
        assert run is not None and run.status is RunStatus.FAILED
        assert job is not None and job.status is RunJobStatus.INTERRUPTED
        assert job.last_error_code == "expired_run_lease"
        assert job.attempt == 2


@pytest.mark.asyncio
async def test_recover_idempotent_when_no_leftovers(session_factory) -> None:
    async with session_factory() as session:
        repo = RunRepository(session)
        await repo.add(_make_run(202605081, status=RunStatus.COMPLETED))
        await session.commit()

    await _recover_stale_run_jobs(session_factory)

    async with session_factory() as session:
        repo = RunRepository(session)
        assert (await repo.get_by_id(202605081)).status is RunStatus.COMPLETED


@pytest.mark.asyncio
async def test_recover_does_not_overwrite_a_terminal_job(session_factory) -> None:
    run_id = 202605084
    async with session_factory() as session:
        runs = RunRepository(session)
        jobs = RunJobRepository(session)
        await runs.add(_make_run(run_id, status=RunStatus.RUNNING))
        await jobs.create_pending(run_id)
        await session.commit()
        claimed = await jobs.claim_next(worker_id="worker", lease_seconds=20)
        assert claimed is not None and claimed.claim_token is not None
        terminal = await jobs.mark_terminal(
            run_id,
            status=RunJobStatus.COMPLETED,
            worker_id="worker",
            claim_token=claimed.claim_token,
        )
        assert terminal is not None
        await session.commit()

    await _recover_stale_run_jobs(session_factory)

    async with session_factory() as session:
        run = await RunRepository(session).get_by_id(run_id)
        job = await RunJobRepository(session).get_by_run_id(run_id)
        assert run is not None and run.status is RunStatus.FAILED
        assert job is not None and job.status is RunJobStatus.COMPLETED
