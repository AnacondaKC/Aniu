"""Tests for durable run worker terminal-state recovery."""

from __future__ import annotations

import asyncio

import pytest

from backend.business.runs import RunJobStatus, StrategyRun, StrategySnapshot
from backend.business.runs.abort_registry import ActiveRunAbortRegistry
from backend.business.shared.enums import RunState, RunStatus, TriggerSource
from backend.infra.repositories import RunJobRepository, RunRepository
from backend.infra.workers.run_worker import RunWorker


@pytest.mark.asyncio
async def test_worker_does_not_replay_a_reclaimed_job(session_factory) -> None:
    run_id = 20260731102
    async with session_factory() as session:
        runs = RunRepository(session)
        jobs = RunJobRepository(session)
        await runs.add(
            StrategyRun(
                run_id=run_id,
                trigger_source=TriggerSource.MANUAL,
                schedule_id=None,
                snapshot=StrategySnapshot(
                    prompt_version="v1",
                    risk_rules_version="risk-v1",
                ),
            )
        )
        await jobs.create_pending(run_id)
        await session.commit()
        claimed = await jobs.claim_next(worker_id="old-worker", lease_seconds=-1)
        assert claimed is not None
        await session.commit()

    executor_called = False

    def unexpected_executor(_session):
        nonlocal executor_called
        executor_called = True
        raise AssertionError("reclaimed runs must not be executed")

    worker = RunWorker(
        session_factory=session_factory,
        executor_factory=unexpected_executor,  # type: ignore[arg-type]
        abort_registry=ActiveRunAbortRegistry(),
        worker_id="recovery-worker",
    )
    assert await worker._claim_and_execute_one()
    assert not executor_called

    async with session_factory() as session:
        run = await RunRepository(session).get_by_id(run_id)
        job = await RunJobRepository(session).get_by_run_id(run_id)
    assert run is not None and run.status is RunStatus.FAILED
    assert job is not None and job.status is RunJobStatus.INTERRUPTED
    assert job.last_error_code == "reclaimed_run_not_replayed"


@pytest.mark.asyncio
async def test_worker_reconciles_reclaimed_completed_run_job(session_factory) -> None:
    run_id = 20260731104
    async with session_factory() as session:
        runs = RunRepository(session)
        jobs = RunJobRepository(session)
        run = StrategyRun(
            run_id=run_id,
            trigger_source=TriggerSource.MANUAL,
            schedule_id=None,
            snapshot=StrategySnapshot(
                prompt_version="v1",
                risk_rules_version="risk-v1",
            ),
        )
        await runs.add(run)
        await jobs.create_pending(run_id)
        await session.commit()
        claimed = await jobs.claim_next(worker_id="old-worker", lease_seconds=-1)
        assert claimed is not None
        run.advance_to(RunState.SUMMARY)
        run.advance_to(RunState.COMPLETED)
        await runs.save(run)
        await session.commit()

    worker = RunWorker(
        session_factory=session_factory,
        executor_factory=lambda _session: None,  # type: ignore[arg-type]
        abort_registry=ActiveRunAbortRegistry(),
        worker_id="recovery-worker",
    )
    assert await worker._claim_and_execute_one()

    async with session_factory() as session:
        run = await RunRepository(session).get_by_id(run_id)
        job = await RunJobRepository(session).get_by_run_id(run_id)
    assert run is not None and run.status is RunStatus.COMPLETED
    assert job is not None and job.status is RunJobStatus.COMPLETED
    assert job.last_error_code is None


@pytest.mark.asyncio
async def test_heartbeat_failure_sets_abort_signal_and_stops(
    session_factory, monkeypatch
) -> None:
    async def fail_heartbeat(*_args, **_kwargs):
        raise RuntimeError("temporary database failure")

    monkeypatch.setattr(RunJobRepository, "heartbeat", fail_heartbeat)
    heartbeat_failed = asyncio.Event()
    worker = RunWorker(
        session_factory=session_factory,
        executor_factory=lambda _session: None,  # type: ignore[arg-type]
        abort_registry=ActiveRunAbortRegistry(),
        heartbeat_seconds=0,
    )

    await worker._heartbeat_loop(20260731103, "claim-token", heartbeat_failed)
    assert heartbeat_failed.is_set()


@pytest.mark.asyncio
async def test_finalize_execution_closes_run_and_job_in_fresh_session(
    session_factory,
) -> None:
    run_id = 20260731101
    async with session_factory() as session:
        runs = RunRepository(session)
        jobs = RunJobRepository(session)
        await runs.add(
            StrategyRun(
                run_id=run_id,
                trigger_source=TriggerSource.MANUAL,
                schedule_id=None,
                snapshot=StrategySnapshot(
                    prompt_version="v1",
                    risk_rules_version="risk-v1",
                ),
            )
        )
        await jobs.create_pending(run_id)
        await session.commit()
        claimed = await jobs.claim_next(worker_id="worker-test", lease_seconds=20)
        await session.commit()
        assert claimed is not None
        assert claimed.claim_token is not None

    worker = RunWorker(
        session_factory=session_factory,
        executor_factory=lambda _session: None,  # type: ignore[arg-type]
        abort_registry=ActiveRunAbortRegistry(),
        worker_id="worker-test",
    )
    await worker._finalize_execution(
        run_id,
        claim_token=claimed.claim_token,
        aborted=False,
        job_status=RunJobStatus.FAILED,
        error_code="execution_error",
        error_message="Session is already flushing",
    )

    async with session_factory() as session:
        run = await RunRepository(session).get_by_id(run_id)
        job = await RunJobRepository(session).get_by_run_id(run_id)

    assert run is not None
    assert run.status is RunStatus.FAILED
    assert run.failure_reason == "Session is already flushing"
    assert [stage.key for stage in run.trace.stages] == ["run"]
    assert run.trace.stages[0].status == "failed"
    assert job is not None
    assert job.status is RunJobStatus.FAILED
    assert job.last_error_code == "execution_error"
