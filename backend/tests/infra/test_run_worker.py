"""Tests for durable run worker terminal-state recovery."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import update

from backend.business.runs import RunJobStatus, StrategyRun, StrategySnapshot
from backend.business.runs.abort_registry import ActiveRunAbortRegistry
from backend.business.runs.run_trace import TraceStage, TraceStep
from backend.business.shared.enums import RunState, RunStatus, TriggerSource
from backend.infra.db.models import RunJobModel
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


@pytest.mark.asyncio
async def test_cancel_request_wins_before_worker_marks_job_completed(
    session_factory,
) -> None:
    run_id = 20260731105
    worker_id = "worker-cancel-race"
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
        run.trace.stages.append(
            TraceStage(
                stage_id="run:na",
                key="run",
                round=None,
                title="执行阶段",
                description="执行",
                status="running",
                steps=[
                    TraceStep(
                        step_id="tool:active",
                        type="tool",
                        title="行情查询",
                        status="running",
                    )
                ],
            )
        )
        run.trace.current_stage_id = "run:na"
        await runs.add(run)
        await jobs.create_pending(run_id)
        await session.commit()
        claimed = await jobs.claim_next(worker_id=worker_id, lease_seconds=20)
        await session.commit()
        assert claimed is not None and claimed.claim_token is not None

    class CancelBeforeReturningExecutor:
        def set_execution_fence(self, _fence) -> None:
            pass

        def set_execution_guard(self, _guard) -> None:
            pass

        async def execute(self, active_run_id: int) -> None:
            async with session_factory() as cancel_session:
                requested = await RunJobRepository(cancel_session).request_cancel(
                    active_run_id, reason="manual_stop"
                )
                assert requested is not None
                await cancel_session.commit()

    worker = RunWorker(
        session_factory=session_factory,
        executor_factory=lambda _session: CancelBeforeReturningExecutor(),  # type: ignore[arg-type]
        abort_registry=ActiveRunAbortRegistry(),
        worker_id=worker_id,
    )
    await worker._execute(run_id, claimed.claim_token)

    async with session_factory() as session:
        run = await RunRepository(session).get_by_id(run_id)
        job = await RunJobRepository(session).get_by_run_id(run_id)

    assert run is not None and run.status is RunStatus.ABORTED
    assert run.trace.current_stage_id is None
    assert run.trace.stages[0].status == "failed"
    assert run.trace.stages[0].steps[0].status == "failed"
    assert job is not None and job.status is RunJobStatus.CANCELLED


@pytest.mark.asyncio
async def test_cancel_request_wins_during_failure_finalization(
    session_factory, monkeypatch
) -> None:
    run_id = 20260731106
    worker_id = "worker-finalize-race"
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
        claimed = await jobs.claim_next(worker_id=worker_id, lease_seconds=20)
        await session.commit()
        assert claimed is not None and claimed.claim_token is not None

    original_get_by_run_id = RunJobRepository.get_by_run_id
    cancellation_injected = False

    async def get_then_request_cancel(repository, requested_run_id):
        nonlocal cancellation_injected
        current = await original_get_by_run_id(repository, requested_run_id)
        if not cancellation_injected and current is not None:
            cancellation_injected = True
            async with session_factory() as cancel_session:
                await cancel_session.execute(
                    update(RunJobModel)
                    .where(RunJobModel.run_id == requested_run_id)
                    .values(
                        status=RunJobStatus.CANCEL_REQUESTED.value,
                        cancel_reason="manual_stop",
                    )
                )
                await cancel_session.commit()
        return current

    monkeypatch.setattr(RunJobRepository, "get_by_run_id", get_then_request_cancel)
    worker = RunWorker(
        session_factory=session_factory,
        executor_factory=lambda _session: None,  # type: ignore[arg-type]
        abort_registry=ActiveRunAbortRegistry(),
        worker_id=worker_id,
    )
    await worker._finalize_execution(
        run_id,
        claim_token=claimed.claim_token,
        aborted=False,
        job_status=RunJobStatus.FAILED,
        error_code="execution_error",
        error_message="provider failed",
    )

    async with session_factory() as session:
        run = await RunRepository(session).get_by_id(run_id)
        job = await RunJobRepository(session).get_by_run_id(run_id)

    assert cancellation_injected
    assert run is not None and run.status is RunStatus.ABORTED
    assert run.trace.stages[0].status == "failed"
    assert job is not None and job.status is RunJobStatus.CANCELLED


@pytest.mark.asyncio
async def test_cancel_request_retries_when_failure_terminal_write_loses_race(
    session_factory, monkeypatch
) -> None:
    run_id = 20260731107
    worker_id = "worker-terminal-race"
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
        claimed = await jobs.claim_next(worker_id=worker_id, lease_seconds=20)
        await session.commit()
        assert claimed is not None and claimed.claim_token is not None

    original_get_by_run_id = RunJobRepository.get_by_run_id
    original_mark_terminal = RunJobRepository.mark_terminal
    terminal_rejected = False

    async def mark_terminal_once_as_cancelled(repository, *args, **kwargs):
        nonlocal terminal_rejected
        if not terminal_rejected and kwargs.get("require_leased"):
            terminal_rejected = True
            return None
        return await original_mark_terminal(repository, *args, **kwargs)

    async def get_with_late_cancel(repository, requested_run_id):
        current = await original_get_by_run_id(repository, requested_run_id)
        if terminal_rejected and current is not None:
            current.status = RunJobStatus.CANCEL_REQUESTED
            current.cancel_reason = "manual_stop"
        return current

    monkeypatch.setattr(
        RunJobRepository, "mark_terminal", mark_terminal_once_as_cancelled
    )
    monkeypatch.setattr(RunJobRepository, "get_by_run_id", get_with_late_cancel)
    worker = RunWorker(
        session_factory=session_factory,
        executor_factory=lambda _session: None,  # type: ignore[arg-type]
        abort_registry=ActiveRunAbortRegistry(),
        worker_id=worker_id,
    )
    await worker._finalize_execution(
        run_id,
        claim_token=claimed.claim_token,
        aborted=False,
        job_status=RunJobStatus.FAILED,
        error_code="execution_error",
        error_message="provider failed",
    )

    monkeypatch.setattr(RunJobRepository, "get_by_run_id", original_get_by_run_id)
    async with session_factory() as session:
        run = await RunRepository(session).get_by_id(run_id)
        job = await RunJobRepository(session).get_by_run_id(run_id)

    assert terminal_rejected
    assert run is not None and run.status is RunStatus.ABORTED
    assert job is not None and job.status is RunJobStatus.CANCELLED
