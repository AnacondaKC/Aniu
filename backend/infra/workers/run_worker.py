"""Durable SQLite-lease worker that executes strategy runs."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.business.runs import StrategyRun
from backend.business.runs.abort_registry import ActiveRunAbortRegistry
from backend.business.runs.executor import RunExecutor
from backend.business.runs.job import RunJobStatus
from backend.business.runs.trace_support import RunTraceSupport
from backend.business.shared import RunAbortError
from backend.business.shared.enums import RunStatus
from backend.infra.repositories import (
    RunRepository,
)
from backend.infra.repositories.run_job_repo import RunJobRepository

logger = logging.getLogger(__name__)

DEFAULT_POLL_SECONDS = 0.5
DEFAULT_LEASE_SECONDS = 20.0
DEFAULT_HEARTBEAT_SECONDS = 5.0
DEFAULT_MAX_ATTEMPTS = 3


class RunWorker:
    """Long-running task that claims durable run jobs via DB leases.

    Manual and scheduled submissions both create a PENDING ``run_jobs`` row.
    This worker claims jobs with conditional UPDATE so multiple app processes
    cannot execute the same run.
    """

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        executor_factory: Callable[[AsyncSession], RunExecutor],
        abort_registry: ActiveRunAbortRegistry,
        worker_id: str | None = None,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> None:
        self._session_factory = session_factory
        self._executor_factory = executor_factory
        self._abort_registry = abort_registry
        self._worker_id = worker_id or f"worker-{uuid.uuid4().hex[:12]}"
        self._poll_seconds = poll_seconds
        self._lease_seconds = lease_seconds
        self._heartbeat_seconds = heartbeat_seconds
        self._max_attempts = max_attempts
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._wake = asyncio.Event()

    @property
    def worker_id(self) -> str:
        return self._worker_id

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done() and not self._stopping

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._task = asyncio.create_task(self._run_loop(), name="aniu-run-worker")

    async def stop(self) -> None:
        self._stopping = True
        self._wake.set()
        if self._task is None:
            return
        try:
            await asyncio.wait_for(self._task, timeout=10.0)
        except TimeoutError:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
        self._task = None

    async def submit(self, run_id: int) -> None:
        """Wake the poll loop after a job was enqueued in the database."""

        del run_id
        if self._task is None or self._task.done():
            self.start()
        self._wake.set()

    async def _run_loop(self) -> None:
        while not self._stopping:
            claimed = False
            try:
                claimed = await self._claim_and_execute_one()
            except Exception:
                logger.exception("run worker loop iteration failed")
            if claimed:
                continue
            with suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=self._poll_seconds)
            self._wake.clear()

    async def _claim_and_execute_one(self) -> bool:
        async with self._session_factory() as session:
            jobs = RunJobRepository(session)
            job = await jobs.claim_next(
                worker_id=self._worker_id,
                lease_seconds=self._lease_seconds,
            )
            if job is None:
                return False
            await session.commit()
            run_id = job.run_id
            claim_token = job.claim_token
            cancel_requested = job.status is RunJobStatus.CANCEL_REQUESTED
            attempt = job.attempt

        if claim_token is None:
            raise RuntimeError(f"claimed run job {run_id} has no fencing token")
        if attempt > self._max_attempts:
            await self._mark_interrupted(
                run_id,
                claim_token=claim_token,
                error_code="max_attempts_exceeded",
                error_message=f"run job exceeded max attempts ({self._max_attempts})",
            )
            return True
        if attempt > 1:
            await self._mark_interrupted(
                run_id,
                claim_token=claim_token,
                error_code="reclaimed_run_not_replayed",
                error_message="运行租约已过期；为避免重复交易，任务不会自动重放",
            )
            return True

        if cancel_requested:
            await self._execute_cancelled(run_id, claim_token)
            return True

        await self._execute(run_id, claim_token)
        return True

    async def _execute(self, run_id: int, claim_token: str) -> None:
        outcome: tuple[bool, RunJobStatus, str, str] | None = None
        heartbeat_failed = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(run_id, claim_token, heartbeat_failed),
            name=f"aniu-run-heartbeat-{run_id}",
        )
        try:
            async with self._session_factory() as session:
                runs = RunRepository(session)
                jobs = RunJobRepository(session)
                current = await jobs.get_by_run_id(run_id)
                if current is None or current.claim_token != claim_token:
                    return
                if current.status is RunJobStatus.CANCEL_REQUESTED:
                    await session.commit()
                    await self._execute_cancelled(run_id, claim_token)
                    return

                executor = self._executor_factory(session)

                async def persist_with_claim(run: StrategyRun) -> bool:
                    return await runs.save_fenced(
                        run,
                        worker_id=self._worker_id,
                        claim_token=claim_token,
                    )

                async def ensure_claim_owned() -> None:
                    if heartbeat_failed.is_set():
                        raise RunAbortError(run_id)
                    owned = await jobs.get_by_run_id(run_id)
                    if (
                        owned is None
                        or owned.worker_id != self._worker_id
                        or owned.claim_token != claim_token
                        or owned.status is not RunJobStatus.LEASED
                        or owned.lease_expires_at is None
                        or owned.lease_expires_at <= datetime.now(tz=UTC)
                    ):
                        raise RunAbortError(run_id)

                executor.set_execution_fence(persist_with_claim)
                executor.set_execution_guard(ensure_claim_owned)
                if heartbeat_failed.is_set():
                    raise RunAbortError(run_id)
                await executor.execute(run_id)
                terminal = await jobs.mark_terminal(
                    run_id,
                    status=RunJobStatus.COMPLETED,
                    worker_id=self._worker_id,
                    claim_token=claim_token,
                )
                if terminal is None:
                    await session.rollback()
                    return
                await session.commit()
        except RunAbortError as exc:
            outcome = (True, RunJobStatus.CANCELLED, "aborted", str(exc))
        except Exception as exc:
            logger.exception(
                "run worker execution failed",
                extra={"run_id": run_id},
            )
            outcome = (False, RunJobStatus.FAILED, "execution_error", str(exc))
        finally:
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                try:
                    await heartbeat_task
                except Exception:
                    logger.exception(
                        "run heartbeat failed",
                        extra={"run_id": run_id},
                    )

        if outcome is not None:
            aborted, job_status, error_code, error_message = outcome
            try:
                await self._finalize_execution(
                    run_id,
                    claim_token=claim_token,
                    aborted=aborted,
                    job_status=job_status,
                    error_code=error_code,
                    error_message=error_message,
                )
            except Exception:
                logger.exception(
                    "failed to finalize run execution",
                    extra={"run_id": run_id},
                )

    async def _finalize_execution(
        self,
        run_id: int,
        *,
        claim_token: str,
        aborted: bool,
        job_status: RunJobStatus,
        error_code: str,
        error_message: str,
    ) -> None:
        """Finalize only while this worker still owns the current claim."""

        async with self._session_factory() as session:
            runs = RunRepository(session)
            jobs = RunJobRepository(session)

            async def persist_with_claim(run: StrategyRun) -> bool:
                return await runs.save_fenced(
                    run,
                    worker_id=self._worker_id,
                    claim_token=claim_token,
                )

            run = await runs.get_by_id(run_id)
            if run is not None and run.status.value == "RUNNING":
                if aborted:
                    run.abort()
                    if not await persist_with_claim(run):
                        await session.rollback()
                        return
                else:
                    await self._record_run_failure(
                        run,
                        runs=runs,
                        session=session,
                        persist_run=persist_with_claim,
                        reason=error_message or error_code,
                    )

            terminal = await jobs.mark_terminal(
                run_id,
                status=job_status,
                worker_id=self._worker_id,
                claim_token=claim_token,
                error_code=error_code,
                error_message=error_message,
            )
            if terminal is None:
                await session.rollback()
                return
            await session.commit()

    async def _execute_cancelled(self, run_id: int, claim_token: str) -> None:
        async with self._session_factory() as session:
            runs = RunRepository(session)
            jobs = RunJobRepository(session)
            job = await jobs.get_by_run_id(run_id)
            if job is None or job.claim_token != claim_token:
                return
            reason = job.cancel_reason or "user_requested"
            executor = self._executor_factory(session)

            async def persist_with_claim(run: StrategyRun) -> bool:
                return await runs.save_fenced(
                    run,
                    worker_id=self._worker_id,
                    claim_token=claim_token,
                )

            async def ensure_cancel_claim_owned() -> None:
                owned = await jobs.get_by_run_id(run_id)
                if (
                    owned is None
                    or owned.worker_id != self._worker_id
                    or owned.claim_token != claim_token
                    or owned.status
                    not in {
                        RunJobStatus.LEASED,
                        RunJobStatus.CANCEL_REQUESTED,
                    }
                    or owned.lease_expires_at is None
                    or owned.lease_expires_at <= datetime.now(tz=UTC)
                ):
                    raise RunAbortError(run_id)

            executor.set_execution_fence(persist_with_claim)
            executor.set_execution_guard(ensure_cancel_claim_owned)
            try:
                await executor.cancel(run_id, reason)
            except Exception:
                # Run may already be terminal; still close the owned job.
                logger.exception(
                    "failed to apply cancel to run during job reclaim",
                    extra={"run_id": run_id},
                )
            terminal = await jobs.mark_terminal(
                run_id,
                status=RunJobStatus.CANCELLED,
                worker_id=self._worker_id,
                claim_token=claim_token,
                error_code="cancelled",
                error_message=reason,
            )
            if terminal is None:
                await session.rollback()
                return
            await session.commit()

    async def _mark_interrupted(
        self,
        run_id: int,
        *,
        claim_token: str,
        error_code: str,
        error_message: str,
    ) -> None:
        async with self._session_factory() as session:
            runs = RunRepository(session)
            jobs = RunJobRepository(session)

            async def persist_with_claim(run: StrategyRun) -> bool:
                return await runs.save_fenced(
                    run,
                    worker_id=self._worker_id,
                    claim_token=claim_token,
                )

            run = await runs.get_by_id(run_id)
            terminal_status = RunJobStatus.INTERRUPTED
            terminal_error_code: str | None = error_code
            terminal_error_message: str | None = error_message
            if run is not None and run.status is RunStatus.RUNNING:
                await self._record_run_failure(
                    run,
                    runs=runs,
                    session=session,
                    persist_run=persist_with_claim,
                    reason=error_message or error_code,
                )
            elif run is not None:
                terminal_status = {
                    RunStatus.COMPLETED: RunJobStatus.COMPLETED,
                    RunStatus.FAILED: RunJobStatus.FAILED,
                    RunStatus.ABORTED: RunJobStatus.CANCELLED,
                }[run.status]
                terminal_error_code = None
                terminal_error_message = None

            terminal = await jobs.mark_terminal(
                run_id,
                status=terminal_status,
                worker_id=self._worker_id,
                claim_token=claim_token,
                error_code=terminal_error_code,
                error_message=terminal_error_message,
            )
            if terminal is None:
                await session.rollback()
                return
            await session.commit()

    async def _record_run_failure(
        self,
        run: StrategyRun,
        *,
        runs: RunRepository,
        session: AsyncSession,
        persist_run: Callable[[StrategyRun], Awaitable[bool]],
        reason: str,
    ) -> None:
        """Persist a worker-level failure on the stage that was executing."""

        async def persist_and_return(stored_run: StrategyRun) -> StrategyRun:
            if not await persist_run(stored_run):
                raise RunAbortError(stored_run.run_id)
            return stored_run

        previous_state = run.current_state.value
        run.fail(reason)
        recorder = RunTraceSupport(
            run_repo=runs,
            committer=session,
            snapshot_publisher=None,
        ).make_recorder(run, persist_run=persist_and_return)
        await recorder.fail_stage(previous_state, f"运行失败：{reason}")

    async def _heartbeat_loop(
        self,
        run_id: int,
        claim_token: str,
        heartbeat_failed: asyncio.Event | None = None,
    ) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_seconds)
            try:
                async with self._session_factory() as session:
                    jobs = RunJobRepository(session)
                    job = await jobs.heartbeat(
                        run_id,
                        worker_id=self._worker_id,
                        claim_token=claim_token,
                        lease_seconds=self._lease_seconds,
                    )
                    await session.commit()
                    if job is None:
                        if heartbeat_failed is not None:
                            heartbeat_failed.set()
                        self._abort_registry.abort(run_id, "worker_lease_lost")
                        return
                    if job.status is RunJobStatus.CANCEL_REQUESTED:
                        if heartbeat_failed is not None:
                            heartbeat_failed.set()
                        self._abort_registry.abort(
                            run_id, job.cancel_reason or "user_requested"
                        )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "run worker heartbeat failed",
                    extra={"run_id": run_id},
                )
                if heartbeat_failed is not None:
                    heartbeat_failed.set()
                self._abort_registry.abort(run_id, "worker_heartbeat_failed")
                return


def build_run_worker(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    executor_factory: Callable[[AsyncSession], RunExecutor],
    abort_registry: ActiveRunAbortRegistry,
    worker_id: str | None = None,
) -> RunWorker:
    return RunWorker(
        session_factory=session_factory,
        executor_factory=executor_factory,
        abort_registry=abort_registry,
        worker_id=worker_id,
    )
