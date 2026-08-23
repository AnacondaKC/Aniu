"""Worker-side execution of an already-created strategy run."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from time import perf_counter

from backend.business.runs import StrategyRun
from backend.business.runs.abort_registry import ActiveRunAbortRegistry
from backend.business.runs.agent_runner import AgentRunnerFactoryPort
from backend.business.runs.callbacks import RunExecutionCallbacks
from backend.business.runs.dto import RunDetailDTO, to_run_detail_dto
from backend.business.runs.orchestration import AniuOrchestrator
from backend.business.runs.ports import RunRepositoryPort
from backend.business.runs.runtime import RunRuntimeState
from backend.business.runs.trace_support import (
    RunSnapshotPublisher,
    RunTraceSupport,
)
from backend.business.shared import CommitterPort, RunAbortError, RunNotFoundError
from backend.business.shared.enums import RunState, RunStatus
from backend.business.shared.stock_api_source import (
    STOCK_API_SOURCE_RUN,
    stock_api_source,
)

NowProvider = Callable[[], datetime]
MarketSessionOpen = Callable[[datetime], bool]
TraceStepDeltaPublisher = Callable[..., Awaitable[None]]
ExecutionGuard = Callable[[], Awaitable[None]]
ExecutionFence = Callable[[StrategyRun], Awaitable[bool]]
logger = logging.getLogger(__name__)


class RunExecutor:
    """Execute one durable run job; no request-side creation or query behavior."""

    def __init__(
        self,
        run_repo: RunRepositoryPort,
        *,
        agent_runner_factory: AgentRunnerFactoryPort,
        abort_registry: ActiveRunAbortRegistry,
        committer: CommitterPort | None = None,
        snapshot_publisher: RunSnapshotPublisher | None = None,
        trace_step_delta_publisher: TraceStepDeltaPublisher | None = None,
        now_provider: NowProvider | None = None,
        market_session_is_open: MarketSessionOpen | None = None,
    ) -> None:
        self._run_repo = run_repo
        self._committer = committer
        self._execution_guard: ExecutionGuard | None = None
        self._execution_fence: ExecutionFence | None = None
        self._agent_runner_factory = agent_runner_factory
        self._abort_registry = abort_registry
        self._now_provider = now_provider or (lambda: datetime.now(tz=UTC))
        self._market_session_is_open = market_session_is_open or (lambda _moment: False)
        self._runtime = RunRuntimeState()
        self._execution_callbacks = RunExecutionCallbacks(
            runtime=self._runtime,
            publish_trace_step_delta=trace_step_delta_publisher,
        )
        self._trace = RunTraceSupport(
            run_repo=run_repo,
            committer=committer,
            snapshot_publisher=snapshot_publisher,
        )

    async def execute(self, run_id: int) -> RunDetailDTO:
        run = await self._run_repo.get_by_id(run_id)
        if run is None:
            raise RunNotFoundError(run_id)
        if run.status is not RunStatus.RUNNING:
            return to_run_detail_dto(run)

        self._activate_runtime(run)
        self._runtime.trace_recorder = self._trace.make_recorder(
            run,
            persist_run=self._persist_run,
        )
        self._execution_callbacks.bind_runtime(self._runtime)
        abort_signal = self._abort_registry.activate(run.run_id)
        started_at = perf_counter()
        logger.info(
            "run_execution_started",
            extra={
                "run_id": run.run_id,
                "job_id": run.run_id,
                "stage_id": run.trace.current_stage_id or run.current_state.value,
                "trigger_source": run.trigger_source.value,
                "current_state": run.current_state.value,
                "status": "running",
            },
        )
        try:
            if self._execution_guard is not None:
                await self._execution_guard()
            agent_runtime = await self._agent_runner_factory.prepare(run.snapshot)
            with stock_api_source(STOCK_API_SOURCE_RUN):
                result = await AniuOrchestrator(
                    state_callbacks=self._execution_callbacks,
                    agent_runner_factory=self._agent_runner_factory,
                    tool_registry=agent_runtime.tool_registry,
                    stage_runtimes=agent_runtime.stage_runtimes,
                    abort_signal=abort_signal,
                    market_session_is_open=self._market_session_is_open,
                    now_provider=self._now_provider,
                ).execute(run)
            recorder = self._runtime.trace_recorder
            if recorder is not None:
                # The recorder persists the already-terminal run and final trace
                # together, so callers never observe COMPLETED without this step.
                await recorder.set_status_step(
                    "Summary",
                    title="最终状态",
                    summary="运行已完成。",
                    data={"total_duration_ms": result.total_duration_ms},
                )
            else:
                await self._persist_run(run)
                await self._commit()
            logger.info(
                "run_execution_completed",
                extra={
                    "run_id": run.run_id,
                    "job_id": run.run_id,
                    "stage_id": run.trace.current_stage_id or "Summary",
                    "duration_ms": result.total_duration_ms,
                    "current_state": run.current_state.value,
                    "status": "completed",
                },
            )
        except RunAbortError as exc:
            logger.warning(
                "run_execution_aborted",
                extra={
                    "run_id": run.run_id,
                    "job_id": run.run_id,
                    "stage_id": run.trace.current_stage_id or run.current_state.value,
                    "duration_ms": int((perf_counter() - started_at) * 1000),
                    "error_code": "RUN_ABORTED",
                    "current_state": run.current_state.value,
                    "status": "aborted",
                },
            )
            await self._record_aborted_run(run, exc)
            raise
        except Exception as exc:
            logger.warning(
                "run_execution_failed",
                extra={
                    "run_id": run.run_id,
                    "job_id": run.run_id,
                    "stage_id": run.trace.current_stage_id or run.current_state.value,
                    "duration_ms": int((perf_counter() - started_at) * 1000),
                    "error_code": type(exc).__name__,
                    "current_state": run.current_state.value,
                    "status": "failed",
                },
                exc_info=True,
            )
            await self._record_failed_run(run, exc)
            raise
        finally:
            recorder = self._runtime.trace_recorder
            if recorder is not None:
                await recorder.aclose()
            self._reset_runtime()
            self._abort_registry.clear(abort_signal)

        stored = await self._run_repo.get_by_id(run.run_id)
        if stored is None:
            raise RunNotFoundError(run.run_id)
        return to_run_detail_dto(stored)

    async def cancel(self, run_id: int, reason: str) -> None:
        run = await self._run_repo.get_by_id(run_id)
        if run is None:
            raise RunNotFoundError(run_id)
        if run.status is not RunStatus.RUNNING:
            return
        self._activate_runtime(run)
        self._runtime.trace_recorder = self._trace.make_recorder(
            run,
            persist_run=self._persist_run,
        )
        self._execution_callbacks.bind_runtime(self._runtime)
        signal = self._abort_registry.activate(run_id)
        signal.abort(reason)
        try:
            await self._record_aborted_run(run, RunAbortError(run_id))
        finally:
            self._reset_runtime()
            self._abort_registry.clear(signal)

    async def _record_aborted_run(
        self,
        run: StrategyRun,
        exc: RunAbortError,
    ) -> None:
        if run.status is not RunStatus.RUNNING:
            return
        previous_state = run.current_state.value
        run.abort()
        await self._persist_run(run)
        await self._commit()
        recorder = self._runtime.trace_recorder
        if recorder is None:
            return
        if previous_state != RunState.FAILED.value:
            await recorder.fail_stage(previous_state, f"运行已中止：{exc}")

    async def _record_failed_run(
        self,
        run: StrategyRun,
        exc: Exception,
    ) -> None:
        if run.status is not RunStatus.RUNNING:
            return
        try:
            previous_state = run.current_state.value
            failure_reason = str(exc).strip() or type(exc).__name__
            run.fail(failure_reason)
            await self._persist_run(run)
            await self._commit()
            recorder = self._runtime.trace_recorder
            if recorder is None:
                return
            if previous_state != RunState.FAILED.value:
                await recorder.fail_stage(previous_state, f"运行失败：{failure_reason}")
        except Exception:
            logger.exception(
                "failed to persist run failure state",
                extra={"run_id": run.run_id},
            )

    async def _persist_run(self, run: StrategyRun) -> StrategyRun:
        if self._execution_fence is None:
            return await self._run_repo.save(run)
        if not await self._execution_fence(run):
            raise RunAbortError(run.run_id)
        return run

    async def _commit(self) -> None:
        if self._execution_guard is not None:
            await self._execution_guard()
        if self._committer is not None:
            await self._committer.commit()

    def set_execution_fence(self, fence: ExecutionFence) -> None:
        """Require every run persistence write to match the live claim."""

        self._execution_fence = fence

    def set_execution_guard(self, guard: ExecutionGuard) -> None:
        """Require a live worker claim before execution and persistence commits."""

        self._execution_guard = guard

    def _activate_runtime(self, run: StrategyRun) -> None:
        self._runtime = RunRuntimeState(active_run=run)
        self._execution_callbacks.bind_runtime(self._runtime)

    def _reset_runtime(self) -> None:
        self._runtime = RunRuntimeState()
        self._execution_callbacks.bind_runtime(self._runtime)
