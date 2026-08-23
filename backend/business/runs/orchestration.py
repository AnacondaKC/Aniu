"""Two-stage orchestration: Run followed by optional HTML Summary."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter
from typing import Protocol, cast

from backend.business.runs import StrategyRun
from backend.business.runs.abort import RunAbortSignal
from backend.business.runs.agent_runner import AgentRunnerFactoryPort
from backend.business.runs.execution import (
    LlmStreamDeltaSink,
    RunExecutionContext,
    RunReport,
    StagePromptPreparedSink,
    SummaryDraft,
    ToolLoopEventSink,
)
from backend.business.runs.run_events import RunEventType
from backend.business.runs.stages.run_stage import RunStage
from backend.business.runs.stages.summary_stage import SummaryStage
from backend.business.shared import RunAbortError, ServiceConfigurationError
from backend.business.shared.enums import RunState, RunStatus

StageOutput = RunReport | SummaryDraft
MarketSessionOpen = Callable[[datetime], bool]
NowProvider = Callable[[], datetime]
MAX_SUMMARY_ATTEMPTS = 2


@dataclass(frozen=True, slots=True)
class RunResult:
    run_id: int
    status: RunStatus
    final_state: RunState
    summary: str | None
    total_duration_ms: int


class StateCallbacks(Protocol):
    async def on_state_entered(self, run_id: int, state_name: str) -> None: ...

    async def on_state_completed(
        self,
        run_id: int,
        state_name: str,
        state_output: dict[str, object],
        duration_ms: int,
    ) -> None: ...

    async def on_state_skipped(
        self, run_id: int, state_name: str, summary: str
    ) -> None: ...

    async def on_state_degraded(
        self, run_id: int, state_name: str, summary: str
    ) -> None: ...

    async def on_stage_prompt_prepared(
        self, run_id: int, stage_name: str, payload: dict[str, object]
    ) -> None: ...

    async def on_tool_loop_event(
        self,
        run_id: int,
        stage_name: str,
        event_type: RunEventType,
        payload: dict[str, object],
    ) -> None: ...

    async def on_llm_stream_delta(
        self,
        run_id: int,
        stage_name: str,
        delta: str,
        channel: str = "text",
    ) -> None: ...


class AniuOrchestrator:
    """Execute one Run agent and then build an optional HTML presentation."""

    def __init__(
        self,
        *,
        state_callbacks: StateCallbacks,
        agent_runner_factory: AgentRunnerFactoryPort,
        tool_registry: object | None = None,
        llm_runtime: object | None = None,
        stage_runtimes: dict[str, object] | None = None,
        run_stage: RunStage | None = None,
        summary_stage: SummaryStage | None = None,
        abort_signal: RunAbortSignal | None = None,
        market_session_is_open: MarketSessionOpen,
        now_provider: NowProvider | None = None,
    ) -> None:
        self._callbacks = state_callbacks
        self._agent_runner_factory = agent_runner_factory
        self._tool_registry = tool_registry
        self._llm_runtime = llm_runtime
        self._stage_runtimes = stage_runtimes or {}
        self._run_stage = run_stage or RunStage()
        self._summary_stage = summary_stage or SummaryStage()
        self._abort_signal = abort_signal
        self._market_session_is_open = market_session_is_open
        self._now_provider = now_provider or (lambda: datetime.now(tz=UTC))

    async def execute(self, run: StrategyRun) -> RunResult:
        started_at = perf_counter()
        context = RunExecutionContext(run=run, snapshot=run.snapshot)
        context.abort_signal = self._abort_signal or RunAbortSignal(run.run_id)
        context.market_session_is_open = self._is_market_session_open
        context.tool_registry = self._tool_registry
        self._attach_runtime_callbacks(context, run.run_id)

        report = await self._execute_run_stage(context)
        context.run_report = report
        run.set_summary(report.content, render_mode="markdown")

        run.advance_to(RunState.SUMMARY)
        await self._callbacks.on_state_entered(run.run_id, RunState.SUMMARY.value)
        try:
            summary = await self._generate_summary(context)
        except RunAbortError:
            raise
        except Exception as exc:
            fallback_reason = f"HTML 总结生成失败，已回退 Markdown：{exc}"
            await self._callbacks.on_state_degraded(
                run.run_id,
                RunState.SUMMARY.value,
                fallback_reason,
            )
        else:
            run.set_summary(summary.summary, render_mode="html")
            await self._callbacks.on_state_completed(
                run.run_id,
                RunState.SUMMARY.value,
                summary.as_payload(),
                0,
            )

        run.advance_to(RunState.COMPLETED)
        await self._callbacks.on_state_entered(run.run_id, RunState.COMPLETED.value)
        return RunResult(
            run_id=run.run_id,
            status=run.status,
            final_state=run.current_state,
            summary=run.summary,
            total_duration_ms=int((perf_counter() - started_at) * 1000),
        )

    async def _execute_run_stage(self, context: RunExecutionContext) -> RunReport:
        runtime = self._runtime_for(RunState.RUN.value)
        if runtime is None:
            raise ServiceConfigurationError("Run llm runtime is not configured")
        context.llm_runtime = runtime
        runner = self._agent_runner_factory.create(
            context, label=RunState.RUN.value, runtime=runtime
        )
        run = context.run
        await self._callbacks.on_state_entered(run.run_id, RunState.RUN.value)
        return cast(
            RunReport,
            await self._execute_stage(
                run_id=run.run_id,
                state_name=RunState.RUN,
                context=context,
                execute=lambda: self._run_stage.execute(context, runner),
            ),
        )

    async def _generate_summary(self, context: RunExecutionContext) -> SummaryDraft:
        runtime = self._runtime_for(RunState.SUMMARY.value)
        if runtime is None:
            raise ServiceConfigurationError("Summary llm runtime is not configured")
        context.llm_runtime = runtime
        runner = self._agent_runner_factory.create(
            context, label=RunState.SUMMARY.value, runtime=runtime
        )
        last_error: Exception | None = None
        for _attempt in range(MAX_SUMMARY_ATTEMPTS):
            try:
                return await self._summary_stage.execute(context, runner)
            except RunAbortError:
                raise
            except Exception as exc:
                last_error = exc
        assert last_error is not None
        raise last_error

    async def _execute_stage(
        self,
        *,
        run_id: int,
        state_name: RunState,
        context: RunExecutionContext,
        execute: Callable[[], Awaitable[StageOutput]],
    ) -> StageOutput:
        abort_signal = context.abort_signal
        if isinstance(abort_signal, RunAbortSignal):
            abort_signal.throw_if_aborted()
        started_at = perf_counter()
        output = await execute()
        if isinstance(abort_signal, RunAbortSignal):
            abort_signal.throw_if_aborted()
        await self._callbacks.on_state_completed(
            run_id,
            state_name.value,
            output.as_payload(),
            int((perf_counter() - started_at) * 1000),
        )
        return output

    def _runtime_for(self, stage_name: str) -> object | None:
        return self._stage_runtimes.get(stage_name, self._llm_runtime)

    def _is_market_session_open(self) -> bool:
        try:
            return self._market_session_is_open(self._now_provider())
        except Exception:
            return False

    def _attach_runtime_callbacks(
        self,
        context: RunExecutionContext,
        run_id: int,
    ) -> None:
        async def emit_tool_loop_event(
            stage_name: str,
            event_type: RunEventType,
            payload: dict[str, object],
        ) -> None:
            await self._callbacks.on_tool_loop_event(
                run_id, stage_name, event_type, payload
            )

        async def emit_llm_stream_delta(
            stage_name: str,
            delta: str,
            channel: str = "text",
        ) -> None:
            await self._callbacks.on_llm_stream_delta(
                run_id, stage_name, delta, channel
            )

        async def emit_stage_prompt_prepared(
            stage_name: str,
            payload: dict[str, object],
        ) -> None:
            await self._callbacks.on_stage_prompt_prepared(run_id, stage_name, payload)

        context.tool_loop_event_sink = cast(ToolLoopEventSink, emit_tool_loop_event)
        context.llm_stream_delta_sink = cast(LlmStreamDeltaSink, emit_llm_stream_delta)
        context.stage_prompt_prepared_sink = cast(
            StagePromptPreparedSink, emit_stage_prompt_prepared
        )
