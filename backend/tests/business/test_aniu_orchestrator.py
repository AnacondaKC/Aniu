"""Tests for the two-stage Run -> Summary orchestrator."""

from __future__ import annotations

import pytest

from backend.business.runs import StrategyRun, StrategySnapshot
from backend.business.runs.execution import RunReport, SummaryDraft
from backend.business.runs.orchestration import AniuOrchestrator
from backend.business.shared import RunAbortError
from backend.business.shared.enums import RunState, RunStatus, TriggerSource


def make_run() -> StrategyRun:
    return StrategyRun(
        run_id=1,
        trigger_source=TriggerSource.MANUAL,
        schedule_id=None,
        snapshot=StrategySnapshot(
            prompt_version="v3",
            risk_rules_version="risk-v1",
        ),
    )


class RecordingCallbacks:
    def __init__(self) -> None:
        self.entered: list[str] = []
        self.completed: list[str] = []
        self.degraded: list[str] = []

    async def on_state_entered(self, run_id: int, state_name: str) -> None:
        self.entered.append(f"{run_id}:{state_name}")

    async def on_state_completed(
        self,
        run_id: int,
        state_name: str,
        state_output: dict[str, object],
        duration_ms: int,
    ) -> None:
        assert state_output
        assert duration_ms >= 0
        self.completed.append(f"{run_id}:{state_name}")

    async def on_state_skipped(
        self, run_id: int, state_name: str, summary: str
    ) -> None:
        raise AssertionError((run_id, state_name, summary))

    async def on_state_degraded(
        self, run_id: int, state_name: str, summary: str
    ) -> None:
        self.degraded.append(f"{run_id}:{state_name}:{summary}")

    async def on_stage_prompt_prepared(
        self, run_id: int, stage_name: str, payload: dict[str, object]
    ) -> None:
        del run_id, stage_name, payload

    async def on_tool_loop_event(
        self,
        run_id: int,
        stage_name: str,
        event_type: object,
        payload: dict[str, object],
    ) -> None:
        del run_id, stage_name, event_type, payload

    async def on_llm_stream_delta(
        self,
        run_id: int,
        stage_name: str,
        delta: str,
        channel: str = "text",
    ) -> None:
        del run_id, stage_name, delta, channel


class NullRunner:
    async def prepare_prompt(self, message: str, **kwargs: object) -> str:
        del message, kwargs
        raise AssertionError("stage double should not invoke runner")

    async def prompt(self, message: str, **kwargs: object) -> object:
        del message, kwargs
        raise AssertionError("stage double should not invoke runner")


class NullAgentFactory:
    def create(self, context: object, *, label: str, runtime: object) -> NullRunner:
        del context, label, runtime
        return NullRunner()


class StaticRunStage:
    def __init__(self, report: RunReport | None = None) -> None:
        self.report = report or RunReport(content="# Run report\n\nNo trade.")
        self.calls = 0
        self.market_session_open: bool | None = None

    async def execute(self, context: object, runner: object) -> RunReport:
        del runner
        self.calls += 1
        market_check = getattr(context, "market_session_is_open")
        self.market_session_open = market_check()
        return self.report


class ScriptedSummaryStage:
    def __init__(self, outcomes: list[SummaryDraft | Exception]) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0
        self.report_contents: list[str] = []

    async def execute(self, context: object, runner: object) -> SummaryDraft:
        del runner
        self.calls += 1
        report = getattr(context, "run_report")
        self.report_contents.append(report.content)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def make_orchestrator(
    callbacks: RecordingCallbacks,
    *,
    run_stage: StaticRunStage,
    summary_stage: ScriptedSummaryStage,
    market_open: bool = True,
) -> AniuOrchestrator:
    return AniuOrchestrator(
        state_callbacks=callbacks,
        agent_runner_factory=NullAgentFactory(),
        stage_runtimes={"Run": object(), "Summary": object()},
        run_stage=run_stage,  # type: ignore[arg-type]
        summary_stage=summary_stage,  # type: ignore[arg-type]
        market_session_is_open=lambda _moment: market_open,
    )


@pytest.mark.asyncio
async def test_orchestrator_runs_one_agent_stage_then_html_summary() -> None:
    callbacks = RecordingCallbacks()
    run_stage = StaticRunStage()
    summary_stage = ScriptedSummaryStage(
        [SummaryDraft(summary="<section><h2>Done</h2></section>")]
    )
    run = make_run()

    result = await make_orchestrator(
        callbacks,
        run_stage=run_stage,
        summary_stage=summary_stage,
    ).execute(run)

    assert result.status is RunStatus.COMPLETED
    assert result.final_state is RunState.COMPLETED
    assert result.summary == "<section><h2>Done</h2></section>"
    assert run.summary_render_mode == "html"
    assert run_stage.calls == 1
    assert summary_stage.report_contents == ["# Run report\n\nNo trade."]
    assert callbacks.entered == ["1:Run", "1:Summary", "1:Completed"]
    assert callbacks.completed == ["1:Run", "1:Summary"]
    assert callbacks.degraded == []


@pytest.mark.asyncio
async def test_closed_market_is_context_not_a_skipped_stage() -> None:
    callbacks = RecordingCallbacks()
    run_stage = StaticRunStage()
    summary_stage = ScriptedSummaryStage([SummaryDraft(summary="<p>Done</p>")])

    await make_orchestrator(
        callbacks,
        run_stage=run_stage,
        summary_stage=summary_stage,
        market_open=False,
    ).execute(make_run())

    assert run_stage.market_session_open is False
    assert callbacks.entered == ["1:Run", "1:Summary", "1:Completed"]


@pytest.mark.asyncio
async def test_summary_retries_twice_then_completes_with_markdown_fallback() -> None:
    callbacks = RecordingCallbacks()
    run_stage = StaticRunStage()
    summary_stage = ScriptedSummaryStage(
        [ValueError("bad html"), RuntimeError("provider unavailable")]
    )
    run = make_run()

    result = await make_orchestrator(
        callbacks,
        run_stage=run_stage,
        summary_stage=summary_stage,
    ).execute(run)

    assert result.status is RunStatus.COMPLETED
    assert run.summary == "# Run report\n\nNo trade."
    assert run.summary_render_mode == "markdown"
    assert summary_stage.calls == 2
    assert callbacks.completed == ["1:Run"]
    assert len(callbacks.degraded) == 1
    assert "provider unavailable" in callbacks.degraded[0]


@pytest.mark.asyncio
async def test_summary_abort_propagates_without_completing_the_run() -> None:
    callbacks = RecordingCallbacks()
    summary_abort = ScriptedSummaryStage([RunAbortError(1)])
    run = make_run()

    with pytest.raises(RunAbortError):
        await make_orchestrator(
            callbacks,
            run_stage=StaticRunStage(),
            summary_stage=summary_abort,
        ).execute(run)

    assert run.status is RunStatus.RUNNING
    assert run.current_state is RunState.SUMMARY
    assert summary_abort.calls == 1
    assert callbacks.degraded == []

    class AbortedRunStage(StaticRunStage):
        async def execute(self, context: object, runner: object) -> RunReport:
            del context, runner
            raise RunAbortError(1)

    with pytest.raises(RunAbortError):
        await make_orchestrator(
            RecordingCallbacks(),
            run_stage=AbortedRunStage(),
            summary_stage=ScriptedSummaryStage([SummaryDraft("<p>unused</p>")]),
        ).execute(make_run())


def test_run_report_counts_only_successful_trade_orders() -> None:
    report = RunReport(
        content="# Complete",
        tool_activity=(
            {
                "tool_name": "trade",
                "status": "ok",
                "content": {"success": True, "data": {"orderId": "1"}},
            },
            {
                "tool_name": "cancel",
                "status": "ok",
                "content": {"success": True},
            },
            {"tool_name": "trade", "status": "error", "content": {}},
            {"tool_name": "query_quote", "status": "ok", "content": {}},
        ),
    )

    assert report.as_payload()["trade_count"] == 1
    assert report.as_payload()["tool_calls_count"] == 4
    assert report.as_payload()["tool_failure_count"] == 1
