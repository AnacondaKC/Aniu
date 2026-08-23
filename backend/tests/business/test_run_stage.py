"""Tests for the single continuous Run agent stage."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.business.runs import StrategyRun, StrategySnapshot
from backend.business.runs.agent_runner import AgentStageResult
from backend.business.runs.execution import RunExecutionContext
from backend.business.runs.stages.run_stage import RunStage
from backend.business.shared.enums import TriggerSource


class RecordingRunner:
    def __init__(self, result: AgentStageResult) -> None:
        self.result = result
        self.prompts: list[str] = []

    async def prepare_prompt(self, message: str, **kwargs: object) -> str:
        del kwargs
        return message

    async def prompt(
        self, message: str, *, abort_signal: object | None = None
    ) -> AgentStageResult:
        del abort_signal
        self.prompts.append(message)
        return self.result


def _context(*, market_open: bool) -> RunExecutionContext:
    snapshot = StrategySnapshot(
        prompt_version="v3",
        risk_rules_version="risk-v1",
    )
    run = StrategyRun(
        run_id=1,
        trigger_source=TriggerSource.MANUAL,
        schedule_id=None,
        snapshot=snapshot,
    )
    context = RunExecutionContext(run=run, snapshot=snapshot)
    context.llm_runtime = SimpleNamespace()
    context.market_session_is_open = lambda: market_open
    return context


@pytest.mark.asyncio
async def test_run_stage_uses_one_prompt_and_preserves_raw_execution_evidence() -> None:
    activity = (
        {
            "tool_call_id": "quote-1",
            "tool_name": "query_quote",
            "status": "ok",
            "content": {"price": 10},
        },
    )
    transcript = (
        {"role": "assistant", "reasoning": "inspect market"},
        {"role": "assistant", "content": "# Final report"},
    )
    runner = RecordingRunner(
        AgentStageResult(
            content="  # Final report\n\nNo trade.  ",
            tool_activity=activity,
            transcript=transcript,
        )
    )

    result = await RunStage().execute(_context(market_open=False), runner)

    assert result.content == "# Final report\n\nNo trade."
    assert result.tool_activity == activity
    assert result.transcript == transcript
    assert len(runner.prompts) == 1
    assert '"market_session_open":false' in runner.prompts[0]
    assert "最终只输出 Markdown 报告正文" in runner.prompts[0]


@pytest.mark.asyncio
async def test_run_stage_rejects_empty_report() -> None:
    runner = RecordingRunner(AgentStageResult(content="  \n"))

    with pytest.raises(ValueError, match="run report must not be empty"):
        await RunStage().execute(_context(market_open=True), runner)


@pytest.mark.asyncio
async def test_run_stage_requires_runtime_before_emitting_or_prompting() -> None:
    context = _context(market_open=True)
    context.llm_runtime = None
    runner = RecordingRunner(AgentStageResult(content="# unused"))

    with pytest.raises(Exception, match="configured llm runtime"):
        await RunStage().execute(context, runner)

    assert runner.prompts == []
