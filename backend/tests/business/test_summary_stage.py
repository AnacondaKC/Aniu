"""Tests for raw HTML Summary generation."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.business.runs import StrategyRun, StrategySnapshot
from backend.business.runs.agent_runner import AgentStageResult
from backend.business.runs.execution import RunExecutionContext, RunReport
from backend.business.runs.stages.summary_stage import (
    SummaryStage,
    _coerce_html_summary,
)
from backend.business.shared.enums import TriggerSource


class RecordingRunner:
    def __init__(self, response: str) -> None:
        self.response = response
        self.prompts: list[str] = []

    async def prepare_prompt(self, message: str, **kwargs: object) -> str:
        del kwargs
        return message

    async def prompt(
        self, message: str, *, abort_signal: object | None = None
    ) -> AgentStageResult:
        del abort_signal
        self.prompts.append(message)
        return AgentStageResult(content=self.response)


def _context(report: RunReport | None = None) -> RunExecutionContext:
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
    context.llm_runtime = SimpleNamespace(
        context_window_tokens=16_000,
        max_output_tokens=2_000,
    )
    context.run_report = report or RunReport(content="# Report\n\nNo trade.")
    return context


@pytest.mark.asyncio
async def test_summary_stage_uses_report_reasoning_and_tool_evidence() -> None:
    report = RunReport(
        content="# Complete report",
        transcript=({"role": "assistant", "reasoning": "checked risk"},),
        tool_activity=(
            {
                "tool_call_id": "trade-1",
                "tool_name": "trade",
                "status": "ok",
                "content": {"success": True, "orderId": "1"},
            },
        ),
    )
    runner = RecordingRunner('<section class="report"><h2>Done</h2></section>')

    result = await SummaryStage().execute(_context(report), runner)

    assert result.summary == '<section class="report"><h2>Done</h2></section>'
    assert len(runner.prompts) == 1
    assert "# Complete report" in runner.prompts[0]
    assert "checked risk" in runner.prompts[0]
    assert "trade-1" in runner.prompts[0]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            "```html\n<section><p>Safe</p></section>\n```",
            "<section><p>Safe</p></section>",
        ),
        (
            '{"html":"<article><strong>Safe</strong></article>"}',
            "<article><strong>Safe</strong></article>",
        ),
        (
            '<section onclick="steal()" style="color:red"><p>Raw</p>'
            '<script>alert(1)</script><img src="https://bad.test/x"></section>',
            '<section onclick="steal()" style="color:red"><p>Raw</p>'
            '<script>alert(1)</script><img src="https://bad.test/x"></section>',
        ),
        (
            '<table><tr><th scope="col" colspan="2">Name</th></tr></table>',
            '<table><tr><th scope="col" colspan="2">Name</th></tr></table>',
        ),
    ],
)
def test_html_summary_envelopes_are_unwrapped_without_sanitizing(
    raw: str, expected: str
) -> None:
    assert _coerce_html_summary(raw) == expected


@pytest.mark.parametrize("raw", ["", "   "])
def test_empty_html_is_rejected(raw: str) -> None:
    with pytest.raises(ValueError, match="HTML summary"):
        _coerce_html_summary(raw)


def test_raw_script_and_style_content_is_preserved() -> None:
    raw = "<style>body{display:none}</style><script>alert(1)</script>"
    assert _coerce_html_summary(raw) == raw


@pytest.mark.asyncio
async def test_summary_requires_run_report_and_runtime() -> None:
    context = _context()
    context.run_report = None
    with pytest.raises(ValueError, match="run report"):
        await SummaryStage().execute(context, RecordingRunner("<p>unused</p>"))

    context = _context()
    context.llm_runtime = None
    with pytest.raises(Exception, match="configured llm runtime"):
        await SummaryStage().execute(context, RecordingRunner("<p>unused</p>"))
