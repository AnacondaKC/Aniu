"""Summary stage: turn one completed Run report and its evidence into HTML."""

from __future__ import annotations

import json
from typing import Any

from backend.business.runs import RunEventType
from backend.business.runs.agent_runner import AgentRunnerPort
from backend.business.runs.execution import RunExecutionContext, SummaryDraft
from backend.business.runs.stage_input import build_summary_stage_payload
from backend.business.runs.stages.stage_helpers import (
    emit_stage_prompt_prepared,
    emit_tool_loop_event,
    require_llm_runtime,
)
from backend.business.shared.serialization import serialize_context
from backend.llm import estimate_tokens

_SUMMARY_INPUT_RESERVE_TOKENS = 2_000


def _summary_payload_budget(
    context: RunExecutionContext,
    stage_prompt: str,
) -> int:
    runtime = context.llm_runtime
    context_tokens = int(getattr(runtime, "context_window_tokens", 128_000))
    output_tokens = int(getattr(runtime, "max_output_tokens", 32_768))
    available_tokens = (
        context_tokens - output_tokens - _SUMMARY_INPUT_RESERVE_TOKENS
    )
    prompt_overhead = estimate_tokens(stage_prompt + "\n\nsummary_source_data:\n")
    return max(0, available_tokens - prompt_overhead)


class SummaryStage:
    async def execute(
        self,
        context: RunExecutionContext,
        agent_runner: AgentRunnerPort,
    ) -> SummaryDraft:
        require_llm_runtime(context, stage_name="Summary")
        report = context.run_report
        if report is None:
            raise ValueError("run report must be available before summary")
        stage_prompt = context.snapshot.settings_for_stage("Summary").prompt
        payload = build_summary_stage_payload(
            report,
            max_tokens=_summary_payload_budget(context, stage_prompt),
        )
        user_prompt = "\n\n".join(
            (
                stage_prompt,
                "summary_source_data:\n" + serialize_context(payload),
            )
        )
        await emit_stage_prompt_prepared(
            context,
            stage_name="Summary",
            phase="summary_input",
            title="HTML 总结提示词",
            summary="已载入 Markdown 报告、思考和工具执行证据",
            display_prompt=stage_prompt,
            payload=payload,
            user_message=user_prompt,
        )
        await emit_tool_loop_event(
            context,
            stage_name="Summary",
            event_type=RunEventType.SUMMARY_GENERATION_STARTED,
            payload={"stage_name": "Summary", "summary": "开始生成 HTML 总结"},
        )
        result = await agent_runner.prompt(
            user_prompt,
            abort_signal=context.abort_signal,
        )
        return SummaryDraft(summary=_coerce_html_summary(result.content))


def _coerce_html_summary(text: str) -> str:
    summary = text.strip()
    if summary.startswith("```") and summary.endswith("```"):
        lines = summary.splitlines()
        if len(lines) >= 2:
            summary = "\n".join(lines[1:-1]).strip()
    try:
        parsed: Any = json.loads(summary)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, dict) and "html" in parsed:
        summary = str(parsed.get("html") or "").strip()
    if not summary:
        raise ValueError("HTML summary must not be empty")
    return summary


__all__ = ["SummaryStage"]
