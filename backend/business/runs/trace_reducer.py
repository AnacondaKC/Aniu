"""Pure RunTrace reducer — no IO, no publish, no commit.

Given a current ``RunTrace`` and a typed domain event, return the next trace.
Illegal transitions raise ``ValueError`` so callers can fail closed.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime

from backend.business.runs.pipeline_stages import (
    TRACE_STAGE_META,
    pipeline_stage_for_state_name,
    pipeline_stage_for_trace_key,
)
from backend.business.runs.run_events import (
    StageCompleted,
    StageDegraded,
    StageEntered,
    StageFailed,
    StageSkipped,
    TextDelta,
    ToolCallFinished,
    ToolCallStarted,
    TraceEvent,
)
from backend.business.runs.run_trace import RunTrace, TraceStage, TraceStep, utc_now


def reduce_trace(trace: RunTrace, event: TraceEvent) -> RunTrace:
    """Apply one event and return a new ``RunTrace`` (input is not mutated)."""

    next_trace = deepcopy(trace)
    if isinstance(event, StageEntered):
        _apply_stage_entered(next_trace, event)
    elif isinstance(event, StageCompleted):
        _apply_stage_completed(next_trace, event)
    elif isinstance(event, StageSkipped):
        _apply_stage_skipped(next_trace, event)
    elif isinstance(event, StageDegraded):
        _apply_stage_degraded(next_trace, event)
    elif isinstance(event, StageFailed):
        _apply_stage_failed(next_trace, event)
    elif isinstance(event, ToolCallStarted):
        _apply_tool_started(next_trace, event)
    elif isinstance(event, ToolCallFinished):
        _apply_tool_finished(next_trace, event)
    elif isinstance(event, TextDelta):
        _apply_text_delta(next_trace, event)
    else:  # pragma: no cover - exhaustiveness guard
        raise ValueError(f"unsupported trace event: {type(event)!r}")
    if event.event_seq is not None:
        if event.event_seq <= trace.event_seq:
            raise ValueError(
                f"event_seq must advance beyond {trace.event_seq}: {event.event_seq}"
            )
        next_trace.event_seq = event.event_seq
    else:
        next_trace.event_seq = trace.event_seq + 1
    next_trace.updated_at = _event_time(event)
    return next_trace


def _event_time(event: TraceEvent) -> datetime:
    return event.occurred_at


def _resolve_stage_meta(stage_name: str) -> tuple[str, str, str]:
    """Return (key, title, description)."""

    stage = pipeline_stage_for_state_name(stage_name) or pipeline_stage_for_trace_key(
        stage_name
    )
    if stage is not None:
        return stage.trace_key, stage.title, stage.description
    if stage_name in TRACE_STAGE_META:
        title, description = TRACE_STAGE_META[stage_name]
        return stage_name, title, description
    raise ValueError(f"unknown stage: {stage_name}")


def _latest_stage_for_key(trace: RunTrace, key: str) -> TraceStage | None:
    """Return the newest round of a stage key (stages are append-ordered)."""

    for stage in reversed(trace.stages):
        if stage.key == key:
            return stage
    return None


def _append_stage(
    trace: RunTrace,
    *,
    key: str,
    title: str,
    description: str,
    round_index: int | None,
) -> TraceStage:
    display_title = title if round_index is None else f"{title}（第{round_index}轮）"
    stage = TraceStage(
        stage_id=f"{key}:{'na' if round_index is None else round_index}",
        key=key,
        round=round_index,
        title=display_title,
        description=description,
        status="pending",
    )
    trace.stages.append(stage)
    return stage


def _ensure_stage(trace: RunTrace, stage_name: str) -> TraceStage:
    """Resolve the newest round of a stage, creating round one when missing."""

    key, title, description = _resolve_stage_meta(stage_name)
    existing = _latest_stage_for_key(trace, key)
    if existing is not None:
        return existing
    return _append_stage(
        trace,
        key=key,
        title=title,
        description=description,
        round_index=None,
    )


def _apply_stage_entered(trace: RunTrace, event: StageEntered) -> None:
    key, title, description = _resolve_stage_meta(event.stage_name)
    stage = _latest_stage_for_key(trace, key)
    if stage is None:
        stage = _append_stage(
            trace,
            key=key,
            title=title,
            description=description,
            round_index=None,
        )
    elif stage.status != "pending":
        raise ValueError(f"cannot re-enter stage: {stage.stage_id}")
    now = event.occurred_at or utc_now()
    if stage.started_at is None:
        stage.started_at = now
    stage.status = "running"
    stage.ended_at = None
    trace.current_stage_id = stage.stage_id


def _close_running_steps(stage: TraceStage, *, status: str, now: datetime) -> None:
    for step in stage.steps:
        if step.status == "running":
            step.status = status
            step.ended_at = now


def _apply_stage_completed(trace: RunTrace, event: StageCompleted) -> None:
    stage = _ensure_stage(trace, event.stage_name)
    now = event.occurred_at or utc_now()
    if stage.status == "failed":
        raise ValueError(f"cannot complete failed stage: {stage.stage_id}")
    stage.status = "completed"
    if event.summary is not None:
        stage.summary = event.summary
    stage.ended_at = now
    _close_running_steps(stage, status="completed", now=now)
    if trace.current_stage_id == stage.stage_id:
        trace.current_stage_id = None


def _apply_stage_skipped(trace: RunTrace, event: StageSkipped) -> None:
    stage = _ensure_stage(trace, event.stage_name)
    now = event.occurred_at or utc_now()
    if stage.status == "failed":
        raise ValueError(f"cannot skip failed stage: {stage.stage_id}")
    if stage.started_at is None:
        stage.started_at = now
    stage.status = "skipped"
    stage.summary = event.summary
    stage.ended_at = now
    _close_running_steps(stage, status="blocked", now=now)
    if trace.current_stage_id == stage.stage_id:
        trace.current_stage_id = None


def _apply_stage_degraded(trace: RunTrace, event: StageDegraded) -> None:
    stage = _ensure_stage(trace, event.stage_name)
    now = event.occurred_at or utc_now()
    if stage.started_at is None:
        stage.started_at = now
    stage.status = "degraded"
    stage.summary = event.summary
    stage.ended_at = now
    _close_running_steps(stage, status="failed", now=now)
    if trace.current_stage_id == stage.stage_id:
        trace.current_stage_id = None


def _apply_stage_failed(trace: RunTrace, event: StageFailed) -> None:
    stage = _ensure_stage(trace, event.stage_name)
    now = event.occurred_at or utc_now()
    stage.status = "failed"
    if event.summary is not None:
        stage.summary = event.summary
    stage.ended_at = now
    _close_running_steps(stage, status="failed", now=now)
    if trace.current_stage_id == stage.stage_id:
        trace.current_stage_id = None


def _tool_step_id(tool_call_id: str, tool_name: str) -> str:
    return f"tool:{tool_call_id or tool_name}"


def _find_step(stage: TraceStage, step_id: str) -> TraceStep | None:
    for step in stage.steps:
        if step.step_id == step_id:
            return step
    return None


def _apply_tool_started(trace: RunTrace, event: ToolCallStarted) -> None:
    stage = _ensure_stage(trace, event.stage_name)
    if stage.status not in {"running", "pending"}:
        raise ValueError(
            f"cannot start tool on stage in status {stage.status}: {stage.stage_id}"
        )
    now = event.occurred_at or utc_now()
    step_id = _tool_step_id(event.tool_call_id, event.tool_name)
    step = _find_step(stage, step_id)
    if step is None:
        step = TraceStep(
            step_id=step_id,
            type="tool",
            title=event.title or event.tool_name or "tool",
            status="running",
        )
        stage.steps.append(step)
    if step.started_at is None:
        step.started_at = now
    step.title = event.title or step.title
    step.status = "running"
    step.summary = event.summary
    step.data = {
        "tool_call_id": event.tool_call_id,
        "tool_name": event.tool_name,
        "arguments": event.arguments or {},
        "result": None,
        "error": None,
        "model_content_characters": None,
        "stock_api_calls": [],
    }
    if stage.status == "pending":
        stage.status = "running"
        if stage.started_at is None:
            stage.started_at = now
    trace.current_stage_id = stage.stage_id
    _move_result_to_end(stage)


def _apply_tool_finished(trace: RunTrace, event: ToolCallFinished) -> None:
    stage = _ensure_stage(trace, event.stage_name)
    now = event.occurred_at or utc_now()
    step_id = _tool_step_id(event.tool_call_id, event.tool_name)
    step = _find_step(stage, step_id)
    if step is None:
        step = TraceStep(
            step_id=step_id,
            type="tool",
            title=event.title or event.tool_name or "tool",
            status=event.status,
        )
        stage.steps.append(step)
    if step.started_at is None:
        step.started_at = now
    if event.status not in {"completed", "blocked", "failed", "error", "ok"}:
        raise ValueError(f"invalid tool status: {event.status}")
    normalized = (
        "completed"
        if event.status in {"completed", "ok"}
        else ("blocked" if event.status == "blocked" else "failed")
    )
    step.title = event.title or step.title
    step.status = normalized
    step.summary = event.summary
    step.ended_at = now
    step.data = {
        "tool_call_id": event.tool_call_id,
        "tool_name": event.tool_name,
        "arguments": event.arguments or {},
        "error": event.error,
        "model_content_characters": event.model_content_characters,
        "stock_api_calls": [dict(call) for call in event.stock_api_calls],
    }
    _move_result_to_end(stage)


def _move_result_to_end(stage: TraceStage) -> None:
    for index, step in enumerate(stage.steps):
        if step.step_id != "result" or index == len(stage.steps) - 1:
            continue
        stage.steps.append(stage.steps.pop(index))
        return


def _apply_text_delta(trace: RunTrace, event: TextDelta) -> None:
    if not event.delta:
        return
    stage = _ensure_stage(trace, event.stage_name)
    now = event.occurred_at or utc_now()
    step = _find_step(stage, event.step_id)
    step_type = "thinking" if event.channel == "thinking" else "result"
    if step is None:
        step = TraceStep(
            step_id=event.step_id,
            type=step_type,
            title="thinking" if step_type == "thinking" else "result",
            status="running",
            content="",
        )
        stage.steps.append(step)
    if step.started_at is None:
        step.started_at = now
    step.status = "running"
    step.content = f"{step.content or ''}{event.delta}"
    if stage.status == "pending":
        stage.status = "running"
        if stage.started_at is None:
            stage.started_at = now
    trace.current_stage_id = stage.stage_id
