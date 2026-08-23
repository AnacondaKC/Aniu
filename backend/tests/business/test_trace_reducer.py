"""Unit tests for the pure two-stage RunTrace reducer."""

from __future__ import annotations

import pytest

from backend.business.runs import (
    RunTrace,
    StageCompleted,
    StageDegraded,
    StageEntered,
    StageFailed,
    TextDelta,
    ToolCallFinished,
    ToolCallStarted,
    reduce_trace,
)


def test_typed_event_has_traceable_envelope() -> None:
    event = StageEntered(
        run_id=7,
        stage_name="Run",
        event_seq=3,
        correlation_id="request-1",
        causation_id="command-1",
        producer="run-worker",
    )

    projection = reduce_trace(RunTrace(), event)

    assert event.event_id
    assert event.occurred_at.tzinfo is not None
    assert projection.event_seq == 3
    assert projection.schema_version == 3
    with pytest.raises(ValueError, match="cannot re-enter stage"):
        reduce_trace(projection, event)
    with pytest.raises(ValueError, match="run_id must be positive"):
        StageEntered(run_id=0, stage_name="Run")


def test_run_lifecycle_and_tool_call() -> None:
    original = RunTrace()
    trace = reduce_trace(original, StageEntered(run_id=1, stage_name="Run"))
    assert trace.current_stage_id == "run:na"
    assert trace.stages[0].status == "running"
    assert original.stages == []

    trace = reduce_trace(
        trace,
        ToolCallStarted(
            run_id=1,
            stage_name="Run",
            tool_call_id="c1",
            tool_name="search",
            arguments={"q": "x"},
        ),
    )
    assert trace.stages[0].steps[0].status == "running"

    trace = reduce_trace(
        trace,
        ToolCallFinished(
            run_id=1,
            stage_name="Run",
            tool_call_id="c1",
            tool_name="search",
            status="ok",
            model_content_characters=10,
        ),
    )
    step = trace.stages[0].steps[0]
    assert step.status == "completed"
    assert step.data == {
        "tool_call_id": "c1",
        "tool_name": "search",
        "arguments": {},
        "error": None,
        "model_content_characters": 10,
        "stock_api_calls": [],
    }

    trace = reduce_trace(
        trace,
        StageCompleted(run_id=1, stage_name="Run", summary="done"),
    )
    assert trace.stages[0].status == "completed"
    assert trace.current_stage_id is None


def test_summary_degraded_is_terminal_trace_status() -> None:
    trace = reduce_trace(RunTrace(), StageEntered(run_id=1, stage_name="Summary"))
    trace = reduce_trace(
        trace,
        StageDegraded(
            run_id=1,
            stage_name="Summary",
            summary="HTML failed; Markdown fallback",
        ),
    )

    stage = trace.stages[0]
    assert stage.key == "summary"
    assert stage.status == "degraded"
    assert stage.ended_at is not None
    assert trace.current_stage_id is None


def test_reentering_running_stage_is_rejected() -> None:
    trace = reduce_trace(RunTrace(), StageEntered(run_id=1, stage_name="Run"))
    with pytest.raises(ValueError, match="cannot re-enter stage"):
        reduce_trace(trace, StageEntered(run_id=1, stage_name="Run"))


def test_text_delta_accumulates() -> None:
    trace = reduce_trace(RunTrace(), StageEntered(run_id=1, stage_name="Run"))
    for delta in ("hello", " world"):
        trace = reduce_trace(
            trace,
            TextDelta(
                run_id=1,
                stage_name="Run",
                step_id="result",
                delta=delta,
            ),
        )

    step = next(item for item in trace.stages[0].steps if item.step_id == "result")
    assert step.content == "hello world"


def test_fail_run_stage_closes_running_tools() -> None:
    trace = reduce_trace(RunTrace(), StageEntered(run_id=1, stage_name="Run"))
    trace = reduce_trace(
        trace,
        ToolCallStarted(
            run_id=1,
            stage_name="Run",
            tool_call_id="t1",
            tool_name="trade",
        ),
    )
    trace = reduce_trace(
        trace,
        StageFailed(run_id=1, stage_name="Run", summary="boom"),
    )

    assert trace.stages[0].status == "failed"
    assert trace.stages[0].steps[0].status == "failed"
