"""Tests for metrics derived from persisted two-stage traces."""

from backend.business.runs import (
    metrics_from_trace_payload,
    run_stage_status_from_trace_payload,
)


def test_metrics_use_only_completed_run_stage_trade_count() -> None:
    trace = {
        "stages": [
            {
                "key": "run",
                "status": "completed",
                "steps": [{"type": "result", "data": {"trade_count": 2}}],
            },
            {
                "key": "summary",
                "status": "completed",
                "steps": [{"type": "result", "data": {"trade_count": 8}}],
            },
        ]
    }

    assert metrics_from_trace_payload(trace) == (0, 0, 0, 2)
    assert run_stage_status_from_trace_payload(trace) == "completed"


def test_tool_metrics_use_model_content_characters_after_redaction() -> None:
    trace = {
        "stages": [
            {
                "key": "run",
                "status": "completed",
                "steps": [
                    {
                        "type": "tool",
                        "data": {
                            "arguments": {"x": "a"},
                            "model_content_characters": 42,
                            "result": "this legacy payload must not be counted",
                        },
                    }
                ],
            }
        ]
    }

    assert metrics_from_trace_payload(trace) == (1, 0, 13, 0)


def test_non_completed_run_does_not_report_trade_count() -> None:
    trace = {
        "stages": [
            {
                "key": "run",
                "status": "failed",
                "steps": [{"type": "result", "data": {"trade_count": 3}}],
            }
        ]
    }

    assert metrics_from_trace_payload(trace)[3] == 0
    assert run_stage_status_from_trace_payload(trace) == "failed"
