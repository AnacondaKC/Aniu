"""Tests for complete persisted Trace v2 projection."""

from __future__ import annotations

from backend.business.runs import bound_trace_payload


def test_large_trace_drops_only_unused_tool_results() -> None:
    steps = [
        {
            "step_id": f"tool:{index}",
            "type": "tool",
            "content": "正文" * 100_000,
            "data": {
                "arguments": {
                    "query": "参数" * 100_000,
                    "internal_payload": "完整数据" * 100_000,
                    "result": "参数中的同名字段必须保留",
                },
                "result": "结果" * 100_000,
            },
        }
        for index in range(40)
    ]
    payload: dict[str, object] = {
        "schema_version": 2,
        "event_seq": 1,
        "current_stage_id": "research:na",
        "updated_at": "2026-01-01T00:00:00+00:00",
        "stages": [{"stage_id": "research:na", "steps": steps}],
    }

    projected = bound_trace_payload(payload)

    projected_steps = projected["stages"][0]["steps"]
    assert len(projected_steps) == 40
    assert projected_steps[0]["content"] == "正文" * 100_000
    assert projected_steps[0]["data"]["arguments"]["internal_payload"] == (
        "完整数据" * 100_000
    )
    assert projected_steps[0]["data"]["arguments"]["result"] == (
        "参数中的同名字段必须保留"
    )
    assert "result" not in projected_steps[0]["data"]
    assert payload["stages"][0]["steps"][0]["data"]["result"]
    assert projected is not payload


def test_small_trace_is_not_mutated() -> None:
    payload: dict[str, object] = {
        "schema_version": 2,
        "event_seq": 0,
        "current_stage_id": None,
        "stages": [],
        "updated_at": "2026-01-01T00:00:00+00:00",
    }

    projected = bound_trace_payload(payload)

    assert projected == payload
    assert projected is not payload
