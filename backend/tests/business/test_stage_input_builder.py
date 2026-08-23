"""Tests for deterministic Summary evidence assembly and budgeting."""

from __future__ import annotations

import pytest

from backend.business.runs.execution import RunReport
from backend.business.runs.stage_input import (
    SummaryEvidenceTooLargeError,
    build_summary_stage_payload,
)
from backend.business.shared.serialization import serialize_context
from backend.llm import estimate_tokens


def _report() -> RunReport:
    return RunReport(
        content="# Run report\n\nActual conclusion.",
        transcript=(
            {"role": "assistant", "reasoning": "old reasoning " * 100},
            {"role": "tool", "content": "ignored"},
            {"role": "assistant", "reasoning": "new reasoning " * 100},
        ),
        tool_activity=(
            {
                "tool_call_id": "query-1",
                "tool_name": "query_quote",
                "status": "ok",
                "content": {"rows": ["market data " * 500]},
            },
            {
                "tool_call_id": "trade-1",
                "tool_name": "trade",
                "status": "ok",
                "content": {"success": True, "data": {"orderId": "123"}},
            },
            {
                "tool_call_id": "query-2",
                "tool_name": "query_portfolio",
                "status": "error",
                "error": "upstream unavailable",
                "content": {"diagnostic": "full error evidence"},
            },
        ),
    )


def test_payload_keeps_complete_evidence_when_it_fits() -> None:
    payload = build_summary_stage_payload(_report(), max_characters=100_000)

    assert payload["run_report_markdown"] == "# Run report\n\nActual conclusion."
    assert [item["index"] for item in payload["reasoning_segments"]] == [1, 2]
    tools = payload["tool_calls"]
    assert [item["tool_call_id"] for item in tools] == [
        "query-1",
        "trade-1",
        "query-2",
    ]
    assert "evidence_omissions" not in payload


def test_budget_drops_query_evidence_before_oldest_reasoning() -> None:
    report = _report()
    payload = build_summary_stage_payload(report, max_characters=1_300)

    assert payload["run_report_markdown"] == report.content
    tools = payload["tool_calls"]
    assert all(item["tool_call_id"] != "query-1" for item in tools)
    assert next(item for item in tools if item["tool_call_id"] == "trade-1")[
        "content"
    ] == {"success": True, "data": {"orderId": "123"}}
    assert next(item for item in tools if item["tool_call_id"] == "query-2")[
        "content"
    ] == {"diagnostic": "full error evidence"}

    omissions = payload["evidence_omissions"]
    assert omissions["query_tool_calls"] == {
        "count": 1,
        "first_activity_index": 1,
        "last_activity_index": 1,
    }
    assert omissions["reasoning_segments"]["first_index"] == 1
    remaining = payload["reasoning_segments"]
    assert not remaining or remaining[0]["index"] == 2


def test_many_query_records_can_be_omitted_entirely() -> None:
    report = RunReport(
        content="# Required report",
        tool_activity=tuple(
            {
                "tool_call_id": f"query-{index}",
                "tool_name": "query_quote",
                "status": "ok",
                "content": "x" * 2_000,
            }
            for index in range(50)
        )
        + (
            {
                "tool_call_id": "cancel-1",
                "tool_name": "cancel",
                "status": "blocked",
                "error": "market closed",
                "content": {"instruction": "cancel 1"},
            },
        ),
    )

    payload = build_summary_stage_payload(report, max_characters=1_100)

    assert payload["tool_calls"][-1]["tool_call_id"] == "cancel-1"
    omission = payload["evidence_omissions"]["query_tool_calls"]
    assert omission["count"] > 0
    assert omission["first_activity_index"] == 1
    assert omission["last_activity_index"] == omission["count"]


def test_required_report_or_trade_evidence_is_never_clipped() -> None:
    report = RunReport(
        content="r" * 1_000,
        tool_activity=(
            {
                "tool_name": "trade",
                "status": "error",
                "error": "e" * 1_000,
                "content": "c" * 1_000,
            },
        ),
    )

    with pytest.raises(
        SummaryEvidenceTooLargeError,
        match="required trade/error evidence",
    ):
        build_summary_stage_payload(report, max_characters=500)


def test_empty_budget_is_rejected() -> None:
    with pytest.raises(SummaryEvidenceTooLargeError, match="budget is empty"):
        build_summary_stage_payload(RunReport(content="report"), max_characters=0)


def test_token_budget_bounds_cjk_evidence_by_estimated_tokens() -> None:
    report = RunReport(content="结论" * 800)

    payload = build_summary_stage_payload(report, max_tokens=1_800)

    assert estimate_tokens(serialize_context(payload)) <= 1_800
    with pytest.raises(SummaryEvidenceTooLargeError, match="required trade/error"):
        build_summary_stage_payload(report, max_tokens=1_000)
