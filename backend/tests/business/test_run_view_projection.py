"""Tests for the client-safe two-stage run trace projection."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from backend.business.runs import RunTrace, TraceStage, TraceStep
from backend.business.runs.view import project_run_trace


def _trace_with_steps(steps: list[TraceStep], *, status: str = "running") -> RunTrace:
    return RunTrace(
        event_seq=12,
        current_stage_id="run:na" if status == "running" else None,
        updated_at=datetime(2026, 7, 28, 10, 0, tzinfo=UTC),
        stages=[
            TraceStage(
                stage_id="run:na",
                key="run",
                round=None,
                title="任务执行",
                description="完成研究、判断、交易与报告",
                status=status,
                steps=steps,
            )
        ],
    )


def test_projection_excludes_prompt_inputs_and_raw_tool_payloads() -> None:
    trace = _trace_with_steps(
        [
            TraceStep(
                step_id="prompt",
                type="prompt",
                title="发给大模型的输入",
                status="completed",
                content="完整用户提示词",
                data={
                    "system_message": "完整系统提示词",
                    "user_message": "完整用户提示词",
                    "tool_definitions": [{"name": "secret_tool"}],
                },
            ),
            TraceStep(
                step_id="tool:1",
                type="tool",
                title="组合查询",
                status="completed",
                summary="tool-summary-secret",
                data={
                    "tool_call_id": "call-1",
                    "tool_name": "query_portfolio",
                    "arguments": {
                        "instruction": "查询我的模拟组合持仓情况",
                        "limit": 50,
                        "full": False,
                        "prompt": "tool-argument-secret",
                    },
                    "error": "tool-error-secret",
                    "result": {"account_secret": "must-not-reach-client"},
                },
            ),
        ]
    )

    projected = project_run_trace(trace)
    stage = projected["stages"][0]  # type: ignore[index]
    assert set(stage) == {
        "stage_id",
        "key",
        "status",
        "started_at",
        "ended_at",
        "steps",
    }
    assert "updated_at" not in projected
    assert stage["steps"] == [
        {
            "step_id": "tool:1",
            "type": "tool",
            "title": "组合查询",
            "status": "completed",
            "summary": None,
            "content": None,
            "tool_call": {
                "call_id": "call-1",
                "intent_line": "组合查询 · 查询我的模拟组合持仓情况",
                "source": "mx",
                "tool_name": "query_portfolio",
                "display_name": "组合查询",
                "query_parameters": (
                    "instruction=查询我的模拟组合持仓情况 · limit=50 · full=false"
                ),
            },
            "started_at": None,
            "ended_at": None,
        }
    ]
    serialized = json.dumps(projected, ensure_ascii=False)
    for private in (
        "完整系统提示词",
        "完整用户提示词",
        "must-not-reach-client",
        "tool-argument-secret",
        "tool-error-secret",
        "tool-summary-secret",
    ):
        assert private not in serialized


def test_projection_preserves_thinking_and_reports_only() -> None:
    trace = _trace_with_steps(
        [
            TraceStep(
                step_id="thinking",
                type="thinking",
                title="深度思考",
                status="completed",
                content="完整思考过程",
                data={"content_artifact": {"preview": "private"}},
            ),
            TraceStep(
                step_id="result",
                type="result",
                title="生成运行报告",
                status="completed",
                content="运行报告正文",
                data={"provider_payload": "private"},
            ),
        ]
    )

    steps = project_run_trace(trace)["stages"][0]["steps"]  # type: ignore[index]
    assert [step["content"] for step in steps] == ["完整思考过程", "运行报告正文"]
    assert "private" not in json.dumps(steps, ensure_ascii=False)


def test_projection_exposes_direct_mx_tool_metadata() -> None:
    trace = _trace_with_steps(
        [
            TraceStep(
                step_id="tool:mx",
                type="tool",
                title="金融数据查询",
                status="completed",
                data={
                    "tool_call_id": "agent-call",
                    "tool_name": "query_market_data",
                    "arguments": {"query": "贵州茅台最新股价"},
                    "stock_api_calls": [
                        {
                            "call_id": "stock-call",
                            "run_id": 1,
                            "stage_name": "Run",
                            "provider": "mx",
                            "interface_name": "金融数据查询",
                            "interface_identifier": "金融数据查询",
                            "operation_id": "query_market_data",
                            "parameters": {"toolQuery": "贵州茅台最新股价"},
                            "status": "success",
                            "duration_ms": 42,
                            "response_characters": 128,
                            "error_message": None,
                        }
                    ],
                    "model_content_characters": 42_872,
                    "result": {"private": True},
                },
            )
        ]
    )

    tool_call = project_run_trace(trace)["stages"][0]["steps"][0][  # type: ignore[index]
        "tool_call"
    ]
    assert tool_call["source"] == "mx"
    assert tool_call["model_content_characters"] == 42_872
    assert tool_call["stock_api_calls"][0]["parameters"] == {  # type: ignore[index]
        "toolQuery": "贵州茅台最新股价"
    }
    assert "run_id" not in json.dumps(tool_call)
    assert "stage_name" not in json.dumps(tool_call)


def test_projection_hides_public_provider_routing_details() -> None:
    trace = _trace_with_steps(
        [
            TraceStep(
                step_id="tool:public",
                type="tool",
                title="实时行情",
                status="completed",
                data={
                    "tool_call_id": "public-call",
                    "tool_name": "stock_quote",
                    "arguments": {"symbols": ["600519.SH"], "detail": "full"},
                    "stock_api_calls": [
                        {
                            "call_id": "public-stock-call",
                            "provider": "eastmoney",
                            "interface_name": "https://private.example.com",
                            "interface_identifier": "private_endpoint",
                            "operation_id": "em_private_endpoint",
                            "parameters": {"secid": "1.600519"},
                            "status": "success",
                            "duration_ms": 26,
                            "response_characters": 256,
                        }
                    ],
                },
            )
        ]
    )

    tool_call = project_run_trace(trace)["stages"][0]["steps"][0][  # type: ignore[index]
        "tool_call"
    ]
    stock_call = tool_call["stock_api_calls"][0]  # type: ignore[index]
    assert tool_call["source"] == "public"
    assert stock_call["interface_name"] == "公开数据"
    assert stock_call["parameters"] == {}
    assert "private.example.com" not in json.dumps(tool_call)


def test_projection_preserves_degraded_summary_status() -> None:
    trace = RunTrace(
        stages=[
            TraceStage(
                stage_id="summary:na",
                key="summary",
                round=None,
                title="展示总结",
                description="生成 HTML 总结",
                status="degraded",
                steps=[
                    TraceStep(
                        step_id="markdown_fallback",
                        type="status",
                        title="回退 Markdown",
                        status="completed",
                        summary="HTML 总结失败",
                    )
                ],
            )
        ]
    )

    stage = project_run_trace(trace)["stages"][0]  # type: ignore[index]
    assert stage["key"] == "summary"
    assert stage["status"] == "degraded"
    assert stage["steps"][0]["summary"] == "HTML 总结失败"
