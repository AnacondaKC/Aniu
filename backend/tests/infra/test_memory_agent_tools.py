"""Tests for Run-stage memory tools."""

from __future__ import annotations

from datetime import date

import pytest

from backend.agent.kernel.runtime_config import LlmRuntimeConfig
from backend.agent.tools import ToolRegistry
from backend.business.memories import MemoryService
from backend.business.runs import StrategyRun, StrategySnapshot
from backend.business.runs.execution import RunExecutionContext
from backend.business.shared.enums import TriggerSource
from backend.infra.integrations.agent_runner import (
    AgentRunnerFactoryAdapter,
    _StageToolRegistry,
)
from backend.infra.integrations.agent_runtime import AgentRuntimeFactory
from backend.infra.integrations.dream_agent_tools import DreamReportReadTool
from backend.infra.integrations.memory_agent_tools import (
    MemoryReadTool,
    MemoryWriteTool,
)
from backend.llm import ModelProtocol


@pytest.mark.asyncio
async def test_memory_tools_write_then_read_across_sessions(session_factory) -> None:
    writer = MemoryWriteTool(session_factory)
    created = await writer.run_for_call(
        run_id=200,
        tool_call_id="memory-write-1",
        operation="create",
        content="弱市缩量反弹时不要追高。",
        reason="本次运行观察到追高后回撤。",
    )

    assert created["status"] == "ok"
    assert created["item"]["version"] == 1
    reader = MemoryReadTool(session_factory)
    loaded = await reader.run(keywords="弱市 追高", limit=5)

    assert loaded["status"] == "ok"
    assert loaded["items"][0]["content"] == "弱市缩量反弹时不要追高。"
    assert loaded["items"][0]["reason"] == "本次运行观察到追高后回撤。"


@pytest.mark.asyncio
async def test_dream_report_tool_accepts_harness_metadata(session_factory) -> None:
    result = await DreamReportReadTool(session_factory, date(2026, 8, 18)).run_for_call(
        run_id=20260818401,
        tool_call_id="dream-report-1",
        offset=0,
        limit=10,
    )

    assert result["status"] == "ok"
    assert result["reports"] == []


@pytest.mark.asyncio
async def test_memory_read_schema_exposes_match_mode(session_factory) -> None:
    definition = MemoryReadTool(session_factory).to_tool_definition()
    parameters = definition["parameters"]

    assert set(parameters["properties"]) == {"keywords", "match_mode", "limit"}
    assert parameters["properties"]["match_mode"]["enum"] == ["and", "or"]
    assert parameters["required"] == ["keywords", "match_mode"]


@pytest.mark.asyncio
async def test_memory_write_supports_update_and_soft_delete(session_factory) -> None:
    writer = MemoryWriteTool(session_factory)
    created = await writer.run_for_call(
        run_id=200,
        tool_call_id="memory-write-1",
        operation="create",
        content="弱市不要追高。",
        reason="本次运行观察到追高后回撤。",
    )
    memory_id = created["item"]["id"]

    updated = await writer.run_for_call(
        run_id=201,
        tool_call_id="memory-write-2",
        operation="update",
        memory_id=memory_id,
        expected_version=1,
        content="弱市缩量反弹时不要追高。",
        reason="补充成交量确认条件。",
    )
    assert updated["item"]["version"] == 2

    deleted = await writer.run_for_call(
        run_id=202,
        tool_call_id="memory-write-3",
        operation="delete",
        memory_id=memory_id,
        expected_version=2,
    )
    assert deleted["item"]["version"] == 3
    assert deleted["item"]["deleted_at"] is not None


@pytest.mark.asyncio
async def test_memory_write_schema_matches_operation_requirements(
    session_factory,
) -> None:
    definition = MemoryWriteTool(session_factory).to_tool_definition()
    parameters = definition["parameters"]
    branches = parameters["oneOf"]
    by_operation = {
        branch["properties"]["operation"]["const"]: branch for branch in branches
    }

    assert set(by_operation) == {"create", "update", "delete"}
    assert by_operation["create"]["required"] == ["operation", "content", "reason"]
    assert by_operation["update"]["required"] == [
        "operation",
        "memory_id",
        "expected_version",
        "content",
        "reason",
    ]
    assert by_operation["delete"]["required"] == [
        "operation",
        "memory_id",
        "expected_version",
    ]
    assert "content" not in by_operation["delete"]["properties"]
    assert "reason" not in by_operation["delete"]["properties"]


@pytest.mark.asyncio
async def test_memory_read_returns_payload_when_activity_audit_fails(
    session_factory, monkeypatch
) -> None:
    async def fail_record_read(*_: object, **__: object) -> object:
        raise RuntimeError("activity database is unavailable")

    monkeypatch.setattr(MemoryService, "record_read", fail_record_read)

    result = await MemoryReadTool(session_factory).run_for_call(
        run_id=301,
        tool_call_id="memory-read-audit-failure",
        keywords="不存在的记忆",
        match_mode="and",
        limit=5,
    )

    assert result == {"status": "ok", "items": []}


@pytest.mark.asyncio
async def test_runtime_factory_registers_memory_tools_when_database_available(
    session_factory,
) -> None:
    registry = await AgentRuntimeFactory(
        session_factory=session_factory
    ).build_tool_registry()

    assert {"memory_read", "memory_write"}.issubset(registry.list_tool_names())
    assert registry.get("memory_read").enabled_stages == ("Run",)
    assert registry.get("memory_write").requires_market_open is False


class MemoryWriteClient:
    def __init__(self) -> None:
        self.calls = 0

    async def chat(self, **_: object) -> dict[str, object]:
        self.calls += 1
        if self.calls == 1:
            return {
                "content": None,
                "tool_calls": [
                    {
                        "id": "memory-write-1",
                        "name": "memory_write",
                        "arguments": {
                            "operation": "create",
                            "content": "弱市缩量反弹时不要追高。",
                            "reason": "本次运行观察到追高后回撤。",
                        },
                    }
                ],
            }
        return {"content": "# Report", "tool_calls": []}


def _context(registry: ToolRegistry) -> RunExecutionContext:
    snapshot = StrategySnapshot(prompt_version="v1", risk_rules_version="v1")
    return RunExecutionContext(
        run=StrategyRun(300, TriggerSource.MANUAL, None, snapshot),
        snapshot=snapshot,
        tool_registry=registry,
        market_session_is_open=lambda: False,
    )


@pytest.mark.asyncio
async def test_run_allows_memory_write_closed_market_and_hides_tools_from_summary(
    session_factory,
) -> None:
    registry = ToolRegistry()
    registry.register(MemoryReadTool(session_factory))
    registry.register(MemoryWriteTool(session_factory))
    context = _context(registry)
    runtime = LlmRuntimeConfig(
        protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        base_url="https://example.invalid",
        api_key="test",
        model="test-model",
    )
    runner = AgentRunnerFactoryAdapter(
        MemoryWriteClient(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        invocation_session_factory=session_factory,
    ).create(context, label="Run", runtime=runtime)

    result = await runner.prompt("执行任务")

    assert result.content == "# Report"
    assert any(
        activity["tool_name"] == "memory_write" and activity["status"] == "ok"
        for activity in result.tool_activity
    )
    summary_registry = _StageToolRegistry(
        registry,
        "Summary",
        run_id=300,
        invocation_session_factory=session_factory,
    )
    assert summary_registry.list_tools() == []
