"""Regression tests for durable write-tool idempotency."""

from __future__ import annotations

import pytest

from backend.agent.actors.tool_executor import execute_tool_call_batch
from backend.agent.kernel.context import AgentContext
from backend.business.runs import StrategyRun, StrategySnapshot
from backend.business.shared.enums import TriggerSource
from backend.infra.integrations.agent_runner import _StageToolRegistry
from backend.infra.integrations.tool_policy import SideEffectLevel
from backend.infra.repositories import RunRepository


class WriteTool:
    name = "write_tool"
    enabled_stages = ("Run",)
    side_effect_level = SideEffectLevel.WRITE

    def is_write_call(self, arguments: object) -> bool:
        del arguments
        return True


class SourceRegistry:
    def __init__(self) -> None:
        self.tool = WriteTool()
        self.calls = 0

    def list_tools(self) -> list[object]:
        return [self.tool]

    async def call_with_abort(self, name: str, **kwargs: object) -> object:
        del name, kwargs
        self.calls += 1
        return {"order_id": "order-1", "status": "ok"}


class FailingSourceRegistry(SourceRegistry):
    async def call_with_abort(self, name: str, **kwargs: object) -> object:
        del name, kwargs
        self.calls += 1
        raise ValueError("local preflight rejected the write")


@pytest.mark.asyncio
async def test_completed_write_tool_result_is_replayed_without_side_effect(
    session,
    session_factory,
) -> None:
    run_id = 202607291
    await RunRepository(session).add(
        StrategyRun(
            run_id=run_id,
            trigger_source=TriggerSource.MANUAL,
            schedule_id=None,
            snapshot=StrategySnapshot(
                prompt_version="v1",
                risk_rules_version="risk-v1",
            ),
        )
    )
    await session.commit()
    source = SourceRegistry()
    registry = _StageToolRegistry(
        source,
        "Run",
        run_id=run_id,
        invocation_session_factory=session_factory,
    )

    context = AgentContext(
        runtime=None,
        llm_client=object(),  # type: ignore[arg-type]
        tool_registry=registry,
    )
    raw_call = {
        "id": "call-1",
        "name": "write_tool",
        "arguments": {"symbol": "600000"},
    }
    normalized_call = dict(raw_call)

    async def fail_result_persistence(_result: object) -> None:
        raise RuntimeError("trace persistence failed")

    with pytest.raises(RuntimeError, match="trace persistence failed"):
        await execute_tool_call_batch(
            context,
            registry,
            [(raw_call, normalized_call, 1)],  # type: ignore[list-item]
            iteration=1,
            max_concurrency=1,
            on_result=fail_result_persistence,  # type: ignore[arg-type]
        )

    replayed = await execute_tool_call_batch(
        context,
        registry,
        [(raw_call, normalized_call, 1)],  # type: ignore[list-item]
        iteration=1,
        max_concurrency=1,
    )

    assert replayed[0].content == {"order_id": "order-1", "status": "ok"}
    assert source.calls == 1


@pytest.mark.asyncio
async def test_failed_write_tool_result_is_finalized_without_retrying_the_write(
    session,
    session_factory,
) -> None:
    run_id = 202607292
    await RunRepository(session).add(
        StrategyRun(
            run_id=run_id,
            trigger_source=TriggerSource.MANUAL,
            schedule_id=None,
            snapshot=StrategySnapshot(
                prompt_version="v1",
                risk_rules_version="risk-v1",
            ),
        )
    )
    await session.commit()
    source = FailingSourceRegistry()
    registry = _StageToolRegistry(
        source,
        "Run",
        run_id=run_id,
        invocation_session_factory=session_factory,
    )
    context = AgentContext(
        runtime=None,
        llm_client=object(),  # type: ignore[arg-type]
        tool_registry=registry,
    )
    raw_call = {
        "id": "failed-call-1",
        "name": "write_tool",
        "arguments": {"symbol": "600000"},
    }
    normalized_call = dict(raw_call)

    first = await execute_tool_call_batch(
        context,
        registry,
        [(raw_call, normalized_call, 1)],  # type: ignore[list-item]
        iteration=1,
        max_concurrency=1,
    )
    replayed = await execute_tool_call_batch(
        context,
        registry,
        [(raw_call, normalized_call, 1)],  # type: ignore[list-item]
        iteration=1,
        max_concurrency=1,
    )

    assert first[0].status == "error"
    assert replayed[0].status == "error"
    assert replayed[0].error == "local preflight rejected the write"
    assert source.calls == 1
