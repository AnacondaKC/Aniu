"""Regression tests for the generic AgentLoop and tool scheduler."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest

from backend.agent.actors.agent_loop import AgentLoop
from backend.agent.actors.tool_executor import execute_tool_call_batch
from backend.agent.contracts import CompactionCheckpoint
from backend.agent.errors import AgentErrorCode, AgentIntegrationError
from backend.agent.events import AgentEventType
from backend.agent.kernel.context import AgentContext
from backend.agent.kernel.runtime_config import LlmRuntimeConfig
from backend.agent.session import AgentSession
from backend.llm import ModelProtocol


@dataclass
class AbortSignal:
    aborted: bool = False
    event: asyncio.Event = field(default_factory=asyncio.Event)

    def abort(self) -> None:
        self.aborted = True
        self.event.set()

    def throw_if_aborted(self) -> None:
        if self.aborted:
            raise asyncio.CancelledError

    async def wait(self) -> None:
        await self.event.wait()


class Tool:
    def __init__(self, name: str, *, execution_mode: str = "parallel") -> None:
        self.name = name
        self.execution_mode = execution_mode

    def to_tool_definition(self):
        return {
            "name": self.name,
            "description": f"tool {self.name}",
            "parameters": {"type": "object", "properties": {}},
        }

    async def run(self, **kwargs):
        return {"tool": self.name, "arguments": kwargs}


class Registry:
    def __init__(self, tools: list[Tool] | None = None) -> None:
        self.tools = [Tool("search")] if tools is None else tools
        self.calls: list[str] = []

    def list_tools(self):
        return self.tools

    def get(self, name: str):
        return next(tool for tool in self.tools if tool.name == name)

    async def call(self, name: str, **kwargs):
        self.calls.append(name)
        return await self.get(name).run(**kwargs)


def runtime(*, parallel: int = 8) -> LlmRuntimeConfig:
    return LlmRuntimeConfig(
        protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        base_url="https://example.invalid",
        api_key="test",
        model="test-model",
        max_parallel_tool_calls=parallel,
        context_window_tokens=2_000,
        max_output_tokens=200,
    )


def context(
    *,
    registry: Registry | None = None,
    signal: AbortSignal | None = None,
) -> AgentContext:
    return AgentContext(
        runtime=runtime(),
        llm_client=object(),  # type: ignore[arg-type]
        system_prompt="generic system",
        tool_registry=registry or Registry(),
        abort_signal=signal,
    )


@pytest.mark.asyncio
async def test_loop_executes_tools_and_returns_complete_transcript(monkeypatch) -> None:
    responses = iter(
        [
            {
                "content": None,
                "tool_calls": [
                    {"id": "c1", "name": "search", "arguments": {"q": "market"}}
                ],
            },
            {"content": "final report", "tool_calls": []},
        ]
    )

    async def fake_response(*args, **kwargs):
        del args, kwargs
        return next(responses)

    monkeypatch.setattr(
        "backend.agent.actors.agent_loop.generate_tool_loop_response",
        fake_response,
    )
    registry = Registry()
    events: list[tuple[str, dict[str, object]]] = []

    async def event_sink(name: str, payload: dict[str, object]) -> None:
        events.append((name, dict(payload)))

    ctx = context(registry=registry)
    ctx.event_sink = event_sink
    result = await AgentLoop().run(ctx, "analyze")

    tool_message = next(
        message for message in result.messages if message["role"] == "tool"
    )
    assert isinstance(tool_message["content"], str)
    completed_payload = next(
        payload
        for name, payload in events
        if name == AgentEventType.TOOL_CALL_COMPLETED.value
    )

    assert result.content == "final report"
    assert registry.calls == ["search"]
    assert result.tool_activity[0]["status"] == "ok"
    assert completed_payload["model_content_characters"] == len(tool_message["content"])
    assert tool_message["content"] == '{"tool":"search"}'
    assert [message["role"] for message in result.messages] == [
        "system",
        "user",
        "assistant",
        "tool",
        "assistant",
    ]


@pytest.mark.asyncio
async def test_loop_uses_tool_trace_arguments_without_mutating_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_arguments: dict[str, object] = {}

    class PresentedTool(Tool):
        def trace_arguments(self, arguments: object) -> dict[str, object]:
            values = dict(arguments) if isinstance(arguments, dict) else {}
            values["interface_name"] = "可信接口名称"
            return values

        async def run(self, **kwargs: object) -> object:
            seen_arguments.update(kwargs)
            return {"ok": True}

    responses = iter(
        [
            {
                "content": None,
                "tool_calls": [
                    {
                        "id": "c1",
                        "name": "lookup",
                        "arguments": {"interface_ref": "if_reference"},
                    }
                ],
            },
            {"content": "done", "tool_calls": []},
        ]
    )

    async def fake_response(*args, **kwargs):
        del args, kwargs
        return next(responses)

    monkeypatch.setattr(
        "backend.agent.actors.agent_loop.generate_tool_loop_response",
        fake_response,
    )
    registry = Registry([PresentedTool("lookup")])
    events: list[tuple[str, dict[str, object]]] = []

    async def event_sink(name: str, payload: dict[str, object]) -> None:
        events.append((name, dict(payload)))

    ctx = context(registry=registry)
    ctx.event_sink = event_sink
    result = await AgentLoop().run(ctx, "analyze")

    requested = next(
        payload
        for name, payload in events
        if name == AgentEventType.TOOL_CALL_REQUESTED.value
    )
    completed = next(
        payload
        for name, payload in events
        if name == AgentEventType.TOOL_CALL_COMPLETED.value
    )
    expected_trace_arguments = {
        "interface_ref": "if_reference",
        "interface_name": "可信接口名称",
    }
    assert requested["arguments"] == expected_trace_arguments
    assert completed["arguments"] == expected_trace_arguments
    assert result.tool_activity[0]["arguments"] == expected_trace_arguments
    assert seen_arguments == {"interface_ref": "if_reference"}


@pytest.mark.asyncio
async def test_tools_in_one_batch_execute_in_parallel() -> None:
    active = 0
    max_active = 0
    both_started = asyncio.Event()

    class ParallelRegistry(Registry):
        async def call(self, name: str, **kwargs):
            nonlocal active, max_active
            del name, kwargs
            active += 1
            max_active = max(max_active, active)
            if active == 2:
                both_started.set()
            await asyncio.wait_for(both_started.wait(), timeout=1)
            active -= 1
            return {"ok": True}

    registry = ParallelRegistry([Tool("a"), Tool("b")])
    calls = [
        (
            {"id": "1", "name": "a", "arguments": {}},
            {"id": "1", "name": "a", "arguments": {}},
            1,
        ),
        (
            {"id": "2", "name": "b", "arguments": {}},
            {"id": "2", "name": "b", "arguments": {}},
            2,
        ),
    ]
    results = await execute_tool_call_batch(
        context(registry=registry),
        registry,
        calls,  # type: ignore[arg-type]
        iteration=1,
        max_concurrency=8,
    )

    assert max_active == 2
    assert [result.status for result in results] == ["ok", "ok"]


@pytest.mark.asyncio
async def test_sequential_tool_makes_whole_batch_serial() -> None:
    active = 0
    max_active = 0

    class SequentialRegistry(Registry):
        async def call(self, name: str, **kwargs):
            nonlocal active, max_active
            del name, kwargs
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1
            return {"ok": True}

    registry = SequentialRegistry(
        [Tool("read"), Tool("write", execution_mode="sequential")]
    )
    calls = [
        (
            {"id": "1", "name": "read", "arguments": {}},
            {"id": "1", "name": "read", "arguments": {}},
            1,
        ),
        (
            {"id": "2", "name": "write", "arguments": {}},
            {"id": "2", "name": "write", "arguments": {}},
            2,
        ),
    ]
    await execute_tool_call_batch(
        context(registry=registry),
        registry,
        calls,  # type: ignore[arg-type]
        iteration=1,
        max_concurrency=8,
    )
    assert max_active == 1


@pytest.mark.asyncio
async def test_cancelled_queue_does_not_start_remaining_tools() -> None:
    signal = AbortSignal()

    class CancellingRegistry(Registry):
        async def call(self, name: str, **kwargs):
            del kwargs
            self.calls.append(name)
            signal.abort()
            return {"ok": True}

    registry = CancellingRegistry([Tool("a", execution_mode="sequential"), Tool("b")])
    calls = [
        (
            {"id": "1", "name": "a", "arguments": {}},
            {"id": "1", "name": "a", "arguments": {}},
            1,
        ),
        (
            {"id": "2", "name": "b", "arguments": {}},
            {"id": "2", "name": "b", "arguments": {}},
            2,
        ),
    ]
    results = await execute_tool_call_batch(
        context(registry=registry, signal=signal),
        registry,
        calls,  # type: ignore[arg-type]
        iteration=1,
        max_concurrency=1,
    )

    assert registry.calls == ["a"]
    assert results[1].is_error


@pytest.mark.asyncio
async def test_context_overflow_forces_compaction_and_retries(monkeypatch) -> None:
    calls = 0
    events: list[str] = []

    async def fake_response(*args, **kwargs):
        nonlocal calls
        del args, kwargs
        calls += 1
        if calls == 1:
            return {
                "content": None,
                "tool_calls": [{"id": "c1", "name": "search", "arguments": {}}],
            }
        if calls == 2:
            raise AgentIntegrationError(
                "request_too_large",
                error_code=AgentErrorCode.CONTEXT_OVERFLOW,
            )
        return {"content": "done", "tool_calls": []}

    async def fake_summary(*args, **kwargs):
        del args, kwargs
        return "## Goal\ncontinue"

    async def event_sink(name: str, payload: dict[str, object]) -> None:
        del payload
        events.append(name)

    monkeypatch.setattr(
        "backend.agent.actors.agent_loop.generate_tool_loop_response", fake_response
    )
    monkeypatch.setattr(
        "backend.agent.actors.agent_loop.generate_text_output", fake_summary
    )
    ctx = context()
    ctx.event_sink = event_sink
    result = await AgentLoop().run(ctx, "analyze")

    assert result.content == "done"
    assert calls == 3
    assert events.count(AgentEventType.CONTEXT_COMPACTED.value) == 1
    assert any(
        isinstance(mutation, CompactionCheckpoint)
        and mutation.cause == "provider_overflow"
        for mutation in result.session_mutations
    )
    session = AgentSession()
    session.commit_turn(result.session_mutations)
    assert session.messages == result.messages


@pytest.mark.asyncio
async def test_loop_continues_past_legacy_round_limit(monkeypatch) -> None:
    calls = 0

    async def fake_response(*args, **kwargs):
        nonlocal calls
        del args, kwargs
        calls += 1
        if calls <= 21:
            return {
                "content": None,
                "tool_calls": [
                    {"id": f"c{calls}", "name": "search", "arguments": {}}
                ],
            }
        return {"content": "done", "tool_calls": []}

    monkeypatch.setattr(
        "backend.agent.actors.agent_loop.generate_tool_loop_response", fake_response
    )
    registry = Registry()

    result = await AgentLoop().run(context(registry=registry), "analyze")

    assert result.content == "done"
    assert result.iterations == 22
    assert registry.calls == ["search"] * 21


def tool_call(name: str, sequence: int):
    call = {"id": str(sequence), "name": name, "arguments": {}}
    return call, call, sequence


@pytest.mark.asyncio
async def test_unavailable_tool_is_blocked_without_execution() -> None:
    registry = Registry([])
    results = await execute_tool_call_batch(
        context(registry=registry),
        registry,
        [tool_call("missing", 1)],  # type: ignore[list-item]
        iteration=1,
        max_concurrency=1,
    )
    assert results[0].status == "blocked"
    assert registry.calls == []


@pytest.mark.asyncio
async def test_tool_authorizer_can_block_a_registered_tool() -> None:
    registry = Registry([Tool("write")])
    ctx = context(registry=registry)
    ctx.tool_authorizer = lambda tool, arguments: "blocked by host policy"
    results = await execute_tool_call_batch(
        ctx,
        registry,
        [tool_call("write", 1)],  # type: ignore[list-item]
        iteration=1,
        max_concurrency=1,
    )
    assert results[0].status == "blocked"
    assert results[0].error == "blocked by host policy"
    assert registry.calls == []


@pytest.mark.asyncio
async def test_completion_callback_order_does_not_change_result_order() -> None:
    class DelayedRegistry(Registry):
        async def call(self, name: str, **kwargs):
            del kwargs
            await asyncio.sleep(0.03 if name == "slow" else 0.001)
            return name

    registry = DelayedRegistry([Tool("slow"), Tool("fast")])
    completed: list[str] = []

    async def on_result(result) -> None:
        completed.append(result.tool_name)

    results = await execute_tool_call_batch(
        context(registry=registry),
        registry,
        [
            tool_call("slow", 1),
            tool_call("fast", 2),
        ],  # type: ignore[list-item]
        iteration=1,
        max_concurrency=2,
        on_result=on_result,
    )
    assert completed == ["fast", "slow"]
    assert [result.tool_name for result in results] == ["slow", "fast"]


@pytest.mark.asyncio
async def test_parallel_completion_callbacks_are_serialized() -> None:
    registry = Registry([Tool("a"), Tool("b")])
    active = 0
    maximum = 0

    async def on_result(_result) -> None:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.01)
        active -= 1

    await execute_tool_call_batch(
        context(registry=registry),
        registry,
        [tool_call("a", 1), tool_call("b", 2)],  # type: ignore[list-item]
        iteration=1,
        max_concurrency=2,
        on_result=on_result,
    )

    assert maximum == 1


@pytest.mark.asyncio
async def test_parallel_scheduler_respects_concurrency_limit() -> None:
    active = 0
    maximum = 0

    class LimitedRegistry(Registry):
        async def call(self, name: str, **kwargs):
            nonlocal active, maximum
            del name, kwargs
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0.01)
            active -= 1
            return {"ok": True}

    tools = [Tool(str(index)) for index in range(5)]
    registry = LimitedRegistry(tools)
    await execute_tool_call_batch(
        context(registry=registry),
        registry,
        [tool_call(str(index), index) for index in range(5)],  # type: ignore[list-item]
        iteration=1,
        max_concurrency=2,
    )
    assert maximum == 2


@pytest.mark.asyncio
async def test_loop_without_tools_does_not_invent_system_protocol(monkeypatch) -> None:
    captured_messages = []

    async def fake_response(context, **kwargs):
        del context
        captured_messages.extend(kwargs["messages"])
        return {"content": "done", "tool_calls": []}

    monkeypatch.setattr(
        "backend.agent.actors.agent_loop.generate_tool_loop_response", fake_response
    )
    ctx = context(registry=Registry([]))
    ctx.system_prompt = ""
    result = await AgentLoop().run(ctx, "hello")

    assert result.content == "done"
    assert captured_messages[0]["role"] == "user"


@pytest.mark.asyncio
async def test_tool_error_payload_becomes_error_result() -> None:
    class ErrorRegistry(Registry):
        async def call(self, name: str, **kwargs):
            del name, kwargs
            return {"status": "error", "error": "failed"}

    registry = ErrorRegistry([Tool("failing")])
    results = await execute_tool_call_batch(
        context(registry=registry),
        registry,
        [tool_call("failing", 1)],  # type: ignore[list-item]
        iteration=1,
        max_concurrency=1,
    )
    assert results[0].status == "error"
    assert results[0].error == "failed"
