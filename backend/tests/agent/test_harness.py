"""Tests for the independent AgentHarness boundary."""

from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError

import pytest

from backend.agent import (
    AgentHarness,
    AgentResult,
    CompactionCheckpoint,
    MessageAppended,
)
from backend.agent.events import AgentEventType
from backend.agent.harness import _AbortController, _CombinedAbortSignal
from backend.agent.kernel.context import AgentContext
from backend.agent.kernel.runtime_config import LlmRuntimeConfig
from backend.llm import ModelProtocol


class CapturingLoop:
    def __init__(self) -> None:
        self.context: AgentContext | None = None
        self.history_size = -1

    async def run(
        self,
        context: AgentContext,
        message: str,
        history=(),
    ) -> AgentResult:
        self.context = context
        self.history_size = len(history)
        new_messages = (
            {"role": "user", "content": message},
            {"role": "assistant", "content": "完成", "tool_calls": []},
        )
        messages = (*history, *new_messages)
        return AgentResult(
            content="完成",
            messages=messages,
            session_mutations=tuple(MessageAppended(item) for item in new_messages),
            iterations=1,
        )


def runtime() -> LlmRuntimeConfig:
    return LlmRuntimeConfig(
        protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        base_url="https://example.invalid",
        api_key="test",
        model="test-model",
    )


@pytest.mark.asyncio
async def test_harness_freezes_runtime_and_owns_session() -> None:
    client = object()
    loop = CapturingLoop()
    harness = AgentHarness(
        runtime=runtime(),
        llm_client=client,  # type: ignore[arg-type]
        system_prompt="system",
        loop=loop,  # type: ignore[arg-type]
    )

    await harness.prompt("first")
    await harness.prompt("second")

    assert loop.context is not None
    assert loop.context.llm_client is client
    assert loop.context.system_prompt == "system"
    assert loop.history_size == 2
    assert len(harness.session.messages) == 4


@pytest.mark.asyncio
async def test_harness_rejects_transcript_without_session_mutations() -> None:
    class UncommittedLoop:
        async def run(
            self,
            context: AgentContext,
            message: str,
            history=(),
        ) -> AgentResult:
            del context, message, history
            return AgentResult(
                content="completed",
                messages=({"role": "user", "content": "uncommitted"},),
            )

    harness = AgentHarness(
        runtime=runtime(),
        llm_client=object(),  # type: ignore[arg-type]
        loop=UncommittedLoop(),  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="without session mutations"):
        await harness.prompt("work")

    assert harness.session.messages == ()
    assert harness.session.journal == ()


class BlockingLoop:
    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def run(self, context: AgentContext, message: str, history=()):
        del message, history
        self.started.set()
        assert context.abort_signal is not None
        await context.abort_signal.wait()
        context.abort_signal.throw_if_aborted()
        raise AssertionError("unreachable")


class TrackingAbortSignal:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()
        self._event = asyncio.Event()

    @property
    def aborted(self) -> bool:
        return self._event.is_set()

    def throw_if_aborted(self) -> None:
        if self.aborted:
            raise asyncio.CancelledError("aborted")

    async def wait(self) -> None:
        self.started.set()
        try:
            await self._event.wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


@pytest.mark.asyncio
async def test_combined_abort_wait_cleans_child_waiters() -> None:
    external = TrackingAbortSignal()
    internal = _AbortController()
    wait_task = asyncio.create_task(_CombinedAbortSignal(internal, external).wait())

    await external.started.wait()
    internal.abort()
    await wait_task

    assert external.cancelled.is_set()


@pytest.mark.asyncio
async def test_combined_abort_wait_cleans_child_waiters_when_cancelled() -> None:
    external = TrackingAbortSignal()
    wait_task = asyncio.create_task(
        _CombinedAbortSignal(_AbortController(), external).wait()
    )

    await external.started.wait()
    wait_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await wait_task

    assert external.cancelled.is_set()


@pytest.mark.asyncio
async def test_harness_rejects_a_second_concurrent_prompt() -> None:
    loop = BlockingLoop()
    harness = AgentHarness(
        runtime=runtime(),
        llm_client=object(),  # type: ignore[arg-type]
        loop=loop,  # type: ignore[arg-type]
    )
    task = asyncio.create_task(harness.prompt("first"))
    await loop.started.wait()

    with pytest.raises(RuntimeError, match="already processing"):
        await harness.prompt("second")

    harness.abort()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_harness_abort_emits_terminal_event() -> None:
    events: list[str] = []
    loop = BlockingLoop()

    async def sink(name: str, payload: dict[str, object]) -> None:
        del payload
        events.append(name)

    harness = AgentHarness(
        runtime=runtime(),
        llm_client=object(),  # type: ignore[arg-type]
        loop=loop,  # type: ignore[arg-type]
        event_sink=sink,
    )
    task = asyncio.create_task(harness.prompt("work"))
    await loop.started.wait()
    harness.abort()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert events == ["AgentStarted", "TurnStarted", "AgentAborted"]


@pytest.mark.asyncio
async def test_harness_emits_success_lifecycle_in_order() -> None:
    events: list[str] = []

    async def sink(name: str, payload: dict[str, object]) -> None:
        del payload
        events.append(name)

    harness = AgentHarness(
        runtime=runtime(),
        llm_client=object(),  # type: ignore[arg-type]
        loop=CapturingLoop(),  # type: ignore[arg-type]
        event_sink=sink,
    )
    await harness.prompt("work")

    assert events == [
        "AgentStarted",
        "TurnStarted",
        "TurnCompleted",
        "AgentCompleted",
    ]


def test_turn_snapshot_is_immutable() -> None:
    harness = AgentHarness(
        runtime=runtime(),
        llm_client=object(),  # type: ignore[arg-type]
    )
    snapshot = harness.create_turn_snapshot()
    with pytest.raises(FrozenInstanceError):
        snapshot.label = "changed"  # type: ignore[misc]


def test_session_projects_checkpoint_without_discarding_raw_history() -> None:
    session = AgentHarness(
        runtime=runtime(),
        llm_client=object(),  # type: ignore[arg-type]
    ).session
    session.commit_turn((MessageAppended({"role": "system", "content": "system"}),))
    session.commit_turn((MessageAppended({"role": "user", "content": "old request"}),))
    session.commit_turn(
        (MessageAppended({"role": "assistant", "content": "old answer"}),)
    )
    session.commit_turn(
        (
            CompactionCheckpoint(
                summary="## Goal\ncontinue",
                retained_messages=({"role": "user", "content": "current request"},),
                original_tokens=900,
                compacted_tokens=180,
                summarized_message_count=2,
                cause="proactive",
            ),
        )
    )
    session.commit_turn(
        (MessageAppended({"role": "assistant", "content": "current answer"}),)
    )

    assert [message["role"] for message in session.messages] == [
        "system",
        "user",
        "user",
        "assistant",
    ]
    assert session.messages[2]["content"] == "current request"
    assert session.messages[3]["content"] == "current answer"
    assert any(
        isinstance(entry, MessageAppended)
        and entry.message.get("content") == "old request"
        for entry in session.journal
    )
    assert len(session.journal) == 5


def test_session_can_be_cleared() -> None:
    session = AgentHarness(
        runtime=runtime(),
        llm_client=object(),  # type: ignore[arg-type]
    ).session
    session.commit_turn((MessageAppended({"role": "user", "content": "message"}),))
    session.clear()
    assert session.messages == ()


@pytest.mark.asyncio
async def test_harness_recovers_when_start_event_sink_fails() -> None:
    calls = 0

    async def sink(name: str, payload: dict[str, object]) -> None:
        nonlocal calls
        del name, payload
        calls += 1
        if calls == 1:
            raise RuntimeError("sink unavailable")

    harness = AgentHarness(
        runtime=runtime(),
        llm_client=object(),  # type: ignore[arg-type]
        loop=CapturingLoop(),  # type: ignore[arg-type]
        event_sink=sink,
    )
    with pytest.raises(RuntimeError, match="sink unavailable"):
        await harness.prompt("first")

    result = await harness.prompt("second")
    assert result.content == "完成"


@pytest.mark.asyncio
async def test_harness_compacts_oversized_stage_handoff_before_turn() -> None:
    calls: list[dict[str, object]] = []
    events: list[tuple[str, dict[str, object]]] = []

    class SummarizingClient:
        async def generate_text(self, **kwargs) -> str:
            calls.append(dict(kwargs))
            return "## Goal\nretain critical report facts"

    async def sink(name: str, payload: dict[str, object]) -> None:
        events.append((name, payload))

    harness = AgentHarness(
        runtime=LlmRuntimeConfig(
            protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
            base_url="https://example.invalid",
            api_key="test",
            model="test-model",
            context_window_tokens=1_000,
            max_output_tokens=200,
        ),
        llm_client=SummarizingClient(),  # type: ignore[arg-type]
        system_prompt="trusted system",
        event_sink=sink,
        label="Decision",
    )
    prefix = "Review the report.\ndecision_stage_payload:\n"
    raw_payload = "原始报告资料" * 2_000

    prepared = await harness.prepare_prompt(
        prefix + raw_payload,
        preserve_prefix=prefix,
    )

    assert calls
    assert prepared.startswith(prefix + "The stage input payload")
    assert raw_payload not in prepared
    assert harness.session.messages == ()
    assert [name for name, _ in events] == [AgentEventType.CONTEXT_COMPACTED.value]
    event_payload = events[0][1]
    assert event_payload["label"] == "Decision"
    assert event_payload["scope"] == "stage_input"
    assert event_payload["summarized_message_count"] == 1
    assert event_payload["compacted_tokens"] < event_payload["original_tokens"]
