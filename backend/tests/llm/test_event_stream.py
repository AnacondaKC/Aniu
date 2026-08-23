"""Tests for the bounded Pi-style assistant event stream."""

from __future__ import annotations

import asyncio

import pytest

from backend.llm import (
    AssistantMessage,
    AssistantMessageEventStream,
    Completed,
    Failed,
    ModelProtocol,
    StopReason,
    TextContent,
    TextDelta,
)


def _message(
    text: str, *, stop_reason: StopReason = StopReason.STOP
) -> AssistantMessage:
    return AssistantMessage(
        protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        model="test-model",
        content=(TextContent(text),),
        stop_reason=stop_reason,
    )


@pytest.mark.asyncio
async def test_result_can_be_awaited_without_iterating_large_stream() -> None:
    stream = AssistantMessageEventStream(max_buffered_events=2)
    expected = _message("done")

    async def produce() -> None:
        for index in range(100):
            await stream.emit(TextDelta(str(index)))
        await stream.emit(Completed(expected))

    stream.attach(asyncio.create_task(produce()))

    result = await asyncio.wait_for(stream.result(), timeout=1)

    assert result is expected


@pytest.mark.asyncio
async def test_result_returns_terminal_error_message_without_raising() -> None:
    stream = AssistantMessageEventStream(max_buffered_events=1)
    expected = _message("failed", stop_reason=StopReason.ERROR)

    async def produce() -> None:
        await stream.emit(Failed(RuntimeError("boom"), expected))

    stream.attach(asyncio.create_task(produce()))

    assert await stream.result() is expected


@pytest.mark.asyncio
async def test_breaking_iteration_cancels_attached_producer() -> None:
    stream = AssistantMessageEventStream(max_buffered_events=1)
    cancelled = asyncio.Event()

    async def produce() -> None:
        try:
            await stream.emit(TextDelta("first"))
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    stream.attach(asyncio.create_task(produce()))
    iterator = stream.__aiter__()
    assert await anext(iterator) == TextDelta("first")
    await iterator.aclose()

    await asyncio.wait_for(cancelled.wait(), timeout=1)
    result = await asyncio.wait_for(stream.result(), timeout=1)
    assert result.stop_reason is StopReason.ABORTED
    assert "closed before completion" in str(result.error_message)


@pytest.mark.asyncio
async def test_event_stream_rejects_second_consumer() -> None:
    stream = AssistantMessageEventStream(max_buffered_events=1)

    async def produce() -> None:
        await stream.emit(TextDelta("first"))
        await asyncio.Event().wait()

    stream.attach(asyncio.create_task(produce()))
    first = stream.__aiter__()
    assert await anext(first) == TextDelta("first")
    second = stream.__aiter__()
    with pytest.raises(RuntimeError, match="single consumer"):
        await anext(second)
    await first.aclose()


def test_event_stream_requires_positive_buffer_size() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        AssistantMessageEventStream(max_buffered_events=0)


@pytest.mark.asyncio
async def test_aclose_completes_result_without_an_attached_producer() -> None:
    stream = AssistantMessageEventStream(
        protocol=ModelProtocol.CLAUDE_API,
        model="claude-test",
    )

    await stream.aclose()
    result = await asyncio.wait_for(stream.result(), timeout=1)

    assert result.protocol is ModelProtocol.CLAUDE_API
    assert result.model == "claude-test"
    assert result.stop_reason is StopReason.ABORTED


@pytest.mark.asyncio
async def test_unterminated_producer_failure_becomes_terminal_event() -> None:
    stream = AssistantMessageEventStream()

    async def produce() -> None:
        raise RuntimeError("producer crashed")

    stream.attach(asyncio.create_task(produce()))
    events = [event async for event in stream]

    assert len(events) == 1
    assert isinstance(events[0], Failed)
    assert str(events[0].error) == "producer crashed"
    assert (await stream.result()).stop_reason is StopReason.ERROR
