"""Pi-style provider-neutral streaming events.

Upstream inspiration: @earendil-works/pi-ai 0.82.1 (MIT).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass

from backend.llm.contracts import (
    AssistantMessage,
    LLMToolCall,
    ModelProtocol,
    StopReason,
)


@dataclass(frozen=True, slots=True)
class StreamStarted:
    protocol: ModelProtocol
    model: str


@dataclass(frozen=True, slots=True)
class TextStarted:
    content_index: int


@dataclass(frozen=True, slots=True)
class TextDelta:
    delta: str
    content_index: int = 0


@dataclass(frozen=True, slots=True)
class TextEnded:
    content: str
    content_index: int = 0


@dataclass(frozen=True, slots=True)
class ReasoningStarted:
    content_index: int


@dataclass(frozen=True, slots=True)
class ReasoningDelta:
    delta: str
    content_index: int = 0


@dataclass(frozen=True, slots=True)
class ReasoningEnded:
    content: str
    content_index: int = 0


@dataclass(frozen=True, slots=True)
class ToolCallStarted:
    content_index: int
    tool_call_id: str
    tool_name: str


@dataclass(frozen=True, slots=True)
class ToolCallDelta:
    content_index: int
    delta: str


@dataclass(frozen=True, slots=True)
class ToolCallEnded:
    content_index: int
    tool_call: LLMToolCall


@dataclass(frozen=True, slots=True)
class Completed:
    message: AssistantMessage


@dataclass(frozen=True, slots=True)
class Failed:
    error: Exception
    message: AssistantMessage


LLMEvent = (
    StreamStarted
    | TextStarted
    | TextDelta
    | TextEnded
    | ReasoningStarted
    | ReasoningDelta
    | ReasoningEnded
    | ToolCallStarted
    | ToolCallDelta
    | ToolCallEnded
    | Completed
    | Failed
)

_TERMINAL = Completed | Failed


class LLMStreamClosedError(RuntimeError):
    """Raised when a consumer closes a stream before a terminal provider event."""


class AssistantMessageEventStream:
    """Bounded single-consumer stream with a total final-result contract."""

    def __init__(
        self,
        *,
        protocol: ModelProtocol = ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        model: str = "unknown",
        max_buffered_events: int = 32,
    ) -> None:
        if max_buffered_events <= 0:
            raise ValueError("max_buffered_events must be positive")
        self._protocol = protocol
        self._model = model
        self._queue: asyncio.Queue[LLMEvent] = asyncio.Queue(
            maxsize=max_buffered_events
        )
        self._result: asyncio.Future[AssistantMessage] = (
            asyncio.get_running_loop().create_future()
        )
        self._closed = False
        self._producer: asyncio.Task[None] | None = None
        self._consumer_active = False
        self._terminal_consumed = False

    def attach(self, producer: asyncio.Task[None]) -> None:
        if self._closed:
            producer.cancel()
            raise RuntimeError("cannot attach producer to a closed event stream")
        if self._producer is not None:
            raise RuntimeError("event stream already has a producer")
        self._producer = producer
        producer.add_done_callback(self._producer_done)

    async def emit(self, event: LLMEvent) -> None:
        if self._closed:
            return
        await self._queue.put(event)
        if isinstance(event, _TERMINAL):
            self._complete(event)

    async def result(self) -> AssistantMessage:
        if not self._result.done() and not self._consumer_active:
            async for _event in self:
                pass
        return await asyncio.shield(self._result)

    async def aclose(self) -> None:
        if not self._result.done():
            error = LLMStreamClosedError("LLM event stream closed before completion")
            message = self._terminal_message(
                StopReason.ABORTED,
                str(error),
            )
            self._force_terminal(Failed(error, message))

        producer = self._producer
        if producer is not None and not producer.done():
            producer.cancel()
            try:
                await producer
            except asyncio.CancelledError:
                pass

    async def __aiter__(self) -> AsyncIterator[LLMEvent]:
        if self._consumer_active or self._terminal_consumed:
            raise RuntimeError("LLM event stream supports a single consumer")
        self._consumer_active = True
        try:
            while True:
                event = await self._queue.get()
                yield event
                if isinstance(event, _TERMINAL):
                    self._terminal_consumed = True
                    return
        finally:
            self._consumer_active = False
            if not self._closed:
                await self.aclose()

    def _complete(self, event: Completed | Failed) -> None:
        self._closed = True
        if not self._result.done():
            self._result.set_result(event.message)

    def _force_terminal(self, event: Completed | Failed) -> None:
        self._closed = True
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        self._queue.put_nowait(event)
        if not self._result.done():
            self._result.set_result(event.message)

    def _producer_done(self, producer: asyncio.Task[None]) -> None:
        if self._result.done():
            return
        asyncio.create_task(self._finalize_unterminated_producer(producer))

    async def _finalize_unterminated_producer(
        self, producer: asyncio.Task[None]
    ) -> None:
        if self._result.done():
            return
        if producer.cancelled():
            error: Exception = LLMStreamClosedError(
                "LLM event stream producer was cancelled"
            )
            stop_reason = StopReason.ABORTED
        else:
            producer_error = producer.exception()
            if isinstance(producer_error, Exception):
                error = producer_error
            else:
                error = RuntimeError(
                    "LLM event stream producer ended without terminal event"
                )
            stop_reason = StopReason.ERROR
        message = self._terminal_message(
            stop_reason, str(error) or type(error).__name__
        )
        self._force_terminal(Failed(error, message))

    def _terminal_message(
        self, stop_reason: StopReason, error_message: str
    ) -> AssistantMessage:
        return AssistantMessage(
            protocol=self._protocol,
            model=self._model,
            content=(),
            stop_reason=stop_reason,
            error_message=error_message,
        )


__all__ = [
    "AssistantMessageEventStream",
    "Completed",
    "Failed",
    "LLMEvent",
    "LLMStreamClosedError",
    "ReasoningDelta",
    "ReasoningEnded",
    "ReasoningStarted",
    "StreamStarted",
    "TextDelta",
    "TextEnded",
    "TextStarted",
    "ToolCallDelta",
    "ToolCallEnded",
    "ToolCallStarted",
]
