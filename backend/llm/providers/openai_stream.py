"""OpenAI Chat Completion chunk accumulation.

Derived from Pi AI's block-oriented stream state machine (MIT).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from backend.llm.contracts import (
    AssistantContent,
    AssistantMessage,
    LLMToolCall,
    ModelProtocol,
    ProviderJsonObject,
    StopReason,
    TextContent,
    ThinkingContent,
    ToolCallContent,
    Usage,
)
from backend.llm.errors import LLMConfigurationError, empty_response, invalid_response
from backend.llm.events import (
    ReasoningDelta,
    ReasoningEnded,
    ReasoningStarted,
    TextDelta,
    TextEnded,
    TextStarted,
    ToolCallDelta,
    ToolCallEnded,
    ToolCallStarted,
)
from backend.llm.providers.extract import _openai_stop_reason
from backend.llm.providers.http import _parse_tool_arguments
from backend.llm.providers.types import EventSink
from backend.llm.sanitize import sanitize_json
from backend.llm.usage import openai_usage


def _reasoning_fragments(delta: dict[str, Any]) -> tuple[str, ...]:
    for key in ("reasoning_content", "reasoning", "reasoning_text"):
        value = delta.get(key)
        if isinstance(value, str) and value:
            return (value,)
    details = delta.get("reasoning_details")
    if not isinstance(details, list):
        return ()
    fragments: list[str] = []
    for detail in details:
        if not isinstance(detail, dict):
            continue
        for key in ("text", "content", "summary"):
            value = detail.get(key)
            if isinstance(value, str) and value:
                fragments.append(value)
                break
    return tuple(fragments)


def _reasoning_detail_objects(delta: dict[str, Any]) -> tuple[ProviderJsonObject, ...]:
    raw_details = delta.get("reasoning_details")
    if raw_details is None:
        return ()
    if not isinstance(raw_details, list):
        raise invalid_response("OpenAI reasoning_details must be an array")
    details: list[ProviderJsonObject] = []
    for raw_detail in raw_details:
        try:
            detail = sanitize_json(raw_detail)
        except LLMConfigurationError as exc:
            raise invalid_response(
                "OpenAI reasoning_details contain invalid JSON", cause=exc
            ) from exc
        if not isinstance(detail, dict):
            raise invalid_response("OpenAI reasoning detail must be an object")
        details.append(detail)
    return tuple(details)


@dataclass(slots=True)
class _ToolState:
    content_index: int
    id: str = ""
    name_parts: list[str] = field(default_factory=list)
    argument_parts: list[str] = field(default_factory=list)
    started: bool = False
    ended: bool = False
    tool_call: LLMToolCall | None = None

    @property
    def name(self) -> str:
        return "".join(self.name_parts)

    @property
    def arguments(self) -> str:
        return "".join(self.argument_parts)


@dataclass(slots=True)
class OpenAIStreamAccumulator:
    protocol: ModelProtocol
    model: str
    text_parts: list[str] = field(default_factory=list)
    reasoning_parts: list[str] = field(default_factory=list)
    reasoning_details: list[ProviderJsonObject] = field(default_factory=list)
    refusal_parts: list[str] = field(default_factory=list)
    tools: dict[int, _ToolState] = field(default_factory=dict)
    order: list[tuple[Literal["text", "reasoning", "tool"], int]] = field(
        default_factory=list
    )
    text_index: int | None = None
    reasoning_index: int | None = None
    text_ended: bool = False
    reasoning_ended: bool = False
    usage: Usage = field(default_factory=Usage)
    response_id: str | None = None
    response_model: str | None = None
    finish_reason: object = None
    saw_finish_reason: bool = False

    @property
    def has_output(self) -> bool:
        return bool(self.order)

    async def accept(self, raw: dict[str, Any], emit: EventSink) -> None:
        self._accept_metadata(raw)
        choices = raw.get("choices")
        if not isinstance(choices, list) or not choices:
            return
        choice = choices[0]
        if not isinstance(choice, dict):
            raise invalid_response("OpenAI stream choice must be an object")
        if choice.get("finish_reason") is not None:
            self.finish_reason = choice["finish_reason"]
            self.saw_finish_reason = True
        delta = choice.get("delta")
        if isinstance(delta, dict):
            await self._accept_delta(delta, emit)

    def _accept_metadata(self, raw: dict[str, Any]) -> None:
        self.response_id = self.response_id or str(raw.get("id") or "").strip() or None
        chunk_model = str(raw.get("model") or "").strip()
        if chunk_model and chunk_model != self.model:
            self.response_model = self.response_model or chunk_model
        if raw.get("usage") is not None:
            self.usage = openai_usage(raw["usage"])

    async def _accept_delta(self, delta: dict[str, Any], emit: EventSink) -> None:
        self.reasoning_details.extend(_reasoning_detail_objects(delta))
        for reasoning in _reasoning_fragments(delta):
            await self._accept_reasoning(reasoning, emit)
        content = delta.get("content")
        refusal = delta.get("refusal")
        if isinstance(refusal, str) and refusal:
            self.refusal_parts.append(refusal)
        text = content if isinstance(content, str) else refusal
        if isinstance(text, str) and text:
            await self._accept_text(text, emit)
        raw_tool_calls = delta.get("tool_calls")
        if raw_tool_calls is not None:
            await self._accept_tool_calls(raw_tool_calls, emit)

    async def _accept_reasoning(self, value: str, emit: EventSink) -> None:
        if self.reasoning_index is None:
            self.reasoning_index = len(self.order)
            self.order.append(("reasoning", 0))
            await emit(ReasoningStarted(self.reasoning_index))
        self.reasoning_parts.append(value)
        await emit(ReasoningDelta(value, self.reasoning_index))

    async def _accept_text(self, value: str, emit: EventSink) -> None:
        if self.text_index is None:
            self.text_index = len(self.order)
            self.order.append(("text", 0))
            await emit(TextStarted(self.text_index))
        self.text_parts.append(value)
        await emit(TextDelta(value, self.text_index))

    async def _accept_tool_calls(self, value: object, emit: EventSink) -> None:
        if not isinstance(value, list):
            raise invalid_response("OpenAI tool deltas must be an array")
        for raw_call in value:
            await self._accept_tool_call(raw_call, emit)

    async def _accept_tool_call(self, value: object, emit: EventSink) -> None:
        if not isinstance(value, dict):
            raise invalid_response("OpenAI tool delta must be an object")
        raw_index = value.get("index", 0)
        if not isinstance(raw_index, int):
            raise invalid_response("OpenAI tool index must be an integer")
        state = self.tools.get(raw_index)
        if state is None:
            state = _ToolState(content_index=len(self.order))
            self.tools[raw_index] = state
            self.order.append(("tool", raw_index))
        raw_id = value.get("id")
        if isinstance(raw_id, str) and raw_id:
            state.id = raw_id
        function = value.get("function")
        if not isinstance(function, dict):
            return
        name = function.get("name")
        if isinstance(name, str) and name:
            state.name_parts.append(name)
        await self._start_tool_if_ready(raw_index, state, emit)
        arguments = function.get("arguments")
        if isinstance(arguments, str) and arguments:
            state.argument_parts.append(arguments)
            if state.started:
                await emit(ToolCallDelta(state.content_index, arguments))

    async def _start_tool_if_ready(
        self,
        tool_index: int,
        state: _ToolState,
        emit: EventSink,
    ) -> None:
        if state.started or not state.name:
            return
        await emit(
            ToolCallStarted(
                state.content_index,
                state.id or f"tool-call-{tool_index}",
                state.name,
            )
        )
        state.started = True
        if state.arguments:
            await emit(ToolCallDelta(state.content_index, state.arguments))

    async def finish(self, emit: EventSink) -> AssistantMessage:
        if not self.saw_finish_reason:
            raise invalid_response("OpenAI stream ended without finish_reason")
        stop_reason = _openai_stop_reason(self.finish_reason)
        blocks: list[AssistantContent] = []
        for kind, index in self.order:
            block = await self._finish_block(kind, index, emit)
            if block is not None:
                blocks.append(block)
        if not blocks:
            raise empty_response("OpenAI stream returned no content")
        if self.refusal_parts:
            refusal = "".join(self.refusal_parts).strip()
            raise invalid_response(f"OpenAI model refused the request: {refusal}")
        return AssistantMessage(
            protocol=self.protocol,
            model=self.model,
            content=tuple(blocks),
            usage=self.usage,
            stop_reason=stop_reason,
            response_id=self.response_id,
            response_model=self.response_model,
            reasoning_details=tuple(self.reasoning_details),
        )

    async def _finish_block(
        self,
        kind: Literal["text", "reasoning", "tool"],
        index: int,
        emit: EventSink,
    ) -> AssistantContent | None:
        if kind == "text":
            text = "".join(self.text_parts)
            if self.text_index is not None and not self.text_ended:
                await emit(TextEnded(text, self.text_index))
                self.text_ended = True
            return TextContent(text) if text else None
        if kind == "reasoning":
            reasoning = "".join(self.reasoning_parts)
            if self.reasoning_index is not None and not self.reasoning_ended:
                await emit(ReasoningEnded(reasoning, self.reasoning_index))
                self.reasoning_ended = True
            return ThinkingContent(reasoning) if reasoning else None
        return await self._finish_tool(index, emit)

    async def _finish_tool(self, index: int, emit: EventSink) -> ToolCallContent:
        state = self.tools[index]
        if state.ended and state.tool_call is not None:
            return ToolCallContent(
                id=state.tool_call["id"],
                name=state.tool_call["name"],
                arguments=state.tool_call["arguments"],
            )
        name = state.name.strip()
        if not name:
            raise invalid_response("OpenAI tool call name must not be empty")
        await self._start_tool_if_ready(index, state, emit)
        tool_call = LLMToolCall(
            id=state.id.strip() or f"tool-call-{index}",
            name=name,
            arguments=_parse_tool_arguments(state.arguments),
        )
        state.tool_call = tool_call
        state.ended = True
        await emit(ToolCallEnded(state.content_index, tool_call))
        return ToolCallContent(
            id=tool_call["id"],
            name=tool_call["name"],
            arguments=tool_call["arguments"],
        )

    async def failure_message(
        self,
        emit: EventSink,
        *,
        stop_reason: StopReason,
        error_message: str,
    ) -> AssistantMessage:
        """Close replayable blocks and retain all valid partial output."""

        blocks: list[AssistantContent] = []
        for kind, index in self.order:
            if kind == "text":
                text = "".join(self.text_parts)
                if self.text_index is not None and not self.text_ended:
                    await emit(TextEnded(text, self.text_index))
                    self.text_ended = True
                if text:
                    blocks.append(TextContent(text))
            elif kind == "reasoning":
                reasoning = "".join(self.reasoning_parts)
                if self.reasoning_index is not None and not self.reasoning_ended:
                    await emit(ReasoningEnded(reasoning, self.reasoning_index))
                    self.reasoning_ended = True
                if reasoning:
                    blocks.append(ThinkingContent(reasoning))
            else:
                state = self.tools[index]
                if state.tool_call is not None:
                    blocks.append(
                        ToolCallContent(
                            id=state.tool_call["id"],
                            name=state.tool_call["name"],
                            arguments=state.tool_call["arguments"],
                        )
                    )
        return AssistantMessage(
            protocol=self.protocol,
            model=self.model,
            content=tuple(blocks),
            usage=self.usage,
            stop_reason=stop_reason,
            response_id=self.response_id,
            response_model=self.response_model,
            reasoning_details=tuple(self.reasoning_details),
            error_message=error_message,
        )


__all__ = ["OpenAIStreamAccumulator"]
