"""Anthropic Messages event accumulation.

Derived from Pi AI's block-oriented stream state machine (MIT).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from backend.llm.contracts import (
    AssistantContent,
    AssistantMessage,
    LLMToolCall,
    ModelProtocol,
    StopReason,
    TextContent,
    ThinkingContent,
    ToolCallContent,
    Usage,
)
from backend.llm.errors import empty_response, invalid_response
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
from backend.llm.providers.extract import _claude_stop_reason
from backend.llm.providers.http import _parse_tool_arguments
from backend.llm.providers.types import EventSink
from backend.llm.usage import anthropic_usage


def _merge_usage(current: Usage, update: Usage) -> Usage:
    input_tokens = update.input or current.input
    output_tokens = update.output or current.output
    cache_read = update.cache_read or current.cache_read
    cache_write = update.cache_write or current.cache_write
    reasoning = update.reasoning if update.reasoning is not None else current.reasoning
    return Usage(
        input=input_tokens,
        output=output_tokens,
        cache_read=cache_read,
        cache_write=cache_write,
        reasoning=reasoning,
        total_tokens=input_tokens + output_tokens + cache_read + cache_write,
    )


@dataclass(slots=True)
class _BlockState:
    type: str
    text_parts: list[str] = field(default_factory=list)
    thinking_parts: list[str] = field(default_factory=list)
    signature_parts: list[str] = field(default_factory=list)
    id: str = ""
    name: str = ""
    argument_parts: list[str] = field(default_factory=list)
    initial_input: object = None
    redacted: bool = False
    tool_call: LLMToolCall | None = None
    closed: bool = False

    @property
    def text(self) -> str:
        return "".join(self.text_parts)

    @property
    def thinking(self) -> str:
        return "".join(self.thinking_parts)

    @property
    def signature(self) -> str | None:
        value = "".join(self.signature_parts)
        return value or None

    @property
    def arguments(self) -> str:
        return "".join(self.argument_parts)


@dataclass(slots=True)
class AnthropicStreamAccumulator:
    protocol: ModelProtocol
    model: str
    states: dict[int, _BlockState] = field(default_factory=dict)
    usage: Usage = field(default_factory=Usage)
    response_id: str | None = None
    response_model: str | None = None
    stop_reason_value: object = None
    saw_message_start: bool = False
    saw_stop_reason: bool = False
    saw_message_stop: bool = False

    @property
    def has_output(self) -> bool:
        return bool(self.states)

    async def accept(self, raw: dict[str, Any], emit: EventSink) -> None:
        event_type = raw.get("type")
        if event_type == "message_start":
            if self.saw_message_start:
                raise invalid_response("Claude stream repeated message_start")
            self.saw_message_start = True
            self._accept_message_start(raw)
        elif event_type == "content_block_start":
            await self._accept_block_start(raw, emit)
        elif event_type == "content_block_delta":
            await self._accept_block_delta(raw, emit)
        elif event_type == "content_block_stop":
            await self._accept_block_stop(raw, emit)
        elif event_type == "message_delta":
            self._accept_message_delta(raw)
        elif event_type == "message_stop":
            if self.saw_message_stop:
                raise invalid_response("Claude stream repeated message_stop")
            self.saw_message_stop = True

    def _accept_message_start(self, raw: dict[str, Any]) -> None:
        message = raw.get("message")
        if not isinstance(message, dict):
            raise invalid_response("Claude message_start is malformed")
        self.response_id = str(message.get("id") or "").strip() or None
        response_model = str(message.get("model") or "").strip()
        if response_model and response_model != self.model:
            self.response_model = response_model
        self.usage = _merge_usage(
            self.usage,
            anthropic_usage(message.get("usage")),
        )

    async def _accept_block_start(
        self,
        raw: dict[str, Any],
        emit: EventSink,
    ) -> None:
        index = raw.get("index")
        block = raw.get("content_block")
        if not isinstance(index, int) or not isinstance(block, dict):
            raise invalid_response("Claude content block start is malformed")
        block_type = str(block.get("type") or "")
        if index in self.states:
            raise invalid_response("Claude content block started more than once")
        if block_type not in {"text", "thinking", "redacted_thinking", "tool_use"}:
            raise invalid_response(
                f"unsupported Claude content block type: {block_type}"
            )
        initial_text = str(block.get("text") or "")
        initial_thinking = str(block.get("thinking") or "")
        initial_signature = str(block.get("signature") or block.get("data") or "")
        state = _BlockState(
            type=block_type,
            text_parts=[initial_text] if initial_text else [],
            thinking_parts=[initial_thinking] if initial_thinking else [],
            signature_parts=[initial_signature] if initial_signature else [],
            id=str(block.get("id") or ""),
            name=str(block.get("name") or ""),
            initial_input=block.get("input"),
            redacted=block_type == "redacted_thinking",
        )
        self.states[index] = state
        await self._emit_block_start(index, state, emit)

    @staticmethod
    async def _emit_block_start(
        index: int,
        state: _BlockState,
        emit: EventSink,
    ) -> None:
        if state.type == "text":
            await emit(TextStarted(index))
            if state.text:
                await emit(TextDelta(state.text, index))
        elif state.type in {"thinking", "redacted_thinking"}:
            await emit(ReasoningStarted(index))
            if state.thinking:
                await emit(ReasoningDelta(state.thinking, index))
        elif state.type == "tool_use":
            await emit(
                ToolCallStarted(
                    index,
                    state.id or f"tool-call-{index}",
                    state.name,
                )
            )

    async def _accept_block_delta(
        self,
        raw: dict[str, Any],
        emit: EventSink,
    ) -> None:
        index = raw.get("index")
        delta = raw.get("delta")
        if not isinstance(index, int) or not isinstance(delta, dict):
            raise invalid_response("Claude content block delta is malformed")
        state = self._require_block(index)
        if state.closed:
            raise invalid_response("Claude content block received delta after stop")
        delta_type = delta.get("type")
        allowed_delta_types = {
            "text": {"text_delta"},
            "thinking": {"thinking_delta", "signature_delta"},
            "redacted_thinking": {"thinking_delta", "signature_delta"},
            "tool_use": {"input_json_delta"},
        }[state.type]
        if delta_type not in allowed_delta_types:
            raise invalid_response(
                f"Claude {state.type} block received invalid delta: {delta_type}"
            )
        if delta_type == "text_delta":
            text = str(delta.get("text") or "")
            if text:
                state.text_parts.append(text)
                await emit(TextDelta(text, index))
        elif delta_type == "thinking_delta":
            thinking = str(delta.get("thinking") or "")
            if thinking:
                state.thinking_parts.append(thinking)
                await emit(ReasoningDelta(thinking, index))
        elif delta_type == "signature_delta":
            signature = str(delta.get("signature") or "")
            if signature:
                state.signature_parts.append(signature)
        elif delta_type == "input_json_delta":
            fragment = str(delta.get("partial_json") or "")
            if fragment:
                state.argument_parts.append(fragment)
                await emit(ToolCallDelta(index, fragment))

    async def _accept_block_stop(
        self,
        raw: dict[str, Any],
        emit: EventSink,
    ) -> None:
        index = raw.get("index")
        if not isinstance(index, int):
            raise invalid_response("Claude content block stop is malformed")
        state = self._require_block(index)
        if state.closed:
            raise invalid_response("Claude content block stopped more than once")
        if state.type == "text":
            await emit(TextEnded(state.text, index))
        elif state.type in {"thinking", "redacted_thinking"}:
            await emit(ReasoningEnded(state.thinking, index))
        elif state.type == "tool_use":
            state.tool_call = self._finish_tool(index, state)
            await emit(ToolCallEnded(index, state.tool_call))
        state.closed = True

    @staticmethod
    def _finish_tool(index: int, state: _BlockState) -> LLMToolCall:
        name = state.name.strip()
        if not name:
            raise invalid_response("Claude tool call name must not be empty")
        raw_arguments = state.arguments if state.arguments else state.initial_input
        return LLMToolCall(
            id=state.id.strip() or f"tool-call-{index}",
            name=name,
            arguments=_parse_tool_arguments(raw_arguments),
        )

    def _accept_message_delta(self, raw: dict[str, Any]) -> None:
        delta = raw.get("delta")
        if isinstance(delta, dict) and delta.get("stop_reason") is not None:
            self.stop_reason_value = delta["stop_reason"]
            self.saw_stop_reason = True
        self.usage = _merge_usage(self.usage, anthropic_usage(raw.get("usage")))

    def _require_block(self, index: int) -> _BlockState:
        try:
            return self.states[index]
        except KeyError as exc:
            raise invalid_response(
                "Claude event referenced an unknown content block"
            ) from exc

    def finish(self) -> AssistantMessage:
        if not self.saw_message_stop or not self.saw_stop_reason:
            raise invalid_response("Claude stream ended without a terminal stop reason")
        if any(not state.closed for state in self.states.values()):
            raise invalid_response("Claude stream ended with an open content block")
        stop_reason = _claude_stop_reason(self.stop_reason_value)
        blocks = [
            block
            for index in sorted(self.states)
            if (block := self._content_block(self.states[index])) is not None
        ]
        if not blocks:
            if stop_reason is StopReason.CONTEXT_OVERFLOW:
                return AssistantMessage(
                    protocol=self.protocol,
                    model=self.model,
                    content=(),
                    usage=self.usage,
                    stop_reason=stop_reason,
                    response_id=self.response_id,
                    response_model=self.response_model,
                )
            raise empty_response("Claude stream returned no content")
        return AssistantMessage(
            protocol=self.protocol,
            model=self.model,
            content=tuple(blocks),
            usage=self.usage,
            stop_reason=stop_reason,
            response_id=self.response_id,
            response_model=self.response_model,
        )

    async def failure_message(
        self,
        emit: EventSink,
        *,
        stop_reason: StopReason,
        error_message: str,
    ) -> AssistantMessage:
        """Close text/thinking blocks and preserve valid partial provider output."""

        for index, state in self.states.items():
            if state.closed:
                continue
            if state.type == "text":
                await emit(TextEnded(state.text, index))
                state.closed = True
            elif state.type in {"thinking", "redacted_thinking"}:
                await emit(ReasoningEnded(state.thinking, index))
                state.closed = True

        blocks: list[AssistantContent] = []
        for index in sorted(self.states):
            state = self.states[index]
            if state.type == "tool_use" and state.tool_call is None:
                continue
            block = self._content_block(state)
            if block is not None:
                blocks.append(block)
        return AssistantMessage(
            protocol=self.protocol,
            model=self.model,
            content=tuple(blocks),
            usage=self.usage,
            stop_reason=stop_reason,
            response_id=self.response_id,
            response_model=self.response_model,
            error_message=error_message,
        )

    @staticmethod
    def _content_block(state: _BlockState) -> AssistantContent | None:
        if state.type == "text" and state.text:
            return TextContent(state.text)
        if state.type in {"thinking", "redacted_thinking"} and (
            state.thinking or state.signature
        ):
            return ThinkingContent(
                thinking=state.thinking,
                signature=state.signature,
                redacted=state.redacted,
            )
        if state.type == "tool_use":
            if state.tool_call is None:
                raise invalid_response("Claude tool call did not finish")
            return ToolCallContent(
                id=state.tool_call["id"],
                name=state.tool_call["name"],
                arguments=state.tool_call["arguments"],
            )
        return None


__all__ = ["AnthropicStreamAccumulator"]
