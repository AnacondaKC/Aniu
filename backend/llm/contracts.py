"""Provider-neutral contracts derived from Pi AI's message model.

Upstream inspiration: @earendil-works/pi-ai 0.82.1 (MIT).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal, NotRequired, TypedDict

from backend.llm.errors import LLMConfigurationError
from backend.llm.sanitize import sanitize_json

type ProviderJsonValue = (
    None
    | bool
    | int
    | float
    | str
    | list[ProviderJsonValue]
    | dict[str, ProviderJsonValue]
)
type ProviderJsonObject = dict[str, ProviderJsonValue]


class ModelProtocol(StrEnum):
    """Supported model provider protocols.

    Deliberately only two: OpenAI Chat Completions (the de-facto relay
    standard) and the Claude API. Responses/Gemini support was removed —
    maintaining four protocol dialects cost more than it returned.
    """

    OPENAI_CHAT_COMPLETIONS = "openai_chat_completions"
    CLAUDE_API = "claude_api"


class StopReason(StrEnum):
    PENDING = "pending"
    STOP = "stop"
    LENGTH = "length"
    CONTEXT_OVERFLOW = "context_overflow"
    TOOL_USE = "tool_use"
    ERROR = "error"
    ABORTED = "aborted"


@dataclass(frozen=True, slots=True)
class Usage:
    input: int = 0
    output: int = 0
    cache_read: int = 0
    cache_write: int = 0
    reasoning: int | None = None
    total_tokens: int = 0


class LLMToolCall(TypedDict):
    id: str
    name: str
    arguments: ProviderJsonObject


class LLMToolDefinition(TypedDict):
    name: str
    description: str
    parameters: ProviderJsonObject


class SystemMessage(TypedDict):
    role: Literal["system"]
    content: str


class UserMessage(TypedDict):
    role: Literal["user"]
    content: str


class AssistantInputMessage(TypedDict):
    role: Literal["assistant"]
    content: str | None
    tool_calls: NotRequired[list[LLMToolCall]]
    reasoning: NotRequired[str | None]
    reasoning_details: NotRequired[tuple[ProviderJsonObject, ...]]
    content_blocks: NotRequired[tuple[AssistantContent, ...]]


class ToolResultMessage(TypedDict):
    role: Literal["tool"]
    content: str
    tool_call_id: str
    name: str
    is_error: NotRequired[bool]


LLMChatMessage = SystemMessage | UserMessage | AssistantInputMessage | ToolResultMessage
ChatMessage = LLMChatMessage
ToolCall = LLMToolCall
ToolDefinition = LLMToolDefinition


@dataclass(frozen=True, slots=True)
class TextContent:
    text: str
    type: Literal["text"] = field(default="text", init=False)


@dataclass(frozen=True, slots=True)
class ThinkingContent:
    thinking: str
    signature: str | None = None
    redacted: bool = False
    type: Literal["thinking"] = field(default="thinking", init=False)


@dataclass(frozen=True, slots=True)
class ToolCallContent:
    id: str
    name: str
    arguments: ProviderJsonObject
    type: Literal["tool_call"] = field(default="tool_call", init=False)


AssistantContent = TextContent | ThinkingContent | ToolCallContent


@dataclass(frozen=True, slots=True)
class AssistantMessage:
    protocol: ModelProtocol
    model: str
    content: tuple[AssistantContent, ...]
    usage: Usage = field(default_factory=Usage)
    stop_reason: StopReason = StopReason.STOP
    timestamp: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    response_id: str | None = None
    response_model: str | None = None
    reasoning_details: tuple[ProviderJsonObject, ...] = ()
    error_message: str | None = None

    @property
    def text(self) -> str | None:
        parts = [block.text for block in self.content if isinstance(block, TextContent)]
        text = "\n".join(part for part in parts if part).strip()
        return text or None

    @property
    def reasoning(self) -> str | None:
        parts = [
            block.thinking
            for block in self.content
            if isinstance(block, ThinkingContent)
        ]
        text = "\n".join(part for part in parts if part).strip()
        return text or None

    @property
    def tool_calls(self) -> list[LLMToolCall]:
        return [
            LLMToolCall(id=block.id, name=block.name, arguments=block.arguments)
            for block in self.content
            if isinstance(block, ToolCallContent)
        ]


class LLMChatResponse(TypedDict):
    content: str | None
    tool_calls: list[LLMToolCall]
    reasoning: NotRequired[str | None]
    reasoning_details: NotRequired[tuple[ProviderJsonObject, ...]]
    usage: NotRequired[Usage]
    stop_reason: NotRequired[StopReason]
    response_id: NotRequired[str | None]
    response_model: NotRequired[str | None]
    assistant_message: NotRequired[AssistantMessage]


ChatResponse = LLMChatResponse


def assistant_input_from_message(message: AssistantMessage) -> AssistantInputMessage:
    """Create a replayable assistant turn without discarding content blocks."""

    return AssistantInputMessage(
        role="assistant",
        content=message.text,
        tool_calls=message.tool_calls,
        reasoning=message.reasoning,
        reasoning_details=message.reasoning_details,
        content_blocks=message.content,
    )


def chat_response_from_assistant(message: AssistantMessage) -> ChatResponse:
    return ChatResponse(
        content=message.text,
        tool_calls=message.tool_calls,
        reasoning=message.reasoning,
        reasoning_details=message.reasoning_details,
        usage=message.usage,
        stop_reason=message.stop_reason,
        response_id=message.response_id,
        response_model=message.response_model,
        assistant_message=message,
    )


def _normalize_tool_calls(value: object) -> list[ToolCall]:
    if not isinstance(value, list):
        raise ValueError("chat response tool_calls must be a list")
    tool_calls: list[ToolCall] = []
    for raw_call in value:
        if not isinstance(raw_call, dict):
            raise ValueError("chat response tool call must be an object")
        call_id = raw_call.get("id")
        name = raw_call.get("name")
        arguments = raw_call.get("arguments")
        if not isinstance(call_id, str) or not call_id.strip():
            raise ValueError("chat response tool call id must be non-empty text")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("chat response tool call name must be non-empty text")
        if not isinstance(arguments, dict):
            raise ValueError("chat response tool call arguments must be an object")
        try:
            sanitized = sanitize_json(arguments)
        except LLMConfigurationError as exc:
            raise ValueError(
                "chat response tool call arguments must contain valid JSON"
            ) from exc
        if not isinstance(sanitized, dict):
            raise ValueError("chat response tool call arguments must be an object")
        tool_calls.append(
            ToolCall(id=call_id.strip(), name=name.strip(), arguments=sanitized)
        )
    return tool_calls


def _optional_text(value: object, field_name: str) -> str | None:
    if value is not None and not isinstance(value, str):
        raise ValueError(f"chat response {field_name} must be text or null")
    return value.strip() if isinstance(value, str) else None


def _add_response_metadata(
    response: ChatResponse,
    value: dict[object, object],
) -> None:
    usage = value.get("usage")
    if usage is not None:
        if not isinstance(usage, Usage):
            raise ValueError("chat response usage must be Usage")
        response["usage"] = usage
    stop_reason = value.get("stop_reason")
    if stop_reason is not None:
        if not isinstance(stop_reason, str):
            raise ValueError("chat response stop_reason must be text")
        response["stop_reason"] = StopReason(stop_reason)
    response_id = _optional_text(value.get("response_id"), "id")
    if response_id is not None:
        response["response_id"] = response_id
    response_model = _optional_text(value.get("response_model"), "model")
    if response_model is not None:
        response["response_model"] = response_model


def normalize_chat_response(value: object) -> ChatResponse:
    """Validate a dynamically supplied client response at the LLM boundary."""

    if isinstance(value, AssistantMessage):
        return chat_response_from_assistant(value)
    if not isinstance(value, dict):
        raise ValueError("chat response must be an object")
    assistant_message = value.get("assistant_message")
    if assistant_message is not None:
        if not isinstance(assistant_message, AssistantMessage):
            raise ValueError("chat response assistant_message must be AssistantMessage")
        return chat_response_from_assistant(assistant_message)
    content = _optional_text(value.get("content"), "content")
    normalized_content = content or None
    tool_calls = _normalize_tool_calls(value.get("tool_calls"))
    raw_stop_reason = value.get("stop_reason")
    context_overflow = (
        isinstance(raw_stop_reason, str)
        and raw_stop_reason == StopReason.CONTEXT_OVERFLOW.value
    )
    if normalized_content is None and not tool_calls and not context_overflow:
        raise ValueError("chat response must contain text or a tool call")

    response = ChatResponse(content=normalized_content, tool_calls=tool_calls)
    if "reasoning" in value:
        response["reasoning"] = _optional_text(value.get("reasoning"), "reasoning")
    if "reasoning_details" in value:
        raw_details = value.get("reasoning_details")
        if not isinstance(raw_details, (list, tuple)):
            raise ValueError("chat response reasoning_details must be an array")
        details: list[ProviderJsonObject] = []
        for raw_detail in raw_details:
            try:
                detail = sanitize_json(raw_detail)
            except LLMConfigurationError as exc:
                raise ValueError(
                    "chat response reasoning_details must contain valid JSON objects"
                ) from exc
            if not isinstance(detail, dict):
                raise ValueError("chat response reasoning_details must contain objects")
            details.append(detail)
        response["reasoning_details"] = tuple(details)
    _add_response_metadata(response, value)
    return response


@dataclass(frozen=True, slots=True)
class ModelCatalogItem:
    model: str
    label: str
    provider_id: str | None = None
