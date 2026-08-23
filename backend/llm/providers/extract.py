"""Non-streaming response parsing and tool schema conversion."""

from __future__ import annotations

from typing import Any

from backend.llm.contracts import (
    ChatResponse,
    LLMChatResponse,
    LLMToolCall,
    LLMToolDefinition,
    StopReason,
)
from backend.llm.errors import (
    LLMErrorCode,
    LLMIntegrationError,
    empty_response,
    invalid_response,
)
from backend.llm.providers.http import _coerce_text, _parse_tool_arguments
from backend.llm.sanitize import sanitize_json, sanitize_unicode
from backend.llm.usage import anthropic_usage, openai_usage


def _mapping(value: object, message: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise invalid_response(message)
    return value


def _openai_stop_reason(value: object) -> StopReason:
    if value in {"stop", "end"}:
        return StopReason.STOP
    if value == "tool_calls":
        return StopReason.TOOL_USE
    if value == "length":
        return StopReason.LENGTH
    if value == "content_filter":
        raise invalid_response("llm output was blocked by the provider content filter")
    raise invalid_response(f"unsupported provider finish_reason: {value}")


def _claude_stop_reason(value: object) -> StopReason:
    if value in {"end_turn", "stop_sequence"}:
        return StopReason.STOP
    if value == "tool_use":
        return StopReason.TOOL_USE
    if value == "max_tokens":
        return StopReason.LENGTH
    if value == "model_context_window_exceeded":
        return StopReason.CONTEXT_OVERFLOW
    if value in {"refusal", "sensitive", "pause_turn"}:
        raise invalid_response(f"unsupported Claude stop_reason: {value}")
    raise invalid_response(f"unsupported Claude stop_reason: {value}")


def _extract_openai_chat_tool_calls(message: dict[str, Any]) -> list[LLMToolCall]:
    raw_calls = message.get("tool_calls")
    if raw_calls is None:
        return []
    if not isinstance(raw_calls, list):
        raise invalid_response("llm tool_calls must be an array")
    calls: list[LLMToolCall] = []
    for index, raw_call in enumerate(raw_calls):
        call = _mapping(raw_call, "llm tool call must be an object")
        function = _mapping(call.get("function"), "llm tool function must be an object")
        name = str(function.get("name") or "").strip()
        if not name:
            raise invalid_response("llm tool call name must not be empty")
        call_id = str(call.get("id") or "").strip() or f"tool-call-{index}"
        calls.append(
            LLMToolCall(
                id=call_id,
                name=name,
                arguments=_parse_tool_arguments(function.get("arguments")),
            )
        )
    return calls


def _extract_openai_chat_result(payload: dict[str, Any]) -> LLMChatResponse:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise invalid_response("llm response did not include choices")
    choice = _mapping(choices[0], "llm choice must be an object")
    finish_reason = choice.get("finish_reason")
    if finish_reason is None:
        raise invalid_response("llm response did not include finish_reason")
    stop_reason = _openai_stop_reason(finish_reason)
    message = _mapping(choice.get("message"), "llm response did not include a message")
    content = _coerce_text(message.get("content")) or None
    tool_calls = _extract_openai_chat_tool_calls(message)
    if content is None and not tool_calls:
        raise empty_response("llm returned empty chat content")
    reasoning = (
        _coerce_text(
            message.get("reasoning_content")
            or message.get("reasoning")
            or message.get("reasoning_text")
        )
        or None
    )
    reasoning_details: tuple[dict[str, Any], ...] = ()
    raw_reasoning_details = message.get("reasoning_details")
    if raw_reasoning_details is not None:
        if not isinstance(raw_reasoning_details, list):
            raise invalid_response("llm reasoning_details must be an array")
        reasoning_details = tuple(
            _parse_tool_arguments(detail) for detail in raw_reasoning_details
        )
    return ChatResponse(
        content=content,
        tool_calls=tool_calls,
        reasoning=reasoning,
        reasoning_details=reasoning_details,
        usage=openai_usage(payload.get("usage")),
        stop_reason=stop_reason,
        response_id=str(payload.get("id") or "").strip() or None,
        response_model=str(payload.get("model") or "").strip() or None,
    )


def _extract_openai_chat_text(payload: dict[str, Any]) -> str:
    result = _extract_openai_chat_result(payload)
    content = result["content"]
    if content is None:
        raise empty_response("llm returned empty chat content")
    return content


def _extract_claude_tool_calls(content: list[object]) -> list[LLMToolCall]:
    calls: list[LLMToolCall] = []
    for index, item in enumerate(content):
        if not isinstance(item, dict) or item.get("type") != "tool_use":
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            raise invalid_response("Claude tool call name must not be empty")
        calls.append(
            LLMToolCall(
                id=str(item.get("id") or "").strip() or f"tool-call-{index}",
                name=name,
                arguments=_parse_tool_arguments(item.get("input")),
            )
        )
    return calls


def _extract_claude_result(payload: dict[str, Any]) -> LLMChatResponse:
    raw_content = payload.get("content")
    if not isinstance(raw_content, list):
        raise invalid_response("llm response did not include Claude content")
    stop_reason_value = payload.get("stop_reason")
    if stop_reason_value is None:
        raise invalid_response("llm response did not include Claude stop_reason")
    stop_reason = _claude_stop_reason(stop_reason_value)
    text_parts: list[str] = []
    thinking_parts: list[str] = []
    for item in raw_content:
        if not isinstance(item, dict):
            raise invalid_response("Claude content block must be an object")
        block_type = item.get("type")
        if block_type == "text":
            text = _coerce_text(item.get("text"))
            if text:
                text_parts.append(text)
        elif block_type == "thinking":
            thinking = _coerce_text(item.get("thinking"))
            if thinking:
                thinking_parts.append(thinking)
    content = "\n".join(text_parts).strip() or None
    tool_calls = _extract_claude_tool_calls(raw_content)
    if (
        content is None
        and not tool_calls
        and stop_reason is not StopReason.CONTEXT_OVERFLOW
    ):
        raise empty_response("llm returned empty Claude content")
    return ChatResponse(
        content=content,
        tool_calls=tool_calls,
        reasoning="\n".join(thinking_parts).strip() or None,
        usage=anthropic_usage(payload.get("usage")),
        stop_reason=stop_reason,
        response_id=str(payload.get("id") or "").strip() or None,
        response_model=str(payload.get("model") or "").strip() or None,
    )


def _extract_claude_text(payload: dict[str, Any]) -> str:
    result = _extract_claude_result(payload)
    if result.get("stop_reason") is StopReason.CONTEXT_OVERFLOW:
        raise LLMIntegrationError(
            "text generation stopped because the provider context window was exceeded",
            error_code=LLMErrorCode.CONTEXT_OVERFLOW,
        )
    content = result["content"]
    if content is None:
        raise empty_response("llm returned empty Claude content")
    return content


def _openai_tool_spec(tool: LLMToolDefinition) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": sanitize_unicode(tool["name"]).strip(),
            "description": sanitize_unicode(tool["description"]).strip(),
            "parameters": sanitize_json(tool["parameters"]),
        },
    }


def _claude_tools_spec(tools: list[LLMToolDefinition]) -> list[dict[str, Any]]:
    return [
        {
            "name": sanitize_unicode(tool["name"]).strip(),
            "description": sanitize_unicode(tool["description"]).strip(),
            "input_schema": sanitize_json(tool["parameters"]),
        }
        for tool in tools
    ]


__all__ = [
    "_claude_stop_reason",
    "_claude_tools_spec",
    "_extract_claude_result",
    "_extract_claude_text",
    "_extract_openai_chat_result",
    "_extract_openai_chat_text",
    "_openai_stop_reason",
    "_openai_tool_spec",
]
