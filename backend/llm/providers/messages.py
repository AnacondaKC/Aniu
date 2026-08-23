"""Provider message transformations derived from Pi AI."""

from __future__ import annotations

import json
from typing import Any, cast

from backend.llm.contracts import (
    AssistantInputMessage,
    LLMChatMessage,
    LLMToolCall,
    ProviderJsonObject,
    TextContent,
    ThinkingContent,
    ToolCallContent,
    ToolResultMessage,
)
from backend.llm.errors import LLMConfigurationError, invalid_response
from backend.llm.sanitize import sanitize_json, sanitize_unicode


def _tool_call_id(tool_call: LLMToolCall, index: int) -> str:
    raw_id = sanitize_unicode(str(tool_call.get("id", ""))).strip()
    return raw_id or f"tool-call-{index}"


def _tool_call_arguments(tool_call: LLMToolCall) -> ProviderJsonObject:
    arguments = tool_call.get("arguments", {})
    if not isinstance(arguments, dict):
        raise invalid_response("tool call arguments must be a JSON object")
    sanitized = sanitize_json(arguments)
    if not isinstance(sanitized, dict):
        raise invalid_response("tool call arguments must be a JSON object")
    return sanitized


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise LLMConfigurationError(f"{field_name} must be text")
    return sanitize_unicode(value)


def _openai_chat_messages(
    messages: list[LLMChatMessage],
    *,
    replay_reasoning_content: bool = False,
) -> list[dict[str, Any]]:
    converted: list[dict[str, Any]] = []
    for message in messages:
        role = message["role"]
        if role in {"system", "user"}:
            converted.append(
                {"role": role, "content": _require_text(message["content"], "content")}
            )
            continue
        if role == "assistant":
            assistant = cast(AssistantInputMessage, message)
            converted_message: dict[str, Any] = {
                "role": "assistant",
                "content": (
                    sanitize_unicode(assistant["content"])
                    if assistant["content"] is not None
                    else None
                ),
            }
            tool_calls = assistant.get("tool_calls", [])
            if tool_calls:
                converted_message["tool_calls"] = [
                    {
                        "id": _tool_call_id(tool_call, index),
                        "type": "function",
                        "function": {
                            "name": sanitize_unicode(tool_call["name"]).strip(),
                            "arguments": json.dumps(
                                _tool_call_arguments(tool_call),
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ),
                        },
                    }
                    for index, tool_call in enumerate(tool_calls)
                    if sanitize_unicode(tool_call["name"]).strip()
                ]
            reasoning_details = assistant.get("reasoning_details", ())
            if reasoning_details:
                converted_details: list[dict[str, Any]] = []
                for raw_detail in reasoning_details:
                    detail = sanitize_json(raw_detail)
                    if not isinstance(detail, dict):
                        raise invalid_response(
                            "assistant reasoning_details must contain JSON objects"
                        )
                    converted_details.append(detail)
                converted_message["reasoning_details"] = converted_details
            elif replay_reasoning_content:
                converted_message["reasoning_content"] = sanitize_unicode(
                    assistant.get("reasoning") or ""
                )
            converted.append(converted_message)
            continue

        tool_message = cast(ToolResultMessage, message)
        tool_result: dict[str, Any] = {
            "role": "tool",
            "tool_call_id": sanitize_unicode(tool_message["tool_call_id"]).strip(),
            "content": sanitize_unicode(tool_message["content"]),
        }
        converted.append(tool_result)
    return converted


def _claude_system_prompt(messages: list[LLMChatMessage]) -> str | None:
    parts = [
        sanitize_unicode(message["content"])
        for message in messages
        if message["role"] == "system"
    ]
    text = "\n\n".join(part for part in parts if part).strip()
    return text or None


def _append_claude_user_blocks(
    converted: list[dict[str, Any]],
    blocks: list[dict[str, Any]],
) -> None:
    if not blocks:
        return
    if converted and converted[-1].get("role") == "user":
        content = converted[-1].get("content")
        if isinstance(content, list):
            content.extend(blocks)
            return
    converted.append({"role": "user", "content": blocks})


def _claude_assistant_blocks(
    assistant: AssistantInputMessage,
) -> list[dict[str, Any]]:
    rich_blocks = assistant.get("content_blocks")
    if rich_blocks is None:
        blocks: list[dict[str, Any]] = []
        content = assistant["content"]
        if content:
            blocks.append({"type": "text", "text": sanitize_unicode(content)})
        for index, tool_call in enumerate(assistant.get("tool_calls", [])):
            name = sanitize_unicode(tool_call["name"]).strip()
            if name:
                blocks.append(
                    {
                        "type": "tool_use",
                        "id": _tool_call_id(tool_call, index),
                        "name": name,
                        "input": _tool_call_arguments(tool_call),
                    }
                )
        return blocks

    blocks = []
    for block in rich_blocks:
        if isinstance(block, TextContent):
            if block.text:
                blocks.append({"type": "text", "text": sanitize_unicode(block.text)})
        elif isinstance(block, ThinkingContent):
            if block.redacted:
                if block.signature:
                    blocks.append(
                        {
                            "type": "redacted_thinking",
                            "data": sanitize_unicode(block.signature),
                        }
                    )
            elif block.thinking and block.signature:
                blocks.append(
                    {
                        "type": "thinking",
                        "thinking": sanitize_unicode(block.thinking),
                        "signature": sanitize_unicode(block.signature),
                    }
                )
        elif isinstance(block, ToolCallContent):
            name = sanitize_unicode(block.name).strip()
            if name:
                blocks.append(
                    {
                        "type": "tool_use",
                        "id": sanitize_unicode(block.id).strip(),
                        "name": name,
                        "input": _tool_call_arguments(
                            LLMToolCall(
                                id=block.id,
                                name=block.name,
                                arguments=block.arguments,
                            )
                        ),
                    }
                )
    return blocks


def _claude_messages(messages: list[LLMChatMessage]) -> list[dict[str, Any]]:
    converted: list[dict[str, Any]] = []
    for message in messages:
        role = message["role"]
        if role == "system":
            continue
        if role == "assistant":
            assistant = cast(AssistantInputMessage, message)
            blocks = _claude_assistant_blocks(assistant)
            if blocks:
                converted.append({"role": "assistant", "content": blocks})
            continue
        if role == "tool":
            tool_message = cast(ToolResultMessage, message)
            _append_claude_user_blocks(
                converted,
                [
                    {
                        "type": "tool_result",
                        "tool_use_id": sanitize_unicode(
                            tool_message["tool_call_id"]
                        ).strip(),
                        "content": sanitize_unicode(tool_message["content"]),
                        "is_error": bool(tool_message.get("is_error", False)),
                    }
                ],
            )
            continue
        _append_claude_user_blocks(
            converted,
            [{"type": "text", "text": _require_text(message["content"], "content")}],
        )
    return converted


__all__ = ["_claude_messages", "_claude_system_prompt", "_openai_chat_messages"]
