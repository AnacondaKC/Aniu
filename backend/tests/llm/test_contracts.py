"""Tests for normalized Agent/LLM type boundaries."""

from __future__ import annotations

import pytest

from backend.llm import (
    AssistantMessage,
    ChatResponse,
    ModelProtocol,
    StopReason,
    ThinkingContent,
    Usage,
    normalize_chat_response,
)


def test_normalize_chat_response_returns_typed_shape() -> None:
    response = normalize_chat_response(
        {
            "content": None,
            "tool_calls": [
                {
                    "id": "call-1",
                    "name": "search_news",
                    "arguments": {"query": "semiconductors"},
                }
            ],
        }
    )

    expected = ChatResponse(
        content=None,
        tool_calls=[
            {
                "id": "call-1",
                "name": "search_news",
                "arguments": {"query": "semiconductors"},
            }
        ],
    )
    assert response == expected


def test_normalize_chat_response_preserves_complete_assistant_message() -> None:
    message = AssistantMessage(
        protocol=ModelProtocol.CLAUDE_API,
        model="claude-sonnet",
        content=(ThinkingContent("thought", signature="signature"),),
        usage=Usage(input=10, output=3, reasoning=3, total_tokens=13),
        stop_reason=StopReason.TOOL_USE,
        response_id="message-1",
    )

    response = normalize_chat_response(message)

    assert response["assistant_message"] is message
    assert response["usage"] == message.usage
    assert response["stop_reason"] is StopReason.TOOL_USE
    assert response["reasoning"] == "thought"
    assert response["response_id"] == "message-1"


@pytest.mark.parametrize(
    "value",
    [
        [],
        {"content": 1, "tool_calls": []},
        {"content": "ok", "tool_calls": {}},
        {"content": "ok", "tool_calls": ["not-an-object"]},
        {
            "content": "ok",
            "tool_calls": [{"id": "call-1", "name": "tool", "arguments": []}],
        },
    ],
)
def test_normalize_chat_response_rejects_unsafe_shapes(value: object) -> None:
    with pytest.raises(ValueError):
        normalize_chat_response(value)
