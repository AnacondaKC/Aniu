"""Provider message conversion and Unicode boundary tests."""

from __future__ import annotations

import pytest

from backend.llm import (
    LLMConfigurationError,
    LLMIntegrationError,
    ModelProtocol,
    ThinkingContent,
    ToolCallContent,
    assistant_input_from_message,
)
from backend.llm.providers.http import _parse_tool_arguments
from backend.llm.providers.messages import (
    _claude_messages,
    _claude_system_prompt,
    _openai_chat_messages,
)
from backend.llm.sanitize import sanitize_json, sanitize_unicode

TOOL_HISTORY = [
    {"role": "system", "content": "system-a"},
    {"role": "system", "content": "system-b"},
    {"role": "user", "content": "task"},
    {
        "role": "assistant",
        "content": None,
        "tool_calls": [
            {
                "id": "call-1",
                "name": "search_news",
                "arguments": {"query": "芯片"},
            }
        ],
        "reasoning": "private thought",
    },
    {
        "role": "tool",
        "tool_call_id": "call-1",
        "name": "search_news",
        "content": '{"items":[]}',
    },
    {"role": "user", "content": "continue"},
]


def test_openai_replays_tool_round_without_stringifying_null_content() -> None:
    converted = _openai_chat_messages(TOOL_HISTORY)

    assistant = converted[3]
    assert assistant["content"] is None
    assert assistant["tool_calls"][0]["function"] == {
        "name": "search_news",
        "arguments": '{"query":"芯片"}',
    }
    assert converted[4] == {
        "role": "tool",
        "tool_call_id": "call-1",
        "content": '{"items":[]}',
    }


def test_openai_replays_reasoning_only_when_compatibility_requires_it() -> None:
    normal = _openai_chat_messages(TOOL_HISTORY)
    compatible = _openai_chat_messages(
        TOOL_HISTORY,
        replay_reasoning_content=True,
    )

    assert "reasoning_content" not in normal[3]
    assert compatible[3]["reasoning_content"] == "private thought"


def test_claude_extracts_system_and_merges_tool_result_with_next_user_turn() -> None:
    assert _claude_system_prompt(TOOL_HISTORY) == "system-a\n\nsystem-b"

    converted = _claude_messages(TOOL_HISTORY)

    assert [message["role"] for message in converted] == [
        "user",
        "assistant",
        "user",
    ]
    assistant_blocks = converted[1]["content"]
    assert assistant_blocks == [
        {
            "type": "tool_use",
            "id": "call-1",
            "name": "search_news",
            "input": {"query": "芯片"},
        }
    ]
    user_blocks = converted[2]["content"]
    assert user_blocks == [
        {
            "type": "tool_result",
            "tool_use_id": "call-1",
            "content": '{"items":[]}',
            "is_error": False,
        },
        {"type": "text", "text": "continue"},
    ]


def test_claude_replays_rich_thinking_blocks_with_signatures() -> None:
    from backend.llm import AssistantMessage

    message = AssistantMessage(
        protocol=ModelProtocol.CLAUDE_API,
        model="claude-sonnet",
        content=(
            ThinkingContent("private thought", signature="signed-thinking"),
            ToolCallContent("call-1", "search_news", {"query": "芯片"}),
        ),
    )
    converted = _claude_messages([assistant_input_from_message(message)])

    assert converted == [
        {
            "role": "assistant",
            "content": [
                {
                    "type": "thinking",
                    "thinking": "private thought",
                    "signature": "signed-thinking",
                },
                {
                    "type": "tool_use",
                    "id": "call-1",
                    "name": "search_news",
                    "input": {"query": "芯片"},
                },
            ],
        }
    ]


def test_unicode_sanitizer_preserves_valid_unicode_and_repairs_surrogates() -> None:
    assert sanitize_unicode("中文😀") == "中文😀"
    assert sanitize_unicode("before\ud800after") == "before\ufffdafter"
    assert sanitize_unicode("\ud83d\ude00") == "😀"
    assert sanitize_json({"\ud800": ["ok", "\udc00"]}) == {"\ufffd": ["ok", "\ufffd"]}


def test_message_conversion_sanitizes_nested_tool_arguments() -> None:
    converted = _openai_chat_messages(
        [
            {
                "role": "assistant",
                "content": "bad\ud800text",
                "tool_calls": [
                    {
                        "id": "call-1",
                        "name": "tool",
                        "arguments": {"value": "bad\udc00value"},
                    }
                ],
            }
        ]
    )

    assert converted[0]["content"] == "bad\ufffdtext"
    assert "bad\ufffdvalue" in converted[0]["tool_calls"][0]["function"]["arguments"]


def test_provider_json_rejects_non_json_values() -> None:
    with pytest.raises(LLMConfigurationError, match="unsupported type"):
        sanitize_json({"bad": object()})
    with pytest.raises(LLMConfigurationError, match="keys must be text"):
        sanitize_json({1: "bad"})
    with pytest.raises(LLMConfigurationError, match="must be finite"):
        sanitize_json({"bad": float("nan")})
    with pytest.raises(LLMIntegrationError, match="invalid JSON"):
        _parse_tool_arguments('{"bad": NaN}')
