"""Wire-level stream compatibility tests for both provider SDKs."""

from __future__ import annotations

import json

import httpx
import pytest

from backend.llm import (
    Completed,
    Failed,
    LLMClient,
    LLMErrorCode,
    ModelProtocol,
    ReasoningDelta,
    ReasoningEnded,
    ReasoningStarted,
    StopReason,
    TextDelta,
    TextEnded,
    TextStarted,
    ThinkingContent,
    is_error_retryable,
)


def _anthropic_sse(events: list[dict[str, object]]) -> bytes:
    return "".join(
        f"event: {event['type']}\ndata: {json.dumps(event)}\n\n" for event in events
    ).encode()


@pytest.mark.asyncio
async def test_openai_sdk_parses_multiline_sse_data_event() -> None:
    body = (
        'data: {"id":"chatcmpl-test","object":"chat.completion.chunk",\n'
        'data: "created":1,"model":"gpt-4o-mini","choices":[{"index":0,\n'
        'data: "delta":{"content":"ok"},"finish_reason":"stop"}]}\n\n'
        "data: [DONE]\n\n"
    )

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=body.encode(),
            headers={"content-type": "text/event-stream"},
        )

    client = LLMClient(transport=httpx.MockTransport(handler))
    result = await client.generate_text(
        protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        base_url="https://api.openai.com/v1",
        api_key="test-key",
        model="gpt-4o-mini",
        system_prompt=None,
        user_prompt="hello",
        temperature=0.0,
    )

    assert result == "ok"


@pytest.mark.asyncio
async def test_openai_failure_preserves_partial_text_and_closes_block() -> None:
    chunk = {
        "id": "chatcmpl-test",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "gpt-4o-mini",
        "choices": [
            {
                "index": 0,
                "delta": {"content": "partial"},
                "finish_reason": None,
            }
        ],
    }
    body = f"data: {json.dumps(chunk)}\n\ndata: [DONE]\n\n"

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=body.encode(),
            headers={"content-type": "text/event-stream"},
        )

    client = LLMClient(transport=httpx.MockTransport(handler))
    events = [
        event
        async for event in client.stream_chat(
            protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
            base_url="https://api.openai.com/v1",
            api_key="test-key",
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "hello"}],
            temperature=0.0,
        )
    ]

    assert TextStarted(0) in events
    assert TextDelta("partial", 0) in events
    assert TextEnded("partial", 0) in events
    terminal = events[-1]
    assert isinstance(terminal, Failed)
    assert terminal.message.text == "partial"
    assert terminal.message.stop_reason is StopReason.ERROR
    await client.aclose()


@pytest.mark.asyncio
async def test_anthropic_stream_preserves_thinking_signature() -> None:
    events: list[dict[str, object]] = [
        {
            "type": "message_start",
            "message": {
                "id": "msg-test",
                "type": "message",
                "role": "assistant",
                "content": [],
                "model": "claude-sonnet-4-6",
                "stop_reason": None,
                "stop_sequence": None,
                "usage": {"input_tokens": 5, "output_tokens": 1},
            },
        },
        {
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "thinking", "thinking": "", "signature": ""},
        },
        {
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "thinking_delta", "thinking": "inspect risk"},
        },
        {
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "signature_delta", "signature": "signed"},
        },
        {"type": "content_block_stop", "index": 0},
        {
            "type": "content_block_start",
            "index": 1,
            "content_block": {"type": "text", "text": "", "citations": None},
        },
        {
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "text_delta", "text": "answer"},
        },
        {"type": "content_block_stop", "index": 1},
        {
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn", "stop_sequence": None},
            "usage": {"output_tokens": 6},
        },
        {"type": "message_stop"},
    ]

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=_anthropic_sse(events),
            headers={"content-type": "text/event-stream"},
        )

    client = LLMClient(transport=httpx.MockTransport(handler))
    stream_events = [
        event
        async for event in client.stream_chat(
            protocol=ModelProtocol.CLAUDE_API,
            base_url="https://api.anthropic.com/v1",
            api_key="test-key",
            model="claude-sonnet-4-6",
            messages=[{"role": "user", "content": "analyze"}],
            temperature=0.0,
        )
    ]

    assert ReasoningStarted(0) in stream_events
    assert ReasoningDelta("inspect risk", 0) in stream_events
    assert ReasoningEnded("inspect risk", 0) in stream_events
    assert TextDelta("answer", 1) in stream_events
    terminal = stream_events[-1]
    assert isinstance(terminal, Completed)
    assert terminal.message.stop_reason is StopReason.STOP
    assert terminal.message.content[0] == ThinkingContent(
        "inspect risk",
        signature="signed",
    )
    await client.aclose()


@pytest.mark.asyncio
async def test_anthropic_stream_error_becomes_typed_failed_event() -> None:
    events = [
        {
            "type": "message_start",
            "message": {
                "id": "msg-test",
                "type": "message",
                "role": "assistant",
                "content": [],
                "model": "claude-sonnet-4-6",
                "stop_reason": None,
                "stop_sequence": None,
                "usage": {"input_tokens": 5, "output_tokens": 1},
            },
        },
        {
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": "", "citations": None},
        },
        {
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "partial"},
        },
        {
            "type": "error",
            "error": {"type": "overloaded_error", "message": "Overloaded"},
        },
    ]

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=_anthropic_sse(events),
            headers={"content-type": "text/event-stream"},
        )

    client = LLMClient(transport=httpx.MockTransport(handler))
    events = [
        item
        async for item in client.stream_chat(
            protocol=ModelProtocol.CLAUDE_API,
            base_url="https://api.anthropic.com/v1",
            api_key="test-key",
            model="claude-sonnet-4-6",
            messages=[{"role": "user", "content": "analyze"}],
            temperature=0.0,
        )
    ]

    terminal = events[-1]
    assert isinstance(terminal, Failed)
    assert terminal.message.stop_reason is StopReason.ERROR
    assert terminal.message.text == "partial"
    assert TextEnded("partial", 0) in events
    assert events.index(TextEnded("partial", 0)) < len(events) - 1
    assert getattr(terminal.error, "error_code") is LLMErrorCode.PROVIDER_5XX
    assert is_error_retryable(terminal.error) is False
    await client.aclose()


@pytest.mark.asyncio
async def test_openai_buffers_tool_arguments_until_tool_start_event() -> None:
    chunks = [
        {
            "id": "chatcmpl-test",
            "object": "chat.completion.chunk",
            "created": 1,
            "model": "gpt-4o-mini",
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "tool_calls": [{"index": 0, "function": {"arguments": '{"q":'}}]
                    },
                    "finish_reason": None,
                }
            ],
        },
        {
            "id": "chatcmpl-test",
            "object": "chat.completion.chunk",
            "created": 1,
            "model": "gpt-4o-mini",
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call-1",
                                "function": {
                                    "name": "search",
                                    "arguments": '"risk"}',
                                },
                            }
                        ]
                    },
                    "finish_reason": "tool_calls",
                }
            ],
        },
    ]
    body = "".join(f"data: {json.dumps(chunk)}\n\n" for chunk in chunks)
    body += "data: [DONE]\n\n"

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=body.encode(),
            headers={"content-type": "text/event-stream"},
        )

    client = LLMClient(transport=httpx.MockTransport(handler))
    events = [
        event
        async for event in client.stream_chat(
            protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
            base_url="https://api.openai.com/v1",
            api_key="test-key",
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "search"}],
            temperature=0.0,
        )
    ]

    event_types = [type(event).__name__ for event in events]
    assert event_types.index("ToolCallStarted") < event_types.index("ToolCallDelta")
    terminal = events[-1]
    assert isinstance(terminal, Completed)
    assert terminal.message.tool_calls[0]["arguments"] == {"q": "risk"}
    await client.aclose()


@pytest.mark.asyncio
async def test_anthropic_preserves_redacted_thinking_block() -> None:
    events: list[dict[str, object]] = [
        {
            "type": "message_start",
            "message": {
                "id": "msg-redacted",
                "type": "message",
                "role": "assistant",
                "content": [],
                "model": "claude-sonnet-4-6",
                "stop_reason": None,
                "stop_sequence": None,
                "usage": {"input_tokens": 5, "output_tokens": 1},
            },
        },
        {
            "type": "content_block_start",
            "index": 0,
            "content_block": {
                "type": "redacted_thinking",
                "data": "encrypted-thinking",
            },
        },
        {"type": "content_block_stop", "index": 0},
        {
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn", "stop_sequence": None},
            "usage": {"output_tokens": 2},
        },
        {"type": "message_stop"},
    ]

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=_anthropic_sse(events),
            headers={"content-type": "text/event-stream"},
        )

    client = LLMClient(transport=httpx.MockTransport(handler))
    terminal_events = [
        event
        async for event in client.stream_chat(
            protocol=ModelProtocol.CLAUDE_API,
            base_url="https://api.anthropic.com/v1",
            api_key="test-key",
            model="claude-sonnet-4-6",
            messages=[{"role": "user", "content": "analyze"}],
            temperature=0.0,
        )
    ]

    terminal = terminal_events[-1]
    assert isinstance(terminal, Completed)
    assert terminal.message.content == (
        ThinkingContent("", signature="encrypted-thinking", redacted=True),
    )
    await client.aclose()
