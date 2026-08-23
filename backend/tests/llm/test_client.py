"""Stream-first LLM client behavior tests."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterable

import httpx
import pytest

from backend.llm import (
    Completed,
    LLMClient,
    LLMErrorCode,
    LLMIntegrationError,
    ModelProtocol,
    StopReason,
    StreamStarted,
    TextContent,
    TextDelta,
    TextEnded,
    TextStarted,
    Usage,
    normalize_chat_response,
)


def _sse_transport(
    events: Iterable[dict[str, object] | str],
    *,
    captured: dict[str, object] | None = None,
) -> httpx.MockTransport:
    body = "".join(
        (
            f"event: {event['type']}\n"
            if isinstance(event, dict) and isinstance(event.get("type"), str)
            else ""
        )
        + f"data: {event if isinstance(event, str) else json.dumps(event)}\n\n"
        for event in events
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        if captured is not None:
            captured["url"] = str(request.url)
            captured["headers"] = dict(request.headers)
            captured["json"] = json.loads(request.read())
        return httpx.Response(
            200,
            content=body.encode(),
            headers={"content-type": "text/event-stream"},
        )

    return httpx.MockTransport(handler)


def _openai_chunk(
    delta: dict[str, object],
    *,
    finish_reason: str | None = None,
    model: str = "gpt-4o-mini",
) -> dict[str, object]:
    return {
        "id": "chatcmpl-test",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": model,
        "choices": [
            {
                "index": 0,
                "delta": delta,
                "finish_reason": finish_reason,
            }
        ],
    }


def _openai_usage_chunk() -> dict[str, object]:
    return {
        "id": "chatcmpl-test",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "gpt-4o-mini-2024-07-18",
        "choices": [],
        "usage": {
            "prompt_tokens": 11,
            "completion_tokens": 7,
            "total_tokens": 18,
            "prompt_tokens_details": {"cached_tokens": 3},
            "completion_tokens_details": {"reasoning_tokens": 2},
        },
    }


def _claude_message_start(model: str = "claude-sonnet-4-6") -> dict[str, object]:
    return {
        "type": "message_start",
        "message": {
            "id": "msg-test",
            "type": "message",
            "role": "assistant",
            "content": [],
            "model": model,
            "stop_reason": None,
            "stop_sequence": None,
            "usage": {"input_tokens": 9, "output_tokens": 1},
        },
    }


def _claude_text_events(
    text: str, *, stop_reason: str = "end_turn"
) -> list[dict[str, object]]:
    return [
        _claude_message_start(),
        {
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": "", "citations": None},
        },
        {
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": text},
        },
        {"type": "content_block_stop", "index": 0},
        {
            "type": "message_delta",
            "delta": {"stop_reason": stop_reason, "stop_sequence": None},
            "usage": {"output_tokens": 4},
        },
        {"type": "message_stop"},
    ]


class _TestAbort(RuntimeError):
    pass


class _AbortSignal:
    def __init__(self, *, aborted: bool = False) -> None:
        self._aborted = aborted
        self._event = asyncio.Event()
        if aborted:
            self._event.set()

    @property
    def aborted(self) -> bool:
        return self._aborted

    def abort(self) -> None:
        self._aborted = True
        self._event.set()

    def throw_if_aborted(self) -> None:
        if self._aborted:
            raise _TestAbort("aborted")

    async def wait(self) -> None:
        await self._event.wait()


@pytest.mark.asyncio
async def test_client_rejects_pre_aborted_request_before_http() -> None:
    signal = _AbortSignal(aborted=True)

    def unexpected_request(_: httpx.Request) -> httpx.Response:
        raise AssertionError("aborted calls must not reach the provider")

    client = LLMClient(transport=httpx.MockTransport(unexpected_request))
    with pytest.raises(_TestAbort, match="aborted"):
        await client.generate_text(
            protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
            base_url="https://llm.example.com/v1",
            api_key="test-key",
            model="test-model",
            system_prompt=None,
            user_prompt="hello",
            temperature=0.0,
            abort_signal=signal,
        )
    await client.aclose()


@pytest.mark.asyncio
async def test_abort_interrupts_in_flight_http_request() -> None:
    request_started = asyncio.Event()
    never_respond = asyncio.Event()

    async def handler(_: httpx.Request) -> httpx.Response:
        request_started.set()
        await never_respond.wait()
        raise AssertionError("unreachable")

    signal = _AbortSignal()
    client = LLMClient(transport=httpx.MockTransport(handler))
    task = asyncio.create_task(
        client.generate_text(
            protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
            base_url="https://api.openai.com/v1",
            api_key="test-key",
            model="gpt-4o-mini",
            system_prompt=None,
            user_prompt="hello",
            temperature=0.0,
            abort_signal=signal,
        )
    )
    await asyncio.wait_for(request_started.wait(), timeout=5)
    signal.abort()
    with pytest.raises(_TestAbort, match="aborted"):
        await asyncio.wait_for(task, timeout=5)
    await client.aclose()


@pytest.mark.asyncio
async def test_openai_chat_streams_text_tool_call_and_usage() -> None:
    events = [
        _openai_chunk({"role": "assistant", "content": "先查询。"}),
        _openai_chunk(
            {
                "tool_calls": [
                    {
                        "index": 0,
                        "id": "call-1",
                        "type": "function",
                        "function": {"name": "search_news", "arguments": ""},
                    }
                ]
            }
        ),
        _openai_chunk(
            {"tool_calls": [{"index": 0, "function": {"arguments": '{"q":'}}]}
        ),
        _openai_chunk(
            {"tool_calls": [{"index": 0, "function": {"arguments": '"芯片"}'}}]},
            finish_reason="tool_calls",
        ),
        _openai_usage_chunk(),
        "[DONE]",
    ]
    client = LLMClient(transport=_sse_transport(events))

    result = await client.chat(
        protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        base_url="https://api.openai.com/v1",
        api_key="test-key",
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": "判断是否需要工具"}],
        temperature=0.0,
        tools=[
            {
                "name": "search_news",
                "description": "search",
                "parameters": {"type": "object"},
            }
        ],
    )

    assert result["content"] == "先查询。"
    assert result["tool_calls"] == [
        {"id": "call-1", "name": "search_news", "arguments": {"q": "芯片"}}
    ]
    assert result["stop_reason"] is StopReason.TOOL_USE
    assert result["usage"] == Usage(
        input=8,
        output=7,
        cache_read=3,
        reasoning=2,
        total_tokens=18,
    )
    assert result["response_id"] == "chatcmpl-test"
    assert result["response_model"] == "gpt-4o-mini-2024-07-18"
    await client.aclose()


@pytest.mark.asyncio
async def test_claude_chat_streams_text_tool_call_and_usage() -> None:
    events = [
        _claude_message_start(),
        {
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": "", "citations": None},
        },
        {
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "需要查询。"},
        },
        {"type": "content_block_stop", "index": 0},
        {
            "type": "content_block_start",
            "index": 1,
            "content_block": {
                "type": "tool_use",
                "id": "toolu-1",
                "name": "get_funds",
                "input": {},
            },
        },
        {
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "input_json_delta", "partial_json": '{"symbol":'},
        },
        {
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "input_json_delta", "partial_json": '"600519"}'},
        },
        {"type": "content_block_stop", "index": 1},
        {
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use", "stop_sequence": None},
            "usage": {"output_tokens": 8},
        },
        {"type": "message_stop"},
    ]
    client = LLMClient(transport=_sse_transport(events))

    result = await client.chat(
        protocol=ModelProtocol.CLAUDE_API,
        base_url="https://api.anthropic.com/v1",
        api_key="test-key",
        model="claude-sonnet-4-6",
        messages=[{"role": "user", "content": "判断"}],
        temperature=0.0,
        tools=[
            {
                "name": "get_funds",
                "description": "funds",
                "parameters": {"type": "object"},
            }
        ],
    )

    assert result["content"] == "需要查询。"
    assert result["tool_calls"] == [
        {
            "id": "toolu-1",
            "name": "get_funds",
            "arguments": {"symbol": "600519"},
        }
    ]
    assert result["stop_reason"] is StopReason.TOOL_USE
    assert result["usage"] == Usage(input=9, output=8, total_tokens=17)
    assert result["response_id"] == "msg-test"
    await client.aclose()


@pytest.mark.asyncio
async def test_claude_context_window_stop_is_preserved_for_recovery() -> None:
    client = LLMClient(
        transport=_sse_transport(
            _claude_text_events("partial", stop_reason="model_context_window_exceeded")
        )
    )

    result = await client.chat(
        protocol=ModelProtocol.CLAUDE_API,
        base_url="https://api.anthropic.com/v1",
        api_key="test-key",
        model="claude-sonnet-4-6",
        messages=[{"role": "user", "content": "判断"}],
        temperature=0.0,
    )

    assert result["content"] == "partial"
    assert result["stop_reason"] is StopReason.CONTEXT_OVERFLOW
    await client.aclose()


def test_normalization_keeps_empty_context_overflow_response() -> None:
    response = normalize_chat_response(
        {
            "content": None,
            "tool_calls": [],
            "stop_reason": StopReason.CONTEXT_OVERFLOW.value,
        }
    )

    assert response["content"] is None
    assert response["stop_reason"] is StopReason.CONTEXT_OVERFLOW


@pytest.mark.asyncio
async def test_openai_request_replays_tool_transcript_and_token_limit() -> None:
    captured: dict[str, object] = {}
    events = [
        _openai_chunk({"content": "停止。"}, finish_reason="stop"),
        "[DONE]",
    ]
    client = LLMClient(transport=_sse_transport(events, captured=captured))

    result = await client.chat(
        protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        base_url="https://api.openai.com/v1",
        api_key="test-key",
        model="gpt-4o-mini",
        messages=[
            {"role": "user", "content": "阶段任务"},
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "call-1",
                        "name": "search_news",
                        "arguments": {"query": "半导体"},
                    }
                ],
            },
            {
                "role": "tool",
                "tool_call_id": "call-1",
                "name": "search_news",
                "content": '{"items":[]}',
            },
        ],
        temperature=0.0,
        max_output_tokens=65_536,
    )

    request_json = captured["json"]
    assert isinstance(request_json, dict)
    assert result["content"] == "停止。"
    assert request_json["stream"] is True
    assert request_json["max_tokens"] == 65_536
    assert request_json["stream_options"] == {"include_usage": True}
    messages = request_json["messages"]
    assert isinstance(messages, list)
    assert [message["role"] for message in messages] == ["user", "assistant", "tool"]
    assert messages[1]["tool_calls"][0]["function"]["arguments"] == '{"query":"半导体"}'
    await client.aclose()


@pytest.mark.asyncio
async def test_generate_text_streams_text_and_reasoning_callbacks() -> None:
    events = [
        _openai_chunk({"reasoning_content": "先看风险"}, model="deepseek-reasoner"),
        _openai_chunk({"reasoning": "再看仓位"}, model="deepseek-reasoner"),
        _openai_chunk(
            {"content": "结论"}, finish_reason="stop", model="deepseek-reasoner"
        ),
        "[DONE]",
    ]
    text_deltas: list[str] = []
    reasoning_deltas: list[str] = []

    async def on_text_delta(delta: str) -> None:
        text_deltas.append(delta)

    async def on_reasoning_delta(delta: str) -> None:
        reasoning_deltas.append(delta)

    client = LLMClient(transport=_sse_transport(events))
    result = await client.generate_text(
        protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        base_url="https://api.openai.com/v1",
        api_key="test-key",
        model="deepseek-reasoner",
        system_prompt=None,
        user_prompt="判断风险",
        temperature=0.0,
        on_text_delta=on_text_delta,
        on_reasoning_delta=on_reasoning_delta,
    )

    assert result == "结论"
    assert text_deltas == ["结论"]
    assert reasoning_deltas == ["先看风险", "再看仓位"]
    await client.aclose()


@pytest.mark.asyncio
async def test_reasoning_callback_does_not_require_text_callback() -> None:
    events = [
        _openai_chunk({"reasoning_content": "思考"}),
        _openai_chunk({"content": "答案"}, finish_reason="stop"),
        "[DONE]",
    ]
    reasoning: list[str] = []

    async def on_reasoning_delta(delta: str) -> None:
        reasoning.append(delta)

    client = LLMClient(transport=_sse_transport(events))
    await client.chat(
        protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        base_url="https://api.openai.com/v1",
        api_key="test-key",
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": "think"}],
        temperature=0.0,
        on_reasoning_delta=on_reasoning_delta,
    )
    assert reasoning == ["思考"]
    await client.aclose()


@pytest.mark.asyncio
async def test_openai_content_filter_is_a_failure() -> None:
    finish_reason = "content_filter"
    client = LLMClient(
        transport=_sse_transport(
            [
                _openai_chunk({"content": "partial"}, finish_reason=finish_reason),
                "[DONE]",
            ]
        )
    )
    with pytest.raises(LLMIntegrationError) as exc_info:
        await client.chat(
            protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
            base_url="https://api.openai.com/v1",
            api_key="test-key",
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "answer"}],
            temperature=0.0,
        )
    assert exc_info.value.error_code is LLMErrorCode.INVALID_RESPONSE
    await client.aclose()


@pytest.mark.asyncio
async def test_openai_refusal_is_a_failure_even_with_stop_reason() -> None:
    client = LLMClient(
        transport=_sse_transport(
            [
                _openai_chunk(
                    {"refusal": "I cannot help with that"},
                    finish_reason="stop",
                ),
                "[DONE]",
            ]
        )
    )

    with pytest.raises(LLMIntegrationError, match="refused"):
        await client.chat(
            protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
            base_url="https://api.openai.com/v1",
            api_key="test-key",
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "answer"}],
            temperature=0.0,
        )
    await client.aclose()


@pytest.mark.asyncio
async def test_openai_length_is_a_normal_terminal_reason() -> None:
    client = LLMClient(
        transport=_sse_transport(
            [
                _openai_chunk({"content": "partial"}, finish_reason="length"),
                "[DONE]",
            ]
        )
    )

    result = await client.chat(
        protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        base_url="https://api.openai.com/v1",
        api_key="test-key",
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": "answer"}],
        temperature=0.0,
    )

    assert result["content"] == "partial"
    assert result["stop_reason"] is StopReason.LENGTH
    await client.aclose()


@pytest.mark.asyncio
async def test_claude_max_tokens_is_a_normal_terminal_reason() -> None:
    client = LLMClient(
        transport=_sse_transport(
            _claude_text_events("partial", stop_reason="max_tokens")
        )
    )

    result = await client.chat(
        protocol=ModelProtocol.CLAUDE_API,
        base_url="https://api.anthropic.com/v1",
        api_key="test-key",
        model="claude-sonnet-4-6",
        messages=[{"role": "user", "content": "answer"}],
        temperature=0.0,
    )

    assert result["content"] == "partial"
    assert result["stop_reason"] is StopReason.LENGTH
    await client.aclose()


@pytest.mark.asyncio
async def test_openai_rejects_stream_without_finish_reason() -> None:
    client = LLMClient(
        transport=_sse_transport([_openai_chunk({"content": "partial"}), "[DONE]"])
    )
    with pytest.raises(LLMIntegrationError, match="finish_reason") as exc_info:
        await client.chat(
            protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
            base_url="https://api.openai.com/v1",
            api_key="test-key",
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "answer"}],
            temperature=0.0,
        )
    assert exc_info.value.error_code is LLMErrorCode.INVALID_RESPONSE
    await client.aclose()


@pytest.mark.asyncio
async def test_generate_text_requires_complete_output() -> None:
    client = LLMClient(
        transport=_sse_transport(
            [
                _openai_chunk({"content": "partial"}, finish_reason="length"),
                "[DONE]",
            ]
        )
    )

    with pytest.raises(LLMIntegrationError, match="token limit"):
        await client.generate_text(
            protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
            base_url="https://api.openai.com/v1",
            api_key="test-key",
            model="gpt-4o-mini",
            system_prompt=None,
            user_prompt="answer",
            temperature=0.0,
        )
    await client.aclose()


@pytest.mark.asyncio
async def test_empty_openai_stream_is_not_success() -> None:
    client = LLMClient(transport=_sse_transport(["[DONE]"]))
    with pytest.raises(LLMIntegrationError, match="finish_reason"):
        await client.chat(
            protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
            base_url="https://api.openai.com/v1",
            api_key="test-key",
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "answer"}],
            temperature=0.0,
        )
    await client.aclose()


@pytest.mark.asyncio
async def test_invalid_tool_arguments_are_wrapped_as_llm_error() -> None:
    events = [
        _openai_chunk(
            {
                "tool_calls": [
                    {
                        "index": 0,
                        "id": "call-1",
                        "function": {"name": "search", "arguments": "not-json"},
                    }
                ]
            },
            finish_reason="tool_calls",
        ),
        "[DONE]",
    ]
    client = LLMClient(transport=_sse_transport(events))
    with pytest.raises(LLMIntegrationError) as exc_info:
        await client.chat(
            protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
            base_url="https://api.openai.com/v1",
            api_key="test-key",
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "answer"}],
            temperature=0.0,
        )
    assert exc_info.value.error_code is LLMErrorCode.INVALID_RESPONSE
    await client.aclose()


@pytest.mark.asyncio
async def test_stream_chat_emits_block_events_and_one_terminal_event() -> None:
    client = LLMClient(transport=_sse_transport(_claude_text_events("分析完成")))
    events = [
        event
        async for event in client.stream_chat(
            protocol=ModelProtocol.CLAUDE_API,
            base_url="https://api.anthropic.com/v1",
            api_key="test-key",
            model="claude-sonnet-4-6",
            messages=[{"role": "user", "content": "分析"}],
            temperature=0.0,
        )
    ]

    assert isinstance(events[0], StreamStarted)
    assert events[1:4] == [
        TextStarted(0),
        TextDelta("分析完成", 0),
        TextEnded("分析完成", 0),
    ]
    assert isinstance(events[-1], Completed)
    completed = events[-1]
    assert isinstance(completed, Completed)
    message = completed.message
    assert message.protocol is ModelProtocol.CLAUDE_API
    assert message.model == "claude-sonnet-4-6"
    assert message.content == (TextContent("分析完成"),)
    assert message.usage == Usage(input=9, output=4, total_tokens=13)
    assert message.stop_reason is StopReason.STOP
    assert message.response_id == "msg-test"
    await client.aclose()


@pytest.mark.asyncio
async def test_llm_client_reuses_shared_httpx_client_and_preserves_base_path() -> None:
    captured: dict[str, object] = {}
    client = LLMClient(
        transport=_sse_transport(
            [_openai_chunk({"content": "ok"}, finish_reason="stop"), "[DONE]"],
            captured=captured,
        )
    )
    first = client._ensure_client()
    assert client._ensure_client() is first

    text = await client.generate_text(
        protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        base_url="https://llm.example.com/v1",
        api_key="key",
        model="gpt-4o-mini",
        system_prompt=None,
        user_prompt="hi",
        temperature=0.0,
    )

    assert text == "ok"
    assert captured["url"] == "https://llm.example.com/v1/chat/completions"
    assert client._clients[()] is first
    await client.aclose()
    assert first.is_closed
    await client.aclose()
    assert client._clients == {}


@pytest.mark.asyncio
async def test_openai_sdk_preserves_base_url_query_parameters() -> None:
    captured: dict[str, object] = {}
    client = LLMClient(
        transport=_sse_transport(
            [_openai_chunk({"content": "ok"}, finish_reason="stop"), "[DONE]"],
            captured=captured,
        )
    )

    await client.generate_text(
        protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        base_url=(
            "https://azure.example.com/openai/deployments/demo"
            "?api-version=2025-01-01-preview&tag=a&tag=b"
        ),
        api_key="key",
        model="gpt-4o-mini",
        system_prompt=None,
        user_prompt="hi",
        temperature=0.0,
    )

    assert captured["url"] == (
        "https://azure.example.com/openai/deployments/demo/chat/completions"
        "?api-version=2025-01-01-preview&tag=a&tag=b"
    )
    assert captured["headers"]["api-key"] == "key"
    assert "authorization" not in captured["headers"]
    await client.aclose()


def test_llm_client_default_timeout_allows_slow_generations() -> None:
    client = LLMClient()
    assert client.timeout.read == 300.0
    assert client.timeout.connect == 10.0

    tuned = LLMClient(read_timeout=42.0, connect_timeout=5.0)
    assert tuned.timeout.read == 42.0
    assert tuned.timeout.connect == 5.0


@pytest.mark.asyncio
async def test_openai_requires_api_key() -> None:
    client = LLMClient(transport=_sse_transport([]))
    with pytest.raises(Exception, match="llm_api_key is not configured"):
        await client.generate_text(
            protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
            base_url="https://api.openai.com/v1",
            api_key="",
            model="gpt-4o-mini",
            system_prompt=None,
            user_prompt="test",
            temperature=0.0,
        )
