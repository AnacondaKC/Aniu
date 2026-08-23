"""Tests for shared LLM runtime helpers."""

from __future__ import annotations

import logging

import httpx
import pytest

from backend.agent.errors import (
    AgentConfigurationError,
    AgentErrorCode,
    AgentIntegrationError,
)
from backend.agent.kernel.context import AgentContext
from backend.agent.kernel.context_budget import ContextBudgetConfig
from backend.agent.kernel.llm_runtime import (
    _retryable_llm_error_code,
    llm_error_code,
)
from backend.agent.kernel.llm_runtime import (
    generate_text_output as _generate_text_output,
)
from backend.agent.kernel.llm_runtime import (
    generate_tool_loop_response as _generate_tool_loop_response,
)
from backend.agent.kernel.runtime_config import LlmRuntimeConfig
from backend.business.runs.abort import RunAbortSignal
from backend.business.shared import RunAbortError
from backend.llm import (
    LLMClient,
    LLMIntegrationError,
    ModelProtocol,
    RetryPolicy,
    estimate_provider_request_tokens,
)

_TEST_CLIENTS: dict[int, object] = {}


async def generate_text_output(
    context: AgentContext,
    **kwargs: object,
) -> str:
    return await _generate_text_output(
        context,
        llm_client=_TEST_CLIENTS.get(id(context)),  # type: ignore[arg-type]
        **kwargs,
    )


async def generate_tool_loop_response(
    context: AgentContext,
    **kwargs: object,
) -> dict[str, object]:
    return await _generate_tool_loop_response(
        context,
        llm_client=_TEST_CLIENTS.get(id(context)),  # type: ignore[arg-type]
        **kwargs,
    )


class FakeLLMClient:
    def __init__(self, response_text: str) -> None:
        self.response_text = response_text
        self.calls: list[dict[str, object]] = []

    async def generate_text(self, **kwargs) -> str:
        self.calls.append(dict(kwargs))
        return self.response_text

    async def chat(self, **kwargs) -> dict[str, object]:
        self.calls.append(dict(kwargs))
        return {"content": self.response_text, "tool_calls": []}


class FlakyTextLLMClient:
    def __init__(self, *, failures_before_success: int) -> None:
        self.failures_before_success = failures_before_success
        self.calls = 0

    async def generate_text(self, **kwargs) -> str:
        del kwargs
        self.calls += 1
        if self.calls <= self.failures_before_success:
            raise AgentIntegrationError("llm returned empty chat content")
        return "重试后成功"


class FlakyChatLLMClient:
    def __init__(self, *, failures_before_success: int) -> None:
        self.failures_before_success = failures_before_success
        self.calls = 0

    async def chat(self, **kwargs) -> dict[str, object]:
        del kwargs
        self.calls += 1
        if self.calls <= self.failures_before_success:
            raise AgentIntegrationError("llm request failed: ReadError")
        return {"content": "重试后成功", "tool_calls": []}


def make_context(
    *,
    with_runtime: bool,
    llm_client: object | None = None,
) -> AgentContext:
    runtime = (
        LlmRuntimeConfig(
            protocol=ModelProtocol.CLAUDE_API,
            base_url="https://example.invalid",
            api_key="test-key",
            model="gpt-4o-mini",
        )
        if with_runtime
        else None
    )
    client = llm_client or FakeLLMClient("分析完成")
    context = AgentContext(
        runtime=runtime,
        llm_client=client,  # type: ignore[arg-type]
    )
    if with_runtime:
        _TEST_CLIENTS[id(context)] = client
    return context


@pytest.mark.asyncio
async def test_generate_text_output_returns_model_text() -> None:
    context = make_context(with_runtime=True)

    result = await generate_text_output(
        context,
        label="Research",
        user_prompt="请输出分析结论",
    )

    assert result == "分析完成"


@pytest.mark.asyncio
async def test_text_call_clamps_output_cap_to_the_remaining_context() -> None:
    client = FakeLLMClient("完成")
    context = make_context(with_runtime=True, llm_client=client)
    runtime = LlmRuntimeConfig(
        protocol=ModelProtocol.CLAUDE_API,
        base_url="https://example.invalid",
        api_key="test-key",
        model="test-model",
        context_window_tokens=1_000,
        max_output_tokens=900,
    )
    context.runtime = runtime
    user_prompt = "字" * 600

    await generate_text_output(
        context,
        label="Research",
        user_prompt=user_prompt,
    )

    input_tokens = estimate_provider_request_tokens(
        [{"role": "user", "content": user_prompt}],
        [],
        protocol=runtime.protocol,
        model=runtime.model,
        provider_config=runtime.provider_config,
    )
    expected_cap = ContextBudgetConfig.from_runtime(runtime).output_tokens_for_input(
        input_tokens
    )
    assert client.calls[0]["max_output_tokens"] == expected_cap
    assert expected_cap < runtime.max_output_tokens


@pytest.mark.asyncio
async def test_tool_call_clamps_output_cap_using_provider_tool_payload() -> None:
    client = FakeLLMClient("完成")
    context = make_context(with_runtime=True, llm_client=client)
    runtime = LlmRuntimeConfig(
        protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        base_url="https://example.invalid",
        api_key="test-key",
        model="test-model",
        context_window_tokens=1_000,
        max_output_tokens=900,
    )
    context.runtime = runtime
    messages = [{"role": "user", "content": "字" * 520}]
    tools = [
        {
            "name": "lookup",
            "description": "Look up a value.",
            "parameters": {"type": "object", "properties": {}},
        }
    ]

    await generate_tool_loop_response(
        context,
        label="Research",
        messages=messages,
        tools=tools,
    )

    input_tokens = estimate_provider_request_tokens(
        messages,
        tools,
        protocol=runtime.protocol,
        model=runtime.model,
        provider_config=runtime.provider_config,
    )
    expected_cap = ContextBudgetConfig.from_runtime(runtime).output_tokens_for_input(
        input_tokens
    )
    assert client.calls[0]["max_output_tokens"] == expected_cap
    assert expected_cap < runtime.max_output_tokens


@pytest.mark.asyncio
async def test_generate_text_output_requires_configured_runtime() -> None:
    context = make_context(with_runtime=False)

    with pytest.raises(AgentConfigurationError, match="llm runtime"):
        await generate_text_output(
            context,
            label="Research",
            user_prompt="请输出分析结论",
        )


@pytest.mark.asyncio
async def test_generate_text_output_retries_transient_error_then_succeeds() -> None:
    client = FlakyTextLLMClient(failures_before_success=1)
    context = make_context(with_runtime=True, llm_client=client)

    result = await generate_text_output(
        context,
        label="Research",
        user_prompt="请输出分析结论",
    )

    assert result == "重试后成功"
    assert client.calls == 2


@pytest.mark.asyncio
async def test_generate_text_output_raises_after_repeated_transient_errors() -> None:
    client = FlakyTextLLMClient(failures_before_success=3)
    context = make_context(with_runtime=True, llm_client=client)

    with pytest.raises(AgentIntegrationError, match="empty chat content"):
        await generate_text_output(
            context,
            label="Research",
            user_prompt="请输出分析结论",
        )

    assert client.calls == 3


@pytest.mark.asyncio
async def test_generate_tool_loop_response_retries_transient_error_then_succeeds() -> (
    None
):
    client = FlakyChatLLMClient(failures_before_success=1)
    context = make_context(with_runtime=True, llm_client=client)

    result = await generate_tool_loop_response(
        context,
        label="Research",
        messages=[{"role": "user", "content": "请分析"}],
        tools=[],
    )

    assert result == {"content": "重试后成功", "tool_calls": []}
    assert client.calls == 2


@pytest.mark.asyncio
async def test_generate_tool_loop_response_raises_after_repeated_transient_errors() -> (
    None
):
    client = FlakyChatLLMClient(failures_before_success=3)
    context = make_context(with_runtime=True, llm_client=client)

    with pytest.raises(AgentIntegrationError, match="ReadError"):
        await generate_tool_loop_response(
            context,
            label="Research",
            messages=[{"role": "user", "content": "请分析"}],
            tools=[],
        )

    assert client.calls == 3


class StreamingChatLLMClient:
    """Simulates mid-turn prose + tool_calls, which must not flash as a report."""

    def __init__(
        self,
        *,
        content: str,
        tool_calls: list[dict[str, object]] | None = None,
    ) -> None:
        self.content = content
        self.tool_calls = tool_calls or []
        self.calls = 0

    async def chat(self, **kwargs) -> dict[str, object]:
        self.calls += 1
        on_text_delta = kwargs.get("on_text_delta")
        if callable(on_text_delta) and self.content:
            # Stream in pieces like a real model.
            mid = max(1, len(self.content) // 2)
            await on_text_delta(self.content[:mid])
            await on_text_delta(self.content[mid:])
        return {"content": self.content, "tool_calls": list(self.tool_calls)}


@pytest.mark.asyncio
async def test_tool_loop_does_not_publish_text_when_turn_has_tool_calls() -> None:
    client = StreamingChatLLMClient(
        content="先查一下行情再写报告",
        tool_calls=[{"id": "c1", "name": "query_market_data", "arguments": {}}],
    )
    context = make_context(with_runtime=True, llm_client=client)
    published: list[tuple[str, str]] = []

    async def sink(label: str, delta: str, channel: str = "text") -> None:
        published.append((label, delta, channel))

    context.stream_sink = sink

    result = await generate_tool_loop_response(
        context,
        label="ResearchToolLoop",
        messages=[{"role": "user", "content": "请分析"}],
        tools=[{"name": "query_market_data"}],
    )

    assert result["tool_calls"]
    assert published == []


@pytest.mark.asyncio
async def test_tool_loop_publishes_text_only_on_final_turn() -> None:
    client = StreamingChatLLMClient(content="这是最终研究报告正文", tool_calls=[])
    context = make_context(with_runtime=True, llm_client=client)
    published: list[str] = []

    async def sink(delta: str, channel: str = "text") -> None:
        if channel == "text":
            published.append(delta)

    context.stream_sink = sink

    result = await generate_tool_loop_response(
        context,
        label="ResearchToolLoop",
        messages=[{"role": "user", "content": "请分析"}],
        tools=[{"name": "query_market_data"}],
    )

    assert result["tool_calls"] == []
    assert "".join(published) == "这是最终研究报告正文"


def test_retryable_llm_error_code_retries_transient_http_statuses() -> None:
    assert (
        _retryable_llm_error_code(
            AgentIntegrationError("rate limited", status_code=429)
        )
        == "LLM_HTTP_TRANSIENT"
    )
    assert (
        _retryable_llm_error_code(AgentIntegrationError("bad gateway", status_code=502))
        == "LLM_HTTP_TRANSIENT"
    )
    assert (
        _retryable_llm_error_code(AgentIntegrationError("timeout", status_code=408))
        == "LLM_NETWORK_ERROR"
    )
    assert (
        _retryable_llm_error_code(AgentIntegrationError("conflict", status_code=409))
        == "LLM_HTTP_TRANSIENT"
    )
    assert (
        _retryable_llm_error_code(AgentIntegrationError("too early", status_code=425))
        == "LLM_HTTP_TRANSIENT"
    )
    assert (
        _retryable_llm_error_code(
            AgentIntegrationError("permission backend unavailable", status_code=429)
        )
        == "LLM_HTTP_TRANSIENT"
    )
    assert (
        _retryable_llm_error_code(
            AgentIntegrationError("api key service unavailable", status_code=503)
        )
        == "LLM_HTTP_TRANSIENT"
    )


def test_retryable_llm_error_code_does_not_retry_401_403_400() -> None:
    assert (
        _retryable_llm_error_code(
            AgentIntegrationError("unauthorized", status_code=401)
        )
        is None
    )
    assert (
        _retryable_llm_error_code(AgentIntegrationError("forbidden", status_code=403))
        is None
    )
    assert (
        _retryable_llm_error_code(AgentIntegrationError("bad request", status_code=400))
        is None
    )
    assert (
        _retryable_llm_error_code(AgentIntegrationError("invalid response shape"))
        is None
    )
    assert (
        _retryable_llm_error_code(AgentIntegrationError("timeout", status_code=401))
        is None
    )
    assert (
        _retryable_llm_error_code(
            AgentIntegrationError("connection rejected", status_code=400)
        )
        is None
    )
    assert (
        _retryable_llm_error_code(
            LLMIntegrationError(
                "provider retry budget exhausted",
                status_code=503,
                retryable_override=False,
            )
        )
        is None
    )
    assert (
        _retryable_llm_error_code(
            AgentIntegrationError(
                "context overflow",
                status_code=409,
                error_code=AgentErrorCode.CONTEXT_OVERFLOW,
            )
        )
        is None
    )
    assert (
        llm_error_code(
            LLMIntegrationError(
                "too long",
                status_code=400,
                error_code="context_overflow",
            )
        )
        == "LLM_CONTEXT_OVERFLOW"
    )


@pytest.mark.asyncio
async def test_generate_text_output_retries_429_then_succeeds() -> None:
    class RateLimitedThenOK:
        def __init__(self) -> None:
            self.calls = 0

        async def generate_text(self, **kwargs) -> str:
            del kwargs
            self.calls += 1
            if self.calls == 1:
                raise AgentIntegrationError(
                    "llm request failed: status 429: rate limited",
                    status_code=429,
                )
            return "限流后成功"

    client = RateLimitedThenOK()
    context = make_context(with_runtime=True, llm_client=client)

    result = await generate_text_output(
        context,
        label="Research",
        user_prompt="请输出分析结论",
    )

    assert result == "限流后成功"
    assert client.calls == 2


@pytest.mark.asyncio
async def test_provider_retry_exhaustion_does_not_multiply_agent_retries() -> None:
    calls = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"error": {"message": "unavailable"}})

    client = LLMClient(
        transport=httpx.MockTransport(handler),
        retry_policy=RetryPolicy(
            max_retries=2,
            base_delay_seconds=0,
            max_retry_delay_seconds=0,
        ),
    )
    context = make_context(with_runtime=True, llm_client=client)
    context.runtime = LlmRuntimeConfig(
        protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        base_url="https://api.openai.com/v1",
        api_key="test-key",
        model="gpt-4o-mini",
    )

    with pytest.raises(AgentIntegrationError, match="unavailable"):
        await generate_text_output(
            context,
            label="Research",
            user_prompt="analyze",
        )

    assert calls == 3
    await client.aclose()


@pytest.mark.asyncio
async def test_generate_text_output_does_not_retry_401() -> None:
    class Always401:
        def __init__(self) -> None:
            self.calls = 0

        async def generate_text(self, **kwargs) -> str:
            del kwargs
            self.calls += 1
            raise AgentIntegrationError(
                "llm request failed: status 401: invalid api key",
                status_code=401,
            )

    client = Always401()
    context = make_context(with_runtime=True, llm_client=client)

    with pytest.raises(AgentIntegrationError, match="401"):
        await generate_text_output(
            context,
            label="Research",
            user_prompt="请输出分析结论",
        )

    assert client.calls == 1


@pytest.mark.asyncio
async def test_text_call_logs_structured_size_and_context_fields(caplog) -> None:
    context = make_context(with_runtime=True)
    system_prompt = "系统"
    user_prompt = "请输出分析结论"
    caplog.set_level(logging.INFO, logger="backend.agent.kernel.llm_runtime")

    await generate_text_output(
        context,
        label="Research",
        system_prompt=system_prompt,
        user_prompt=user_prompt,
    )

    record = next(
        item for item in caplog.records if item.message == "llm_call_completed"
    )
    assert record.agent_label == "agent"
    assert record.provider == "example.invalid"
    assert record.model == "gpt-4o-mini"
    assert record.llm_mode == "text"
    assert record.system_prompt_bytes == len(system_prompt.encode())
    assert record.user_prompt_bytes == len(user_prompt.encode())
    assert record.input_bytes == len(system_prompt.encode()) + len(user_prompt.encode())
    assert record.output_bytes == len("分析完成".encode())
    assert record.attempt == 1
    assert record.retry_count == 0
    assert record.duration_ms >= 0
    assert not hasattr(record, "api_key")


@pytest.mark.asyncio
async def test_retry_logs_attempt_duration_and_error_code(caplog) -> None:
    client = FlakyTextLLMClient(failures_before_success=1)
    context = make_context(with_runtime=True, llm_client=client)
    caplog.set_level(logging.INFO, logger="backend.agent.kernel.llm_runtime")

    await generate_text_output(
        context,
        label="Research",
        user_prompt="请输出分析结论",
    )

    retry = next(item for item in caplog.records if item.message == "llm_call_retry")
    completed = next(
        item for item in caplog.records if item.message == "llm_call_completed"
    )
    assert retry.agent_label == "agent"
    assert retry.attempt == 1
    assert retry.retry_count == 1
    assert retry.error_code == "LLM_EMPTY_RESPONSE"
    assert retry.duration_ms >= 0
    assert completed.attempt == 2
    assert completed.retry_count == 1


@pytest.mark.asyncio
async def test_chat_call_logs_message_tool_and_output_sizes(caplog) -> None:
    context = make_context(with_runtime=True)
    messages = [{"role": "user", "content": "请分析"}]
    tools = [{"name": "query_market_data"}]
    caplog.set_level(logging.INFO, logger="backend.agent.kernel.llm_runtime")

    await generate_tool_loop_response(
        context,
        label="Research",
        messages=messages,
        tools=tools,
    )

    record = next(
        item for item in caplog.records if item.message == "llm_call_completed"
    )
    assert record.llm_mode == "chat"
    assert record.message_count == 1
    assert record.tool_definition_count == 1
    assert record.tool_call_count == 0
    assert record.input_bytes > 0
    assert record.output_bytes > 0


@pytest.mark.asyncio
async def test_text_call_logs_unexpected_client_failure(caplog) -> None:
    class BrokenClient:
        async def generate_text(self, **kwargs) -> str:
            del kwargs
            raise RuntimeError("unexpected callback failure")

    context = make_context(with_runtime=True, llm_client=BrokenClient())
    caplog.set_level(logging.INFO, logger="backend.agent.kernel.llm_runtime")

    with pytest.raises(RuntimeError, match="unexpected callback failure"):
        await generate_text_output(
            context,
            label="Research",
            user_prompt="请输出分析结论",
        )

    failed = next(item for item in caplog.records if item.message == "llm_call_failed")
    assert failed.error_code == "RuntimeError"
    assert failed.duration_ms >= 0
    assert failed.status == "failed"
    assert failed.exc_info is not None


@pytest.mark.asyncio
async def test_text_call_logs_abort_after_provider_returns(caplog) -> None:
    abort_signal = RunAbortSignal(run_id=1)

    class AbortAfterResponseClient:
        async def generate_text(self, **kwargs) -> str:
            del kwargs
            abort_signal.abort("test")
            return "completed but discarded"

    context = make_context(with_runtime=True, llm_client=AbortAfterResponseClient())
    context.abort_signal = abort_signal
    caplog.set_level(logging.INFO, logger="backend.agent.kernel.llm_runtime")

    with pytest.raises(RunAbortError):
        await generate_text_output(
            context,
            label="Research",
            user_prompt="请输出分析结论",
        )

    aborted = next(
        item for item in caplog.records if item.message == "llm_call_aborted"
    )
    assert aborted.error_code == "LLM_CALL_ABORTED"
    assert aborted.duration_ms >= 0
    assert aborted.status == "aborted"


@pytest.mark.asyncio
async def test_text_call_logs_abort_during_retry_backoff(caplog) -> None:
    abort_signal = RunAbortSignal(run_id=1)

    class AbortBeforeRetryClient:
        async def generate_text(self, **kwargs) -> str:
            del kwargs
            abort_signal.abort("test")
            raise AgentIntegrationError("llm request failed: ReadError")

    context = make_context(with_runtime=True, llm_client=AbortBeforeRetryClient())
    context.abort_signal = abort_signal
    caplog.set_level(logging.INFO, logger="backend.agent.kernel.llm_runtime")

    with pytest.raises(RunAbortError):
        await generate_text_output(
            context,
            label="Research",
            user_prompt="请输出分析结论",
        )

    events = [item.message for item in caplog.records]
    assert "llm_call_retry" in events
    aborted = next(
        item for item in caplog.records if item.message == "llm_call_aborted"
    )
    assert aborted.retry_count == 1
    assert aborted.status == "aborted"


@pytest.mark.asyncio
async def test_chat_call_logs_abort_during_retry_backoff(caplog) -> None:
    abort_signal = RunAbortSignal(run_id=1)

    class AbortBeforeRetryClient:
        async def chat(self, **kwargs):
            del kwargs
            abort_signal.abort("test")
            raise AgentIntegrationError("llm request failed: ReadError")

    context = make_context(with_runtime=True, llm_client=AbortBeforeRetryClient())
    context.abort_signal = abort_signal
    caplog.set_level(logging.INFO, logger="backend.agent.kernel.llm_runtime")

    with pytest.raises(RunAbortError):
        await generate_tool_loop_response(
            context,
            label="Research",
            messages=[{"role": "user", "content": "请分析"}],
            tools=[],
        )

    events = [item.message for item in caplog.records]
    assert "llm_call_retry" in events
    aborted = next(
        item for item in caplog.records if item.message == "llm_call_aborted"
    )
    assert aborted.retry_count == 1
    assert aborted.status == "aborted"
