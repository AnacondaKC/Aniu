"""Provider error normalization and retry behavior tests."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest

from backend.llm import (
    LLMClient,
    LLMErrorCode,
    LLMIntegrationError,
    ModelProtocol,
    RetryPolicy,
)
from backend.llm.abort import await_with_abort
from backend.llm.errors import is_error_retryable, provider_error
from backend.llm.retry import retry_provider_request


class _FakeProviderError(Exception):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        headers: dict[str, str] | None = None,
        body: object = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response = SimpleNamespace(
            status_code=status_code,
            headers=headers or {},
        )
        self.body = body


class _Abort(RuntimeError):
    pass


class _AbortSignal:
    def __init__(self) -> None:
        self._event = asyncio.Event()
        self.wait_started = asyncio.Event()
        self.wait_cancelled = asyncio.Event()

    @property
    def aborted(self) -> bool:
        return self._event.is_set()

    def abort(self) -> None:
        self._event.set()

    def throw_if_aborted(self) -> None:
        if self.aborted:
            raise _Abort("aborted")

    async def wait(self) -> None:
        self.wait_started.set()
        try:
            await self._event.wait()
        except asyncio.CancelledError:
            self.wait_cancelled.set()
            raise


def test_provider_error_classifies_status_retry_after_and_context_overflow() -> None:
    rate_limit = provider_error(
        _FakeProviderError(
            "rate limited",
            status_code=429,
            headers={"retry-after-ms": "250"},
        )
    )
    overflow = provider_error(
        _FakeProviderError(
            "bad request",
            status_code=400,
            body={"error": {"code": "context_length_exceeded"}},
        )
    )

    assert rate_limit.error_code is LLMErrorCode.RATE_LIMIT
    assert rate_limit.retry_after == 0.25
    assert overflow.error_code is LLMErrorCode.CONTEXT_OVERFLOW


@pytest.mark.parametrize(
    "message",
    [
        "maximum context length is exceeded",
        "model_context_window_exceeded",
        "prompt too long; exceeded the limit",
        "request too large for model with this context window",
        "context length exceeded",
    ],
)
def test_provider_error_recognizes_provider_context_overflow_markers(
    message: str,
) -> None:
    error = provider_error(_FakeProviderError(message, status_code=400))

    assert error.error_code is LLMErrorCode.CONTEXT_OVERFLOW


def test_provider_error_prefers_authentication_over_context_text() -> None:
    error = provider_error(
        _FakeProviderError(
            "authentication failed: context window is unavailable",
            status_code=401,
        )
    )

    assert error.error_code is LLMErrorCode.AUTHENTICATION


def test_context_overflow_is_not_retryable_for_transient_status() -> None:
    error = provider_error(
        _FakeProviderError(
            "context length exceeded",
            status_code=409,
            headers={"x-should-retry": "true"},
        )
    )

    assert error.error_code is LLMErrorCode.CONTEXT_OVERFLOW
    assert is_error_retryable(error) is False

    forced_off = provider_error(
        _FakeProviderError(
            "server error",
            status_code=503,
            headers={"x-should-retry": "false"},
        )
    )
    forced_on = provider_error(
        _FakeProviderError(
            "bad request",
            status_code=400,
            headers={"x-should-retry": "true"},
        )
    )

    assert is_error_retryable(forced_off) is False
    assert is_error_retryable(forced_on) is True


@pytest.mark.parametrize(
    ("message", "body", "expected"),
    [
        ("stream error", {"type": "overloaded_error"}, LLMErrorCode.PROVIDER_5XX),
        ("stream error", {"type": "rate_limit_error"}, LLMErrorCode.RATE_LIMIT),
        (
            "stream error",
            {"type": "authentication_error"},
            LLMErrorCode.AUTHENTICATION,
        ),
    ],
)
def test_provider_error_classifies_anthropic_stream_error_types(
    message: str,
    body: object,
    expected: LLMErrorCode,
) -> None:
    error = provider_error(_FakeProviderError(message, body=body))
    assert error.error_code is expected


@pytest.mark.asyncio
async def test_retry_provider_request_retries_transient_failure() -> None:
    calls = 0

    async def operation() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise LLMIntegrationError(
                "temporary",
                status_code=503,
                error_code=LLMErrorCode.PROVIDER_5XX,
            )
        return "ok"

    result = await retry_provider_request(
        operation,
        signal=None,
        policy=RetryPolicy(
            max_retries=2,
            base_delay_seconds=0,
            max_retry_delay_seconds=0,
        ),
    )

    assert result == "ok"
    assert calls == 3


@pytest.mark.asyncio
async def test_exhausted_provider_retry_budget_disables_outer_retry() -> None:
    calls = 0

    async def operation() -> str:
        nonlocal calls
        calls += 1
        raise LLMIntegrationError("temporary", status_code=503)

    with pytest.raises(LLMIntegrationError) as exc_info:
        await retry_provider_request(
            operation,
            signal=None,
            policy=RetryPolicy(
                max_retries=1,
                base_delay_seconds=0,
                max_retry_delay_seconds=0,
            ),
        )

    assert calls == 2
    assert is_error_retryable(exc_info.value) is False


@pytest.mark.asyncio
async def test_server_retry_delay_over_policy_limit_fails_without_sleeping() -> None:
    async def operation() -> str:
        raise LLMIntegrationError(
            "wait",
            status_code=429,
            retry_after=61,
        )

    with pytest.raises(LLMIntegrationError, match="requested 61s") as exc_info:
        await retry_provider_request(
            operation,
            signal=None,
            policy=RetryPolicy(max_retries=1, max_retry_delay_seconds=60),
        )

    assert exc_info.value.error_code is LLMErrorCode.UNKNOWN
    assert is_error_retryable(exc_info.value) is False


@pytest.mark.asyncio
async def test_retry_provider_request_does_not_retry_permanent_failure() -> None:
    calls = 0

    async def operation() -> str:
        nonlocal calls
        calls += 1
        raise LLMIntegrationError(
            "bad key",
            status_code=401,
            error_code=LLMErrorCode.AUTHENTICATION,
        )

    with pytest.raises(LLMIntegrationError, match="bad key"):
        await retry_provider_request(
            operation,
            signal=None,
            policy=RetryPolicy(max_retries=3, base_delay_seconds=0),
        )
    assert calls == 1


@pytest.mark.asyncio
async def test_abort_interrupts_retry_backoff() -> None:
    signal = _AbortSignal()
    attempted = asyncio.Event()

    async def operation() -> str:
        attempted.set()
        raise LLMIntegrationError(
            "retry later",
            status_code=429,
            error_code=LLMErrorCode.RATE_LIMIT,
            retry_after=60,
        )

    task = asyncio.create_task(
        retry_provider_request(
            operation,
            signal=signal,
            policy=RetryPolicy(max_retries=2),
        )
    )
    await attempted.wait()
    signal.abort()

    with pytest.raises(_Abort, match="aborted"):
        await asyncio.wait_for(task, timeout=1)


@pytest.mark.asyncio
async def test_await_with_abort_waits_for_abort_waiter_cleanup() -> None:
    signal = _AbortSignal()

    async def operation() -> str:
        await signal.wait_started.wait()
        return "ok"

    assert await await_with_abort(operation(), signal) == "ok"
    assert signal.wait_cancelled.is_set()


@pytest.mark.asyncio
async def test_llm_client_retries_openai_request_establishment() -> None:
    calls = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                429,
                json={"error": {"message": "slow down", "type": "rate_limit_error"}},
                headers={"retry-after": "0"},
            )
        chunk = {
            "id": "chatcmpl-test",
            "object": "chat.completion.chunk",
            "created": 1,
            "model": "gpt-4o-mini",
            "choices": [
                {
                    "index": 0,
                    "delta": {"content": "ok"},
                    "finish_reason": "stop",
                }
            ],
        }
        body = f"data: {json.dumps(chunk)}\n\ndata: [DONE]\n\n"
        return httpx.Response(
            200,
            content=body.encode(),
            headers={"content-type": "text/event-stream"},
        )

    client = LLMClient(
        transport=httpx.MockTransport(handler),
        retry_policy=RetryPolicy(
            max_retries=1,
            base_delay_seconds=0,
            max_retry_delay_seconds=0,
        ),
    )
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
    assert calls == 2
    await client.aclose()
