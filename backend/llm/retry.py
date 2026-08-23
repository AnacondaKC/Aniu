"""Provider-request retry policy derived from Pi AI."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from backend.llm.abort import AbortSignal, await_with_abort, throw_if_aborted
from backend.llm.errors import (
    LLMErrorCode,
    LLMIntegrationError,
    is_error_retryable,
    provider_error,
)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_retries: int = 2
    base_delay_seconds: float = 0.5
    max_retry_delay_seconds: float = 60.0

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.base_delay_seconds < 0:
            raise ValueError("base_delay_seconds must be >= 0")
        if self.max_retry_delay_seconds < 0:
            raise ValueError("max_retry_delay_seconds must be >= 0")


def _retry_delay(
    error: LLMIntegrationError, retry_index: int, policy: RetryPolicy
) -> float:
    if error.retry_after is not None:
        if (
            policy.max_retry_delay_seconds > 0
            and error.retry_after > policy.max_retry_delay_seconds
        ):
            raise LLMIntegrationError(
                (
                    f"server requested {error.retry_after:g}s retry delay "
                    f"(max: {policy.max_retry_delay_seconds:g}s): {error}"
                ),
                status_code=error.status_code,
                error_code=LLMErrorCode.UNKNOWN,
                retryable_override=False,
            )
        return error.retry_after
    exponential = min(policy.base_delay_seconds * (2**retry_index), 8.0)
    return float(exponential * (1.0 - random.random() * 0.25))


async def retry_provider_request[T](
    operation: Callable[[], Awaitable[T]],
    *,
    signal: AbortSignal | None,
    policy: RetryPolicy,
) -> T:
    """Retry request establishment using SDK-compatible backoff semantics."""

    for attempt in range(policy.max_retries + 1):
        throw_if_aborted(signal)
        try:
            return await await_with_abort(operation(), signal)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if signal is not None and signal.aborted:
                signal.throw_if_aborted()
            error = provider_error(exc)
            if attempt >= policy.max_retries or not is_error_retryable(error):
                error.retryable_override = False
                raise error from exc
            delay = _retry_delay(error, attempt, policy)
            await await_with_abort(asyncio.sleep(delay), signal)
    raise RuntimeError("retry loop exhausted unexpectedly")


__all__ = ["RetryPolicy", "retry_provider_request"]
