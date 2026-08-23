"""LLM-specific failures and retry classification."""

from __future__ import annotations

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from enum import StrEnum
from typing import Any

import httpx


class LLMErrorCode(StrEnum):
    AUTHENTICATION = "authentication"
    RATE_LIMIT = "rate_limit"
    TIMEOUT = "timeout"
    NETWORK = "network"
    INVALID_RESPONSE = "invalid_response"
    PROVIDER_4XX = "provider_4xx"
    PROVIDER_5XX = "provider_5xx"
    EMPTY_RESPONSE = "empty_response"
    CONTEXT_OVERFLOW = "context_overflow"
    CONFIGURATION = "configuration"
    UNKNOWN = "unknown"


def classify_http_status(status_code: int | None) -> LLMErrorCode:
    if status_code is None:
        return LLMErrorCode.UNKNOWN
    if status_code in {401, 403}:
        return LLMErrorCode.AUTHENTICATION
    if status_code == 408:
        return LLMErrorCode.TIMEOUT
    if status_code == 429:
        return LLMErrorCode.RATE_LIMIT
    if 500 <= status_code <= 599:
        return LLMErrorCode.PROVIDER_5XX
    if 400 <= status_code <= 499:
        return LLMErrorCode.PROVIDER_4XX
    return LLMErrorCode.UNKNOWN


def is_retryable(code: LLMErrorCode, status_code: int | None = None) -> bool:
    if code is LLMErrorCode.CONTEXT_OVERFLOW:
        return False
    if code in {
        LLMErrorCode.RATE_LIMIT,
        LLMErrorCode.TIMEOUT,
        LLMErrorCode.NETWORK,
        LLMErrorCode.PROVIDER_5XX,
        LLMErrorCode.EMPTY_RESPONSE,
    }:
        return True
    return status_code in {408, 409, 425, 429}


def is_error_retryable(error: LLMIntegrationError) -> bool:
    if error.error_code is LLMErrorCode.CONTEXT_OVERFLOW:
        return False
    if error.retryable_override is not None:
        return error.retryable_override
    return is_retryable(error.error_code, error.status_code)


def _headers_from_exception(exc: BaseException) -> Any:
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None)
    if headers is not None:
        return headers
    return getattr(exc, "headers", None)


def _header(headers: Any, name: str) -> str | None:
    if headers is None:
        return None
    getter = getattr(headers, "get", None)
    if not callable(getter):
        return None
    value = getter(name)
    return str(value).strip() if value is not None else None


def _retry_override(exc: BaseException) -> bool | None:
    value = _header(_headers_from_exception(exc), "x-should-retry")
    if value == "true":
        return True
    if value == "false":
        return False
    return None


def _retry_after_seconds(exc: BaseException) -> float | None:
    headers = _headers_from_exception(exc)
    milliseconds = _header(headers, "retry-after-ms")
    if milliseconds:
        try:
            return max(0.0, float(milliseconds) / 1000.0)
        except ValueError:
            pass
    value = _header(headers, "retry-after")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            moment = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
        return max(0.0, (moment - datetime.now(tz=UTC)).total_seconds())


class LLMIntegrationError(Exception):
    """Normalized provider failure safe for policy and API adapters."""

    def __init__(
        self,
        *args: object,
        status_code: int | None = None,
        error_code: LLMErrorCode | str | None = None,
        retry_after: float | None = None,
        retryable_override: bool | None = None,
    ) -> None:
        super().__init__(*args)
        self.status_code = status_code
        self.error_code = (
            classify_http_status(status_code)
            if error_code is None
            else LLMErrorCode(error_code)
        )
        self.retry_after = retry_after
        self.retryable_override = retryable_override


class LLMConfigurationError(LLMIntegrationError):
    def __init__(self, *args: object, status_code: int | None = None) -> None:
        super().__init__(
            *args,
            status_code=status_code,
            error_code=LLMErrorCode.CONFIGURATION,
        )


def invalid_response(
    message: str, *, cause: BaseException | None = None
) -> LLMIntegrationError:
    error = LLMIntegrationError(message, error_code=LLMErrorCode.INVALID_RESPONSE)
    if cause is not None:
        error.__cause__ = cause
    return error


def empty_response(message: str = "llm returned empty response") -> LLMIntegrationError:
    return LLMIntegrationError(message, error_code=LLMErrorCode.EMPTY_RESPONSE)


def _error_text(exc: BaseException) -> str:
    parts = [str(exc)]
    body = getattr(exc, "body", None)
    if body is not None:
        parts.append(str(body))
    return " ".join(parts).lower()


def _structured_error_values(exc: BaseException) -> tuple[str, ...]:
    body = getattr(exc, "body", None)
    if not isinstance(body, dict):
        return ()
    values: list[str] = []
    for value in (body.get("code"), body.get("type")):
        if isinstance(value, str):
            values.append(value.lower())
    nested = body.get("error")
    if isinstance(nested, dict):
        for value in (nested.get("code"), nested.get("type")):
            if isinstance(value, str):
                values.append(value.lower())
    return tuple(values)


def _semantic_error_code(
    exc: BaseException,
    *,
    status_code: int | None,
) -> LLMErrorCode | None:
    text = _error_text(exc)
    structured = _structured_error_values(exc)
    authentication_codes = {
        "authentication_error",
        "invalid_api_key",
        "permission_error",
        "invalid_api_key_error",
    }
    rate_limit_codes = {"rate_limit_error", "rate_limit_exceeded"}
    context_codes = {
        "context_length_exceeded",
        "model_context_window_exceeded",
        "context_window_exceeded",
    }

    if (
        status_code in {401, 403}
        or any(code in authentication_codes for code in structured)
        or "authentication_error" in text
        or "permission_error" in text
    ):
        return LLMErrorCode.AUTHENTICATION
    if status_code == 429 or any(code in rate_limit_codes for code in structured):
        return LLMErrorCode.RATE_LIMIT
    if "rate_limit_error" in text:
        return LLMErrorCode.RATE_LIMIT
    if any(code in {"overloaded_error", "overloaded"} for code in structured):
        return LLMErrorCode.PROVIDER_5XX
    if "overloaded_error" in text or "overloaded" in text:
        return LLMErrorCode.PROVIDER_5XX
    if status_code is not None and not 400 <= status_code <= 499:
        return None
    if any(code in context_codes for code in structured):
        return LLMErrorCode.CONTEXT_OVERFLOW
    if any(
        marker in text
        for marker in (
            "context_length_exceeded",
            "context length exceeded",
            "maximum context length",
            "model_context_window_exceeded",
            "prompt too long; exceeded",
            "too large for model with",
            "exceed context limit",
        )
    ):
        return LLMErrorCode.CONTEXT_OVERFLOW
    return None


def provider_error(exc: BaseException) -> LLMIntegrationError:
    """Normalize OpenAI, Anthropic, and httpx failures without SDK coupling."""

    if isinstance(exc, LLMIntegrationError):
        return exc
    if isinstance(exc, (httpx.TimeoutException, TimeoutError)):
        return LLMIntegrationError(
            str(exc) or type(exc).__name__,
            error_code=LLMErrorCode.TIMEOUT,
        )
    if isinstance(exc, httpx.TransportError):
        return LLMIntegrationError(
            str(exc) or type(exc).__name__,
            error_code=LLMErrorCode.NETWORK,
        )

    status = getattr(exc, "status_code", None)
    status_code = status if isinstance(status, int) else None
    if status_code is None:
        response = getattr(exc, "response", None)
        response_status = getattr(response, "status_code", None)
        status_code = response_status if isinstance(response_status, int) else None

    message = str(exc).strip() or type(exc).__name__
    code = _semantic_error_code(exc, status_code=status_code) or classify_http_status(
        status_code
    )
    class_name = type(exc).__name__.lower()
    if "timeout" in class_name:
        code = LLMErrorCode.TIMEOUT
    elif "connection" in class_name:
        code = LLMErrorCode.NETWORK
    return LLMIntegrationError(
        message,
        status_code=status_code,
        error_code=code,
        retry_after=_retry_after_seconds(exc),
        retryable_override=_retry_override(exc),
    )


__all__ = [
    "LLMConfigurationError",
    "LLMErrorCode",
    "LLMIntegrationError",
    "classify_http_status",
    "empty_response",
    "invalid_response",
    "is_error_retryable",
    "is_retryable",
    "provider_error",
]
