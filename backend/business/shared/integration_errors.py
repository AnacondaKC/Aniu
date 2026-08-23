"""Typed integration error codes for external providers."""

from __future__ import annotations

from enum import StrEnum


class IntegrationErrorCode(StrEnum):
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


def classify_http_status(status_code: int | None) -> IntegrationErrorCode:
    if status_code is None:
        return IntegrationErrorCode.NETWORK
    if status_code in {401, 403}:
        return IntegrationErrorCode.AUTHENTICATION
    if status_code == 408:
        return IntegrationErrorCode.TIMEOUT
    if status_code == 429:
        return IntegrationErrorCode.RATE_LIMIT
    if 500 <= status_code <= 599:
        return IntegrationErrorCode.PROVIDER_5XX
    if 400 <= status_code <= 499:
        return IntegrationErrorCode.PROVIDER_4XX
    return IntegrationErrorCode.UNKNOWN


def is_retryable(code: IntegrationErrorCode, status_code: int | None = None) -> bool:
    if code is IntegrationErrorCode.CONTEXT_OVERFLOW:
        return False
    if code in {
        IntegrationErrorCode.RATE_LIMIT,
        IntegrationErrorCode.TIMEOUT,
        IntegrationErrorCode.NETWORK,
        IntegrationErrorCode.PROVIDER_5XX,
    }:
        return True
    return status_code in {408, 409, 425, 429}


__all__ = ["IntegrationErrorCode", "classify_http_status", "is_retryable"]
