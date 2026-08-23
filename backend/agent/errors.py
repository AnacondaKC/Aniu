"""Failures owned by the independent agent package."""

from __future__ import annotations

from enum import StrEnum


class AgentErrorCode(StrEnum):
    AUTHENTICATION = "authentication"
    RATE_LIMIT = "rate_limit"
    TIMEOUT = "timeout"
    NETWORK = "network"
    INVALID_RESPONSE = "invalid_response"
    CONTEXT_OVERFLOW = "context_overflow"
    PROVIDER_4XX = "provider_4xx"
    PROVIDER_5XX = "provider_5xx"
    EMPTY_RESPONSE = "empty_response"
    CONFIGURATION = "configuration"
    UNKNOWN = "unknown"


class AgentError(Exception):
    """Base failure raised by AgentHarness."""


class AgentConfigurationError(AgentError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = AgentErrorCode.CONFIGURATION


class AgentIntegrationError(AgentError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        error_code: AgentErrorCode = AgentErrorCode.UNKNOWN,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code


def is_retryable_agent_error(
    code: AgentErrorCode,
    status_code: int | None = None,
) -> bool:
    if code is AgentErrorCode.CONTEXT_OVERFLOW:
        return False
    return code in {
        AgentErrorCode.RATE_LIMIT,
        AgentErrorCode.TIMEOUT,
        AgentErrorCode.NETWORK,
        AgentErrorCode.PROVIDER_5XX,
    } or status_code in {408, 409, 425, 429}


__all__ = [
    "AgentConfigurationError",
    "AgentError",
    "AgentErrorCode",
    "AgentIntegrationError",
    "is_retryable_agent_error",
]
