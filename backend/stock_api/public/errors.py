"""Errors exposed by the normalized public stock-data service."""

from __future__ import annotations

from typing import Literal

type ErrorCategory = Literal[
    "timeout",
    "network",
    "rate_limited",
    "upstream_http",
    "invalid_response",
    "business_failure",
    "cancelled",
    "unknown",
]


class PublicStockDataError(Exception):
    """Base error with a stable, caller-safe error code."""

    code = "upstream_unavailable"
    retryable = False

    def __init__(
        self,
        message: str,
        *,
        retryable: bool | None = None,
        error_category: ErrorCategory | None = None,
    ) -> None:
        super().__init__(message)
        if retryable is not None:
            self.retryable = retryable
        self.error_category = error_category


class InvalidStockRequest(PublicStockDataError):
    code = "invalid_request"


class UnsupportedStockRequest(PublicStockDataError):
    code = "not_supported"


class UpstreamUnavailable(PublicStockDataError):
    code = "upstream_unavailable"
    retryable = True


class NoStockData(PublicStockDataError):
    code = "no_data"
    retryable = False


__all__ = [
    "ErrorCategory",
    "NoStockData",
    "PublicStockDataError",
    "UnsupportedStockRequest",
    "UpstreamUnavailable",
]
