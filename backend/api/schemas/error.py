"""Shared OpenAPI models for the API error envelope."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class ErrorDetailResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    message: str
    request_id: str
    details: dict[str, object] | None = None


class ErrorResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    error: ErrorDetailResponse


ERROR_RESPONSES: dict[int, dict[str, Any]] = {
    400: {"model": ErrorResponse, "description": "Invalid business request"},
    401: {"model": ErrorResponse, "description": "Authentication required"},
    403: {"model": ErrorResponse, "description": "Request forbidden"},
    404: {"model": ErrorResponse, "description": "Resource not found"},
    409: {"model": ErrorResponse, "description": "Resource state conflict"},
    422: {"model": ErrorResponse, "description": "Request validation failed"},
    429: {"model": ErrorResponse, "description": "Request rate limited"},
    502: {"model": ErrorResponse, "description": "Upstream integration failed"},
    503: {"model": ErrorResponse, "description": "Service unavailable"},
}


def error_responses(*status_codes: int) -> dict[int | str, dict[str, Any]]:
    """Return only the error statuses relevant to one router group."""

    unknown = set(status_codes).difference(ERROR_RESPONSES)
    if unknown:
        raise ValueError(f"unsupported documented error status: {sorted(unknown)}")
    return {status_code: ERROR_RESPONSES[status_code] for status_code in status_codes}
