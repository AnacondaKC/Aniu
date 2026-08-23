"""Shared HTTP error mapping for domain and application exceptions."""

from __future__ import annotations

from uuid import uuid4

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from backend.business.auth.service import ForbiddenError, UnauthorizedError
from backend.business.shared import (
    AccountRefreshThrottledError,
    ConcurrentRunError,
    ConfigurationConflictError,
    DomainError,
    RunAbortError,
    RunDeletionNotAllowedError,
    RunNotFoundError,
    ScheduleNotFoundError,
    ServiceConfigurationError,
    ServiceIntegrationError,
)


def _request_id(request: Request) -> str:
    header = request.headers.get("X-Request-ID") or request.headers.get("X-Request-Id")
    if header and header.strip():
        return header.strip()[:128]
    existing = getattr(request.state, "request_id", None)
    if isinstance(existing, str) and existing:
        return existing
    request_id = str(uuid4())
    request.state.request_id = request_id
    return request_id


def _error_body(
    code: str,
    message: str,
    *,
    request_id: str,
    details: dict[str, object] | None = None,
) -> dict[str, object]:
    error: dict[str, object] = {
        "code": code,
        "message": message,
        "request_id": request_id,
    }
    if details:
        error["details"] = details
    return {"error": error}


async def _domain_error_handler(request: Request, exc: DomainError) -> JSONResponse:
    request_id = _request_id(request)
    details: dict[str, object] | None = None
    if isinstance(exc, AccountRefreshThrottledError):
        status_code = status.HTTP_429_TOO_MANY_REQUESTS
    elif isinstance(exc, UnauthorizedError):
        status_code = status.HTTP_401_UNAUTHORIZED
    elif isinstance(exc, ForbiddenError):
        status_code = status.HTTP_403_FORBIDDEN
    elif isinstance(exc, (RunNotFoundError, ScheduleNotFoundError)):
        status_code = status.HTTP_404_NOT_FOUND
    elif isinstance(
        exc,
        (
            ConcurrentRunError,
            ConfigurationConflictError,
            RunDeletionNotAllowedError,
            RunAbortError,
        ),
    ):
        status_code = status.HTTP_409_CONFLICT
        if isinstance(exc, ConfigurationConflictError):
            details = {
                "resource": exc.resource,
                "expected_revision": exc.expected,
                "actual_revision": exc.actual,
            }
    elif isinstance(exc, ServiceConfigurationError):
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    elif isinstance(exc, ServiceIntegrationError):
        status_code = status.HTTP_502_BAD_GATEWAY
    else:
        status_code = status.HTTP_400_BAD_REQUEST
    response = JSONResponse(
        status_code=status_code,
        content=_error_body(
            type(exc).__name__,
            str(exc),
            request_id=request_id,
            details=details,
        ),
    )
    response.headers["X-Request-ID"] = request_id
    return response


async def _value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    request_id = _request_id(request)
    response = JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content=_error_body("ValidationError", str(exc), request_id=request_id),
    )
    response.headers["X-Request-ID"] = request_id
    return response


async def _request_validation_error_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    request_id = _request_id(request)
    response = JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content=_error_body(
            "RequestValidationError",
            "request validation failed",
            request_id=request_id,
            details={"errors": jsonable_encoder(exc.errors())},
        ),
    )
    response.headers["X-Request-ID"] = request_id
    return response


def register_exception_handlers(app: FastAPI) -> None:
    """Attach domain/application exception handlers to the FastAPI app."""

    app.add_exception_handler(DomainError, _domain_error_handler)  # type: ignore[arg-type]
    app.add_exception_handler(ValueError, _value_error_handler)  # type: ignore[arg-type]
    app.add_exception_handler(
        RequestValidationError,
        _request_validation_error_handler,  # type: ignore[arg-type]
    )
