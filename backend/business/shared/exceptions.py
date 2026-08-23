"""Shared domain/application-facing exceptions."""

from __future__ import annotations

from backend.business.shared.integration_errors import (
    IntegrationErrorCode,
    classify_http_status,
)


class DomainError(Exception):
    """Base exception for domain-safe application errors."""


class AccountRefreshThrottledError(DomainError):
    """Raised when account data refreshes exceed the local frequency limit."""


class ConfigurationConflictError(DomainError):
    """Raised when a stale settings/profile revision is written."""

    def __init__(self, resource: str, expected: int, actual: int):
        super().__init__(
            f"stale {resource} revision: expected={expected}, actual={actual}"
        )
        self.resource = resource
        self.expected = expected
        self.actual = actual


class ConcurrentRunError(DomainError):
    """Raised when a new run is started while another run is active."""

    def __init__(self, running_run_id: int):
        super().__init__(f"another run is already active: run_id={running_run_id}")
        self.running_run_id = running_run_id


class RunNotFoundError(DomainError):
    """Raised when a strategy run cannot be found."""

    def __init__(self, run_id: int):
        super().__init__(f"strategy run not found: run_id={run_id}")
        self.run_id = run_id


class RunDeletionNotAllowedError(DomainError):
    """Raised when a run cannot be deleted safely."""

    def __init__(self, run_id: int, reason: str):
        super().__init__(
            f"strategy run cannot be deleted: run_id={run_id}, reason={reason}"
        )
        self.run_id = run_id
        self.reason = reason


class RunAbortError(DomainError):
    """Raised when an active run is aborted by user or system request."""

    def __init__(self, run_id: int):
        super().__init__(f"strategy run aborted: run_id={run_id}")
        self.run_id = run_id


class ServiceIntegrationError(DomainError):
    """Raised when an external service integration fails."""

    def __init__(
        self,
        *args: object,
        status_code: int | None = None,
        error_code: IntegrationErrorCode | str | None = None,
    ) -> None:
        super().__init__(*args)
        self.status_code = status_code
        if error_code is None:
            self.error_code = classify_http_status(status_code)
        elif isinstance(error_code, IntegrationErrorCode):
            self.error_code = error_code
        else:
            self.error_code = IntegrationErrorCode(error_code)


class ServiceConfigurationError(ServiceIntegrationError):
    """Raised when an integration is misconfigured."""

    def __init__(self, *args: object, status_code: int | None = None) -> None:
        super().__init__(
            *args,
            status_code=status_code,
            error_code=IntegrationErrorCode.CONFIGURATION,
        )


class ScheduleNotFoundError(DomainError):
    """Raised when a schedule cannot be found."""

    def __init__(self, schedule_id: int):
        super().__init__(f"schedule not found: schedule_id={schedule_id}")
        self.schedule_id = schedule_id
