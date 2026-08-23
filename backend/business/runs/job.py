"""Domain model for durable run execution jobs."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum


class RunJobStatus(StrEnum):
    PENDING = "PENDING"
    LEASED = "LEASED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    INTERRUPTED = "INTERRUPTED"


ACTIVE_JOB_STATUSES: frozenset[RunJobStatus] = frozenset(
    {
        RunJobStatus.PENDING,
        RunJobStatus.LEASED,
        RunJobStatus.CANCEL_REQUESTED,
    }
)


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


@dataclass(slots=True)
class RunJob:
    """One durable execution unit for a strategy run."""

    run_id: int
    status: RunJobStatus = RunJobStatus.PENDING
    attempt: int = 0
    worker_id: str | None = None
    claim_token: str | None = None
    lease_expires_at: datetime | None = None
    heartbeat_at: datetime | None = None
    available_at: datetime = field(default_factory=utc_now)
    last_error_code: str | None = None
    last_error_message_preview: str | None = None
    cancel_reason: str | None = None
    active_guard: int | None = 1
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
