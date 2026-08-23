"""Shared domain enums."""

from enum import StrEnum


class TriggerSource(StrEnum):
    """Supported strategy run trigger sources."""

    MANUAL = "manual"
    SCHEDULED = "scheduled"


class RunStatus(StrEnum):
    """Strategy run statuses."""

    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ABORTED = "ABORTED"


class RunState(StrEnum):
    """FSM states for the two-stage agent runtime."""

    RUN = "Run"
    SUMMARY = "Summary"
    COMPLETED = "Completed"
    FAILED = "Failed"
