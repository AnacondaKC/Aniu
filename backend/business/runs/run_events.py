"""Typed run events and legacy loop signal names.

Typed events carry a common immutable envelope for traceability. ``RunEventType``
is retained only for the current agent-loop callback protocol while producers are
migrated to dataclass events.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


@dataclass(frozen=True, slots=True, kw_only=True)
class RunEvent:
    """Common immutable event envelope."""

    run_id: int
    event_id: str = field(default_factory=lambda: str(uuid4()))
    event_seq: int | None = None
    occurred_at: datetime = field(default_factory=_utc_now)
    correlation_id: str | None = None
    causation_id: str | None = None
    producer: str = "aniu-runtime"

    def __post_init__(self) -> None:
        if self.run_id <= 0:
            raise ValueError("run event run_id must be positive")
        if not self.event_id.strip():
            raise ValueError("run event event_id is required")
        if self.event_seq is not None and self.event_seq < 0:
            raise ValueError("run event event_seq must be non-negative")
        if not self.producer.strip():
            raise ValueError("run event producer is required")


@dataclass(frozen=True, slots=True)
class StageEntered(RunEvent):
    stage_name: str


@dataclass(frozen=True, slots=True)
class StageCompleted(RunEvent):
    stage_name: str
    summary: str | None = None


@dataclass(frozen=True, slots=True)
class StageSkipped(RunEvent):
    stage_name: str
    summary: str


@dataclass(frozen=True, slots=True)
class StageDegraded(RunEvent):
    stage_name: str
    summary: str


@dataclass(frozen=True, slots=True)
class StageFailed(RunEvent):
    stage_name: str
    summary: str | None = None


@dataclass(frozen=True, slots=True)
class ToolCallStarted(RunEvent):
    stage_name: str
    tool_call_id: str
    tool_name: str
    arguments: dict[str, Any] | None = None
    title: str | None = None
    summary: str | None = None


@dataclass(frozen=True, slots=True)
class ToolCallFinished(RunEvent):
    stage_name: str
    tool_call_id: str
    tool_name: str
    status: str
    arguments: dict[str, Any] | None = None
    error: str | None = None
    model_content_characters: int | None = None
    title: str | None = None
    summary: str | None = None
    stock_api_calls: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True, slots=True)
class TextDelta(RunEvent):
    stage_name: str
    step_id: str
    delta: str
    channel: str = "text"


TraceEvent = (
    StageEntered
    | StageCompleted
    | StageSkipped
    | StageDegraded
    | StageFailed
    | ToolCallStarted
    | ToolCallFinished
    | TextDelta
)


class RunEventType(StrEnum):
    """Legacy signal names used by the agent-loop callback adapter."""

    SUMMARY_GENERATION_STARTED = "SummaryGenerationStarted"
    TOOL_LOOP_STARTED = "ToolLoopStarted"
    CONTEXT_COMPACTED = "ContextCompacted"
    TOOL_CALL_REQUESTED = "ToolCallRequested"
    TOOL_CALL_COMPLETED = "ToolCallCompleted"
    TOOL_CALL_BLOCKED = "ToolCallBlocked"
    TOOL_CALL_FAILED = "ToolCallFailed"
    TOOL_LOOP_STOPPED = "ToolLoopStopped"
    TOOL_LOOP_FAILED = "ToolLoopFailed"


__all__ = [
    "RunEvent",
    "RunEventType",
    "StageCompleted",
    "StageDegraded",
    "StageEntered",
    "StageFailed",
    "StageSkipped",
    "TextDelta",
    "ToolCallFinished",
    "ToolCallStarted",
    "TraceEvent",
]
