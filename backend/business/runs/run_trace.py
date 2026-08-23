"""Run trace models used as the single runtime display record."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _normalize_mapping(value: object | None) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("trace mapping fields must be objects")
    return dict(value)


def _parse_datetime(value: object) -> datetime | None:
    if value in {None, ""}:
        return None
    return datetime.fromisoformat(str(value))


@dataclass(slots=True)
class TraceStep:
    step_id: str
    type: str
    title: str
    status: str = "pending"
    summary: str | None = None
    content: str | None = None
    data: dict[str, Any] | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.step_id:
            raise ValueError("step_id is required")
        if not self.type:
            raise ValueError("step type is required")
        if not self.title:
            raise ValueError("step title is required")
        if not self.status:
            raise ValueError("step status is required")
        self.data = _normalize_mapping(self.data)

    def as_dict(self) -> dict[str, Any]:
        return {
            "step_id": self.step_id,
            "type": self.type,
            "title": self.title,
            "status": self.status,
            "summary": self.summary,
            "content": self.content,
            "data": None if self.data is None else dict(self.data),
            "started_at": None
            if self.started_at is None
            else self.started_at.isoformat(),
            "ended_at": None if self.ended_at is None else self.ended_at.isoformat(),
        }

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> TraceStep:
        return cls(
            step_id=str(payload.get("step_id") or ""),
            type=str(payload.get("type") or ""),
            title=str(payload.get("title") or ""),
            status=str(payload.get("status") or "pending"),
            summary=None
            if payload.get("summary") is None
            else str(payload.get("summary")),
            content=None
            if payload.get("content") is None
            else str(payload.get("content")),
            data=_normalize_mapping(payload.get("data")),
            started_at=_parse_datetime(payload.get("started_at")),
            ended_at=_parse_datetime(payload.get("ended_at")),
        )


@dataclass(slots=True)
class TraceStage:
    stage_id: str
    key: str
    round: int | None
    title: str
    description: str
    status: str = "pending"
    summary: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    steps: list[TraceStep] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.stage_id:
            raise ValueError("stage_id is required")
        if not self.key:
            raise ValueError("stage key is required")
        if not self.title:
            raise ValueError("stage title is required")
        if not self.status:
            raise ValueError("stage status is required")
        self.steps = [
            step if isinstance(step, TraceStep) else TraceStep.from_mapping(step)
            for step in self.steps
        ]

    def as_dict(self) -> dict[str, Any]:
        return {
            "stage_id": self.stage_id,
            "key": self.key,
            "round": self.round,
            "title": self.title,
            "description": self.description,
            "status": self.status,
            "summary": self.summary,
            "started_at": None
            if self.started_at is None
            else self.started_at.isoformat(),
            "ended_at": None if self.ended_at is None else self.ended_at.isoformat(),
            "steps": [step.as_dict() for step in self.steps],
        }

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> TraceStage:
        round_value = payload.get("round")
        round_index = int(round_value) if isinstance(round_value, int) else None
        return cls(
            stage_id=str(payload.get("stage_id") or ""),
            key=str(payload.get("key") or ""),
            round=round_index,
            title=str(payload.get("title") or ""),
            description=str(payload.get("description") or ""),
            status=str(payload.get("status") or "pending"),
            summary=None
            if payload.get("summary") is None
            else str(payload.get("summary")),
            started_at=_parse_datetime(payload.get("started_at")),
            ended_at=_parse_datetime(payload.get("ended_at")),
            steps=[TraceStep.from_mapping(step) for step in payload.get("steps", [])],
        )


@dataclass(slots=True)
class RunTrace:
    schema_version: int = 3
    event_seq: int = 0
    current_stage_id: str | None = None
    stages: list[TraceStage] = field(default_factory=list)
    updated_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        if self.schema_version != 3:
            raise ValueError("unsupported run trace schema_version")
        if self.event_seq < 0:
            raise ValueError("run trace event_seq must be non-negative")
        self.stages = [
            stage if isinstance(stage, TraceStage) else TraceStage.from_mapping(stage)
            for stage in self.stages
        ]

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "event_seq": self.event_seq,
            "current_stage_id": self.current_stage_id,
            "stages": [stage.as_dict() for stage in self.stages],
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_mapping(cls, payload: dict[str, Any] | None) -> RunTrace:
        if not payload:
            return cls()
        return cls(
            schema_version=int(payload.get("schema_version", 3)),
            event_seq=int(payload.get("event_seq", 0)),
            current_stage_id=(
                None
                if payload.get("current_stage_id") in {None, ""}
                else str(payload.get("current_stage_id"))
            ),
            stages=[
                TraceStage.from_mapping(stage) for stage in payload.get("stages", [])
            ],
            updated_at=_parse_datetime(payload.get("updated_at")) or utc_now(),
        )


def empty_trace() -> RunTrace:
    return RunTrace()


__all__ = ["RunTrace", "TraceStage", "TraceStep", "empty_trace"]
