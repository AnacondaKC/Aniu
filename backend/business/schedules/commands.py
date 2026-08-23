"""Schedule commands."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CreateScheduleCommand:
    task_type: str
    interval_minutes: int
    enabled: bool = True
    schedule_times: list[str] | None = None


@dataclass(frozen=True, slots=True)
class UpdateScheduleCommand:
    schedule_id: int
    task_type: str
    interval_minutes: int
    enabled: bool = True
    expected_revision: int | None = None
    schedule_times: list[str] | None = None
