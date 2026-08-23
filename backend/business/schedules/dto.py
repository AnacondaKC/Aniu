"""Schedule business read models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from backend.business.schedules.models import StrategySchedule


@dataclass(frozen=True, slots=True)
class StrategyScheduleDTO:
    schedule_id: int
    enabled: bool
    task_type: str
    interval_minutes: int
    custom_schedule_times: tuple[str, ...] | None
    schedule_times: tuple[str, ...]
    revision: int
    runtime_synced_revision: int
    sync_error: str | None
    updated_at: datetime


def to_schedule_dto(schedule: StrategySchedule) -> StrategyScheduleDTO:
    return StrategyScheduleDTO(
        schedule_id=schedule.schedule_id,
        enabled=schedule.enabled,
        task_type=schedule.task_type,
        interval_minutes=schedule.interval_minutes,
        custom_schedule_times=schedule.custom_schedule_times,
        schedule_times=schedule.schedule_times,
        revision=schedule.revision,
        runtime_synced_revision=schedule.runtime_synced_revision,
        sync_error=schedule.sync_error,
        updated_at=schedule.updated_at,
    )
