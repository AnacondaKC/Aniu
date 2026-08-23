"""Schedule feature ports."""

from __future__ import annotations

from typing import Protocol

from backend.business.schedules.models import StrategySchedule


class ScheduleRepositoryPort(Protocol):
    async def list_schedules(self) -> list[StrategySchedule]: ...

    async def get_by_id(self, schedule_id: int) -> StrategySchedule | None: ...

    async def add(self, schedule: StrategySchedule) -> StrategySchedule: ...

    async def update_if_revision(
        self,
        schedule: StrategySchedule,
        *,
        expected_revision: int,
    ) -> StrategySchedule | None: ...

    async def save_sync_state_if_revision(
        self,
        schedule: StrategySchedule,
    ) -> StrategySchedule | None: ...

    async def get_revision(self, schedule_id: int) -> int | None: ...


class ScheduleRunnerPort(Protocol):
    async def sync_schedule(self, schedule: StrategySchedule) -> None: ...

    async def sync_all(self, schedules: list[StrategySchedule]) -> None: ...
