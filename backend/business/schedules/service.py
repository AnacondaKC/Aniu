"""Application service for strategy schedules."""

from __future__ import annotations

from datetime import UTC, datetime

from backend.business.schedules import ALLOWED_TASK_TYPES, StrategySchedule
from backend.business.schedules.commands import (
    CreateScheduleCommand,
    UpdateScheduleCommand,
)
from backend.business.schedules.dto import StrategyScheduleDTO, to_schedule_dto
from backend.business.schedules.ports import ScheduleRepositoryPort, ScheduleRunnerPort
from backend.business.shared import (
    CommitterPort,
    ConfigurationConflictError,
    ScheduleNotFoundError,
)


def _ensure_supported_task_type(task_type: str) -> None:
    if task_type not in ALLOWED_TASK_TYPES:
        raise ValueError(f"unsupported schedule task_type: {task_type}")


class ScheduleAppService:
    """Create, update, list, and synchronize strategy schedules."""

    def __init__(
        self,
        schedule_repo: ScheduleRepositoryPort,
        *,
        committer: CommitterPort | None = None,
        schedule_runner: ScheduleRunnerPort | None = None,
    ) -> None:
        self._schedule_repo = schedule_repo
        self._committer = committer
        self._schedule_runner = schedule_runner

    async def list_schedules(self) -> list[StrategyScheduleDTO]:
        schedules = await self._schedule_repo.list_schedules()
        return [to_schedule_dto(schedule) for schedule in schedules]

    async def create_schedule(
        self,
        command: CreateScheduleCommand,
    ) -> StrategyScheduleDTO:
        _ensure_supported_task_type(command.task_type)
        stored = await self._schedule_repo.add(
            StrategySchedule(
                enabled=command.enabled,
                interval_minutes=command.interval_minutes,
                custom_schedule_times=(
                    tuple(command.schedule_times)
                    if command.schedule_times is not None
                    else None
                ),
                revision=1,
            )
        )
        await self._commit()
        stored = await self._sync(stored)
        return to_schedule_dto(stored)

    async def update_schedule(
        self,
        command: UpdateScheduleCommand,
    ) -> StrategyScheduleDTO:
        current = await self._schedule_repo.get_by_id(command.schedule_id)
        if current is None:
            raise ScheduleNotFoundError(command.schedule_id)
        if (
            command.expected_revision is not None
            and command.expected_revision != current.revision
        ):
            raise ConfigurationConflictError(
                "strategy_schedule", command.expected_revision, current.revision
            )
        _ensure_supported_task_type(command.task_type)
        expected_revision = current.revision
        current.apply_update(
            enabled=command.enabled,
            interval_minutes=command.interval_minutes,
            custom_schedule_times=command.schedule_times,
        )
        stored = await self._schedule_repo.update_if_revision(
            current,
            expected_revision=expected_revision,
        )
        if stored is None:
            actual_revision = await self._schedule_repo.get_revision(
                command.schedule_id
            )
            if actual_revision is None:
                raise ScheduleNotFoundError(command.schedule_id)
            raise ConfigurationConflictError(
                "strategy_schedule", expected_revision, actual_revision
            )
        await self._commit()
        stored = await self._sync(stored)
        return to_schedule_dto(stored)

    async def _sync(self, schedule: StrategySchedule) -> StrategySchedule:
        if self._schedule_runner is None:
            return schedule
        try:
            await self._schedule_runner.sync_schedule(schedule)
            schedule.runtime_synced_revision = schedule.revision
            schedule.sync_error = None
        except Exception as exc:  # noqa: BLE001 - synchronization state is user-visible
            schedule.sync_error = str(exc)
        schedule.updated_at = datetime.now(tz=UTC)
        stored = await self._schedule_repo.save_sync_state_if_revision(schedule)
        await self._commit()
        return stored or schedule

    async def _commit(self) -> None:
        if self._committer is not None:
            await self._committer.commit()
