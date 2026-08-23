"""Repository for persisted strategy schedules."""

from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.business.runs import SCHEDULE_TASK_TYPE
from backend.business.schedules import StrategySchedule
from backend.infra.db.models import StrategyScheduleModel
from backend.infra.repositories.task_numbering import next_task_id


class ScheduleRepository:
    """Persistence adapter for strategy schedules."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def list_schedules(self) -> list[StrategySchedule]:
        statement = select(StrategyScheduleModel).order_by(
            StrategyScheduleModel.id.asc()
        )
        models = list((await self._session.scalars(statement)).all())
        return [self._to_domain(model) for model in models]

    async def get_by_id(self, schedule_id: int) -> StrategySchedule | None:
        model = await self._session.get(StrategyScheduleModel, schedule_id)
        return None if model is None else self._to_domain(model)

    async def add(self, schedule: StrategySchedule) -> StrategySchedule:
        if schedule.schedule_id > 0:
            raise ValueError("new schedule must not have an id")
        schedule_id = await next_task_id(
            self._session,
            schedule.updated_at.date(),
            task_type=SCHEDULE_TASK_TYPE,
        )
        model = StrategyScheduleModel(
            id=schedule_id,
            enabled=schedule.enabled,
            interval_minutes=schedule.interval_minutes,
            custom_schedule_times_json=_serialize_times(schedule.custom_schedule_times),
            revision=schedule.revision,
            runtime_synced_revision=schedule.runtime_synced_revision,
            sync_error=schedule.sync_error,
            updated_at=schedule.updated_at.isoformat(),
        )
        self._session.add(model)
        await self._session.flush()
        return self._to_domain(model)

    async def update_if_revision(
        self,
        schedule: StrategySchedule,
        *,
        expected_revision: int,
    ) -> StrategySchedule | None:
        statement = (
            update(StrategyScheduleModel)
            .where(
                StrategyScheduleModel.id == schedule.schedule_id,
                StrategyScheduleModel.revision == expected_revision,
            )
            .values(
                enabled=schedule.enabled,
                interval_minutes=schedule.interval_minutes,
                custom_schedule_times_json=_serialize_times(
                    schedule.custom_schedule_times
                ),
                revision=schedule.revision,
                runtime_synced_revision=schedule.runtime_synced_revision,
                sync_error=schedule.sync_error,
                updated_at=schedule.updated_at.isoformat(),
            )
            .returning(StrategyScheduleModel)
            .execution_options(populate_existing=True)
        )
        model = (await self._session.scalars(statement)).one_or_none()
        return None if model is None else self._to_domain(model)

    async def save_sync_state_if_revision(
        self,
        schedule: StrategySchedule,
    ) -> StrategySchedule | None:
        statement = (
            update(StrategyScheduleModel)
            .where(
                StrategyScheduleModel.id == schedule.schedule_id,
                StrategyScheduleModel.revision == schedule.revision,
            )
            .values(
                runtime_synced_revision=schedule.runtime_synced_revision,
                sync_error=schedule.sync_error,
                updated_at=schedule.updated_at.isoformat(),
            )
            .returning(StrategyScheduleModel)
            .execution_options(populate_existing=True)
        )
        model = (await self._session.scalars(statement)).one_or_none()
        return None if model is None else self._to_domain(model)

    async def get_revision(self, schedule_id: int) -> int | None:
        statement = select(StrategyScheduleModel.revision).where(
            StrategyScheduleModel.id == schedule_id
        )
        revision = await self._session.scalar(statement)
        return None if revision is None else int(revision)

    @staticmethod
    def _to_domain(model: StrategyScheduleModel) -> StrategySchedule:
        return StrategySchedule(
            schedule_id=model.id,
            enabled=model.enabled,
            interval_minutes=model.interval_minutes,
            custom_schedule_times=_parse_times(model.custom_schedule_times_json),
            revision=model.revision,
            runtime_synced_revision=model.runtime_synced_revision,
            sync_error=model.sync_error,
            updated_at=datetime.fromisoformat(model.updated_at),
        )


def _serialize_times(times: tuple[str, ...] | None) -> str | None:
    if times is None:
        return None
    return json.dumps(list(times))


def _parse_times(raw: str | None) -> tuple[str, ...] | None:
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        # Corrupt or legacy payloads degrade to the interval-derived schedule.
        return None
    if not isinstance(parsed, list):
        return None
    values = tuple(item for item in parsed if isinstance(item, str))
    return values if values else None
