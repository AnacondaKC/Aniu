"""Persistence adapter for nightly memory-maintenance tasks."""

from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.business.dreams import DREAM_TASK_TYPE, DreamStatus, MemoryDream
from backend.infra.db.models import MemoryDreamModel, TaskLeaseModel
from backend.infra.repositories.task_numbering import next_task_id


class MemoryDreamRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def next_task_id(self, reference_date: date) -> int:
        return await next_task_id(
            self._session,
            reference_date,
            task_type=DREAM_TASK_TYPE,
        )

    async def get_by_id(self, task_id: int) -> MemoryDream | None:
        model = await self._session.get(MemoryDreamModel, task_id)
        return None if model is None else _to_domain(model)

    async def delete(self, task_id: int) -> bool:
        result = await self._session.execute(
            delete(MemoryDreamModel).where(MemoryDreamModel.id == task_id)
        )
        await self._session.flush()
        return int(getattr(result, "rowcount", 0)) == 1

    async def get_by_date(self, target_date: date) -> MemoryDream | None:
        model = await self._session.scalar(
            select(MemoryDreamModel).where(
                MemoryDreamModel.target_date == target_date.isoformat()
            )
        )
        return None if model is None else _to_domain(model)

    async def add(self, dream: MemoryDream) -> MemoryDream:
        model = _to_model(dream)
        self._session.add(model)
        await self._session.flush()
        return _to_domain(model)

    async def save(self, dream: MemoryDream) -> MemoryDream:
        model = await self._session.get(MemoryDreamModel, dream.task_id)
        if model is None:
            return await self.add(dream)
        model.target_date = dream.target_date.isoformat()
        model.status = dream.status.value
        model.result = dream.result
        model.failure_reason = dream.failure_reason
        model.created_at = dream.created_at.isoformat()
        model.started_at = _serialize_datetime(dream.started_at)
        model.completed_at = _serialize_datetime(dream.completed_at)
        await self._session.flush()
        return _to_domain(model)

    async def save_fenced(
        self,
        dream: MemoryDream,
        *,
        lease_key: str,
        owner_id: str,
        expected_status: DreamStatus,
    ) -> bool:
        lease_exists = (
            select(TaskLeaseModel.lease_key)
            .where(
                TaskLeaseModel.lease_key == lease_key,
                TaskLeaseModel.owner_id == owner_id,
                TaskLeaseModel.lease_expires_at > datetime.now(tz=UTC).isoformat(),
            )
            .exists()
        )
        result = await self._session.execute(
            update(MemoryDreamModel)
            .where(
                MemoryDreamModel.id == dream.task_id,
                MemoryDreamModel.status == expected_status.value,
                lease_exists,
            )
            .values(
                target_date=dream.target_date.isoformat(),
                status=dream.status.value,
                result=dream.result,
                failure_reason=dream.failure_reason,
                created_at=dream.created_at.isoformat(),
                started_at=_serialize_datetime(dream.started_at),
                completed_at=_serialize_datetime(dream.completed_at),
            )
        )
        await self._session.flush()
        return int(getattr(result, "rowcount", 0)) == 1

    async def list_recent(self, *, limit: int, offset: int) -> list[MemoryDream]:
        statement = (
            select(MemoryDreamModel)
            .order_by(
                MemoryDreamModel.target_date.desc(),
                MemoryDreamModel.id.desc(),
            )
            .limit(limit)
            .offset(offset)
        )
        models = (await self._session.scalars(statement)).all()
        return [_to_domain(model) for model in models]

    async def count(self) -> int:
        result = await self._session.execute(select(func.count(MemoryDreamModel.id)))
        return int(result.scalar_one())

    async def list_pending(self, *, limit: int = 256) -> list[MemoryDream]:
        statement = (
            select(MemoryDreamModel)
            .where(MemoryDreamModel.status == DreamStatus.PENDING.value)
            .order_by(MemoryDreamModel.target_date.asc(), MemoryDreamModel.id.asc())
            .limit(limit)
        )
        models = (await self._session.scalars(statement)).all()
        return [_to_domain(model) for model in models]

    async def list_running(self) -> list[MemoryDream]:
        statement = (
            select(MemoryDreamModel)
            .where(MemoryDreamModel.status == DreamStatus.RUNNING.value)
            .order_by(MemoryDreamModel.target_date.asc(), MemoryDreamModel.id.asc())
        )
        models = (await self._session.scalars(statement)).all()
        return [_to_domain(model) for model in models]


def _serialize_datetime(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _to_model(dream: MemoryDream) -> MemoryDreamModel:
    return MemoryDreamModel(
        id=dream.task_id,
        target_date=dream.target_date.isoformat(),
        status=dream.status.value,
        result=dream.result,
        failure_reason=dream.failure_reason,
        created_at=dream.created_at.isoformat(),
        started_at=_serialize_datetime(dream.started_at),
        completed_at=_serialize_datetime(dream.completed_at),
    )


def _to_domain(model: MemoryDreamModel) -> MemoryDream:
    return MemoryDream(
        task_id=model.id,
        target_date=date.fromisoformat(model.target_date),
        status=DreamStatus(model.status),
        result=model.result,
        failure_reason=model.failure_reason,
        created_at=datetime.fromisoformat(model.created_at),
        started_at=(
            None
            if model.started_at is None
            else datetime.fromisoformat(model.started_at)
        ),
        completed_at=(
            None
            if model.completed_at is None
            else datetime.fromisoformat(model.completed_at)
        ),
    )


__all__ = ["MemoryDreamRepository"]
