"""Shared database-backed leases for schedulers and background workers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.infra.db.models import TaskLeaseModel


def _updated_one(result: object) -> bool:
    return int(getattr(result, "rowcount", 0)) == 1


class TaskLeaseRepository:
    """Acquire and fence short-lived ownership claims in a shared database."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def try_acquire(
        self, *, lease_key: str, owner_id: str, lease_seconds: float
    ) -> bool:
        now = datetime.now(tz=UTC)
        now_iso = now.isoformat()
        expires_iso = (now + timedelta(seconds=lease_seconds)).isoformat()
        result = await self._session.execute(
            update(TaskLeaseModel)
            .where(
                TaskLeaseModel.lease_key == lease_key,
                or_(
                    TaskLeaseModel.lease_expires_at <= now_iso,
                    TaskLeaseModel.owner_id == owner_id,
                ),
            )
            .values(
                owner_id=owner_id,
                lease_expires_at=expires_iso,
                updated_at=now_iso,
            )
        )
        if _updated_one(result):
            return True

        self._session.add(
            TaskLeaseModel(
                lease_key=lease_key,
                owner_id=owner_id,
                lease_expires_at=expires_iso,
                updated_at=now_iso,
            )
        )
        try:
            await self._session.flush()
        except IntegrityError:
            await self._session.rollback()
            return False
        return True

    async def renew(
        self, *, lease_key: str, owner_id: str, lease_seconds: float
    ) -> bool:
        now = datetime.now(tz=UTC)
        result = await self._session.execute(
            update(TaskLeaseModel)
            .where(
                TaskLeaseModel.lease_key == lease_key,
                TaskLeaseModel.owner_id == owner_id,
                TaskLeaseModel.lease_expires_at > now.isoformat(),
            )
            .values(
                lease_expires_at=(now + timedelta(seconds=lease_seconds)).isoformat(),
                updated_at=now.isoformat(),
            )
        )
        return _updated_one(result)

    async def is_active(self, *, lease_key: str) -> bool:
        expires_at = await self._session.scalar(
            select(TaskLeaseModel.lease_expires_at).where(
                TaskLeaseModel.lease_key == lease_key
            )
        )
        if not isinstance(expires_at, str):
            return False
        return expires_at > datetime.now(tz=UTC).isoformat()

    async def is_owned(self, *, lease_key: str, owner_id: str) -> bool:
        expires_at = await self._session.scalar(
            select(TaskLeaseModel.lease_expires_at).where(
                TaskLeaseModel.lease_key == lease_key,
                TaskLeaseModel.owner_id == owner_id,
            )
        )
        if not isinstance(expires_at, str):
            return False
        return expires_at > datetime.now(tz=UTC).isoformat()

    async def release(self, *, lease_key: str, owner_id: str) -> None:
        await self._session.execute(
            delete(TaskLeaseModel).where(
                TaskLeaseModel.lease_key == lease_key,
                TaskLeaseModel.owner_id == owner_id,
            )
        )


__all__ = ["TaskLeaseRepository"]
