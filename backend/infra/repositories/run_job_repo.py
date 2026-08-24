"""Repository for durable run execution jobs (SQLite lease worker)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.business.runs.job import ACTIVE_JOB_STATUSES, RunJob, RunJobStatus
from backend.infra.db.models import RunJobModel, utc_now_iso


def _deserialize_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


class RunJobRepository:
    """Persistence adapter for run_jobs lease state."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_run_id(self, run_id: int) -> RunJob | None:
        statement = (
            select(RunJobModel)
            .where(RunJobModel.run_id == run_id)
            .execution_options(populate_existing=True)
        )
        model = await self._session.scalar(statement)
        if model is None:
            return None
        return self._to_domain(model)

    async def get_active_job(self) -> RunJob | None:
        statement = (
            select(RunJobModel)
            .where(RunJobModel.active_guard == 1)
            .order_by(RunJobModel.created_at.asc())
            .limit(1)
        )
        model = (await self._session.scalars(statement)).first()
        if model is None:
            return None
        return self._to_domain(model)

    async def create_pending(self, run_id: int) -> RunJob:
        now = utc_now_iso()
        model = RunJobModel(
            run_id=run_id,
            status=RunJobStatus.PENDING.value,
            attempt=0,
            available_at=now,
            active_guard=1,
            created_at=now,
            updated_at=now,
        )
        self._session.add(model)
        await self._session.flush()
        return self._to_domain(model)

    async def claim_next(
        self,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> RunJob | None:
        """Atomically claim one pending or expired active job."""

        clock = now or datetime.now(tz=UTC)
        now_iso = clock.isoformat()
        lease_until = (clock + timedelta(seconds=lease_seconds)).isoformat()
        claim_token = uuid.uuid4().hex

        claimable = or_(
            RunJobModel.status == RunJobStatus.PENDING.value,
            and_(
                RunJobModel.status.in_(
                    (
                        RunJobStatus.LEASED.value,
                        RunJobStatus.CANCEL_REQUESTED.value,
                    )
                ),
                RunJobModel.lease_expires_at.is_not(None),
                RunJobModel.lease_expires_at < now_iso,
            ),
        )
        candidate_stmt = (
            select(RunJobModel.run_id, RunJobModel.status)
            .where(
                claimable,
                RunJobModel.available_at <= now_iso,
            )
            .order_by(RunJobModel.available_at.asc(), RunJobModel.run_id.asc())
            .limit(1)
        )
        candidate = (await self._session.execute(candidate_stmt)).first()
        if candidate is None:
            return None
        run_id = int(candidate.run_id)
        claimed_status = (
            RunJobStatus.CANCEL_REQUESTED.value
            if candidate.status == RunJobStatus.CANCEL_REQUESTED.value
            else RunJobStatus.LEASED.value
        )

        result = await self._session.execute(
            update(RunJobModel)
            .where(
                RunJobModel.run_id == run_id,
                claimable,
            )
            .values(
                status=claimed_status,
                worker_id=worker_id,
                claim_token=claim_token,
                attempt=RunJobModel.attempt + 1,
                lease_expires_at=lease_until,
                heartbeat_at=now_iso,
                active_guard=1,
                updated_at=now_iso,
            )
        )
        if getattr(result, "rowcount", None) != 1:
            return None
        await self._session.flush()
        return await self.get_by_run_id(int(run_id))

    async def reclaim_expired(
        self,
        run_id: int,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> RunJob | None:
        """Take a one-time recovery claim without making the job runnable again."""

        existing = await self.get_by_run_id(run_id)
        if (
            existing is None
            or existing.status
            not in {RunJobStatus.LEASED, RunJobStatus.CANCEL_REQUESTED}
            or existing.lease_expires_at is None
        ):
            return None
        clock = now or datetime.now(tz=UTC)
        now_iso = clock.isoformat()
        if existing.lease_expires_at > clock:
            return None
        lease_until = (clock + timedelta(seconds=lease_seconds)).isoformat()
        claim_token = uuid.uuid4().hex
        result = await self._session.execute(
            update(RunJobModel)
            .where(
                RunJobModel.run_id == run_id,
                RunJobModel.status == existing.status.value,
                RunJobModel.lease_expires_at.is_not(None),
                RunJobModel.lease_expires_at <= now_iso,
            )
            .values(
                worker_id=worker_id,
                claim_token=claim_token,
                attempt=RunJobModel.attempt + 1,
                lease_expires_at=lease_until,
                heartbeat_at=now_iso,
                updated_at=now_iso,
            )
        )
        if getattr(result, "rowcount", None) != 1:
            return None
        await self._session.flush()
        return await self.get_by_run_id(run_id)

    async def heartbeat(
        self,
        run_id: int,
        *,
        worker_id: str,
        claim_token: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> RunJob | None:
        clock = now or datetime.now(tz=UTC)
        now_iso = clock.isoformat()
        lease_until = (clock + timedelta(seconds=lease_seconds)).isoformat()
        result = await self._session.execute(
            update(RunJobModel)
            .where(
                RunJobModel.run_id == run_id,
                RunJobModel.worker_id == worker_id,
                RunJobModel.claim_token == claim_token,
                RunJobModel.status.in_(
                    (
                        RunJobStatus.LEASED.value,
                        RunJobStatus.CANCEL_REQUESTED.value,
                    )
                ),
                RunJobModel.lease_expires_at.is_not(None),
                RunJobModel.lease_expires_at > now_iso,
            )
            .values(
                heartbeat_at=now_iso,
                lease_expires_at=lease_until,
                updated_at=now_iso,
            )
        )
        if getattr(result, "rowcount", None) != 1:
            return None
        await self._session.flush()
        return await self.get_by_run_id(run_id)

    async def request_cancel(
        self,
        run_id: int,
        *,
        reason: str,
        now: datetime | None = None,
    ) -> RunJob | None:
        clock = now or datetime.now(tz=UTC)
        now_iso = clock.isoformat()
        for _attempt in range(3):
            existing = await self.get_by_run_id(run_id)
            if existing is None or existing.status not in ACTIVE_JOB_STATUSES:
                return existing

            pending = existing.status is RunJobStatus.PENDING
            values: dict[str, object | None] = {
                "status": (
                    RunJobStatus.CANCELLED.value
                    if pending
                    else RunJobStatus.CANCEL_REQUESTED.value
                ),
                "cancel_reason": reason,
                "active_guard": None if pending else 1,
                "updated_at": now_iso,
            }
            if pending:
                values.update(
                    lease_expires_at=None,
                    worker_id=None,
                    claim_token=None,
                )
            result = await self._session.execute(
                update(RunJobModel)
                .where(
                    RunJobModel.run_id == run_id,
                    RunJobModel.status == existing.status.value,
                )
                .values(**values)
            )
            if getattr(result, "rowcount", None) == 1:
                await self._session.flush()
                return await self.get_by_run_id(run_id)
            self._session.expire_all()
        return await self.get_by_run_id(run_id)

    async def mark_terminal(
        self,
        run_id: int,
        *,
        status: RunJobStatus,
        worker_id: str,
        claim_token: str,
        error_code: str | None = None,
        error_message: str | None = None,
        require_leased: bool = False,
        now: datetime | None = None,
    ) -> RunJob | None:
        if status in ACTIVE_JOB_STATUSES:
            raise ValueError(f"status {status} is not terminal")
        clock = now or datetime.now(tz=UTC)
        now_iso = clock.isoformat()
        preview = None
        if error_message:
            preview = error_message[:500]
        source_statuses = (
            (RunJobStatus.LEASED.value,)
            if require_leased
            else (
                RunJobStatus.LEASED.value,
                RunJobStatus.CANCEL_REQUESTED.value,
            )
        )
        ownership_filters = [
            RunJobModel.worker_id == worker_id,
            RunJobModel.claim_token == claim_token,
            RunJobModel.status.in_(source_statuses),
            RunJobModel.lease_expires_at.is_not(None),
            RunJobModel.lease_expires_at > now_iso,
        ]
        result = await self._session.execute(
            update(RunJobModel)
            .where(
                RunJobModel.run_id == run_id,
                *ownership_filters,
            )
            .values(
                status=status.value,
                last_error_code=error_code,
                last_error_message_preview=preview,
                active_guard=None,
                lease_expires_at=None,
                worker_id=None,
                claim_token=None,
                updated_at=now_iso,
            )
        )
        if getattr(result, "rowcount", None) != 1:
            return None
        await self._session.flush()
        return await self.get_by_run_id(run_id)

    def _to_domain(self, model: RunJobModel) -> RunJob:
        return RunJob(
            run_id=model.run_id,
            status=RunJobStatus(model.status),
            attempt=model.attempt,
            worker_id=model.worker_id,
            claim_token=model.claim_token,
            lease_expires_at=_deserialize_datetime(model.lease_expires_at),
            heartbeat_at=_deserialize_datetime(model.heartbeat_at),
            available_at=(
                _deserialize_datetime(model.available_at) or datetime.now(tz=UTC)
            ),
            last_error_code=model.last_error_code,
            last_error_message_preview=model.last_error_message_preview,
            cancel_reason=model.cancel_reason,
            active_guard=model.active_guard,
            created_at=(
                _deserialize_datetime(model.created_at) or datetime.now(tz=UTC)
            ),
            updated_at=(
                _deserialize_datetime(model.updated_at) or datetime.now(tz=UTC)
            ),
        )
