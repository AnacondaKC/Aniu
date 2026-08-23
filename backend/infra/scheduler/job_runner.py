"""APScheduler-based job runner for persisted schedules."""

# mypy: disable-error-code=import-untyped

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.business.schedules import StrategySchedule
from backend.business.settings.models import (
    DEFAULT_DREAM_SCHEDULE_TIME,
    normalize_dream_schedule_time,
)
from backend.business.shared import (
    AccountRefreshThrottledError,
    ServiceIntegrationError,
)
from backend.infra.calendar import is_trading_day
from backend.infra.lease_guard import LeaseLostError, run_while_lease_valid
from backend.infra.repositories.task_lease_repo import TaskLeaseRepository

logger = logging.getLogger(__name__)
MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")
ACCOUNT_REFRESH_JOB_ID = "account-cache:market-hours"
MEMORY_DREAM_JOB_ID = "memory-dream:nightly"
SCHEDULER_MEMORY_DREAM_LEASE_KEY = "scheduler:memory-dream"
SCHEDULER_ACCOUNT_REFRESH_LEASE_KEY = "scheduler:account-refresh"
SCHEDULER_SCHEDULE_LEASE_PREFIX = "scheduler:strategy-schedule:"
SCHEDULER_LEASE_SECONDS = 60.0
SCHEDULER_LEASE_RENEW_SECONDS = 20.0

LeaseCheck = Callable[[], Awaitable[None]]
MarketAnalysisHandler = Callable[[StrategySchedule, LeaseCheck], Awaitable[None]]
MemoryDreamHandler = Callable[[date, LeaseCheck], Awaitable[None]]
AccountRefreshHandler = Callable[[LeaseCheck], Awaitable[None]]


class JobRunner:
    """Synchronize persisted schedules with APScheduler and execute jobs.

    Application use-cases are injected as handlers so this infrastructure
    module never imports application services.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        market_analysis_handler: MarketAnalysisHandler | None = None,
        memory_dream_handler: MemoryDreamHandler | None = None,
        account_refresh_handler: AccountRefreshHandler | None = None,
        scheduler: AsyncIOScheduler | None = None,
        enabled: bool = True,
    ) -> None:
        self._session_factory = session_factory
        self._scheduler = scheduler or AsyncIOScheduler(timezone=UTC)
        self._owner_id = f"scheduler-{uuid.uuid4().hex}"
        self._lease_locks: dict[str, asyncio.Lock] = {}
        self._enabled = enabled
        self._market_analysis_handler = market_analysis_handler
        self._memory_dream_handler = memory_dream_handler
        self._account_refresh_handler = account_refresh_handler

    @property
    def enabled(self) -> bool:
        """Whether this runner is allowed to register or run scheduled work."""

        return self._enabled

    async def start(self) -> None:
        if not self._enabled:
            return
        if not self._scheduler.running:
            self._scheduler.start()

    async def shutdown(self) -> None:
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)

    async def _run_as_leader(
        self,
        handler: Callable[[LeaseCheck], Awaitable[None]],
        *,
        lease_key: str,
        task_name: str,
    ) -> None:
        lease_lock = self._lease_locks.setdefault(lease_key, asyncio.Lock())
        async with lease_lock:
            owner_id = f"{self._owner_id}:{uuid.uuid4().hex}"
            async with self._session_factory() as session:
                lease_repo = TaskLeaseRepository(session)
                acquired = await lease_repo.try_acquire(
                    lease_key=lease_key,
                    owner_id=owner_id,
                    lease_seconds=SCHEDULER_LEASE_SECONDS,
                )
                if not acquired:
                    logger.debug("another process owns scheduler lease; skip tick")
                    return
                await session.commit()

            async def ensure_lease() -> None:
                async with self._session_factory() as session:
                    owned = await TaskLeaseRepository(session).is_owned(
                        lease_key=lease_key,
                        owner_id=owner_id,
                    )
                if not owned:
                    raise LeaseLostError(f"scheduler lease lost before {task_name}")

            stop_renewal = asyncio.Event()
            lease_lost = asyncio.Event()
            renewal_task = asyncio.create_task(
                self._renew_leader_lease(lease_key, owner_id, stop_renewal, lease_lost),
                name=f"aniu-scheduler-lease-renewal:{lease_key}",
            )
            try:
                await run_while_lease_valid(
                    lambda: handler(ensure_lease),
                    lease_lost=lease_lost,
                    task_name=task_name,
                )
            finally:
                stop_renewal.set()
                renewal_task.cancel()
                try:
                    await renewal_task
                except asyncio.CancelledError:
                    pass
                async with self._session_factory() as session:
                    await TaskLeaseRepository(session).release(
                        lease_key=lease_key,
                        owner_id=owner_id,
                    )
                    await session.commit()

    async def _renew_leader_lease(
        self,
        lease_key: str,
        owner_id: str,
        stop: asyncio.Event,
        lease_lost: asyncio.Event,
    ) -> None:
        while True:
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=SCHEDULER_LEASE_RENEW_SECONDS
                )
                return
            except TimeoutError:
                pass
            try:
                async with self._session_factory() as session:
                    renewed = await TaskLeaseRepository(session).renew(
                        lease_key=lease_key,
                        owner_id=owner_id,
                        lease_seconds=SCHEDULER_LEASE_SECONDS,
                    )
                    await session.commit()
                if not renewed:
                    logger.warning(
                        "scheduler lease was lost",
                        extra={"lease_key": lease_key},
                    )
                    lease_lost.set()
                    return
            except Exception:
                logger.exception("failed to renew scheduler leader lease")
                lease_lost.set()
                return

    async def sync_all(self, schedules: list[StrategySchedule]) -> None:
        if not self._enabled:
            logger.info("scheduler disabled; skip syncing %d schedules", len(schedules))
            return
        await self.start()
        for schedule in schedules:
            await self.sync_schedule(schedule)

    async def sync_memory_dream_job(
        self, schedule_time: str = DEFAULT_DREAM_SCHEDULE_TIME
    ) -> datetime | None:
        if not self._enabled:
            return None
        normalized_time = normalize_dream_schedule_time(schedule_time)
        hour, minute = normalized_time.split(":")
        await self.start()
        self._scheduler.add_job(
            self.trigger_memory_dream,
            trigger=CronTrigger(
                hour=int(hour),
                minute=int(minute),
                timezone=MARKET_TIMEZONE,
            ),
            id=MEMORY_DREAM_JOB_ID,
            replace_existing=True,
            coalesce=True,
            max_instances=1,
        )
        job = self._scheduler.get_job(MEMORY_DREAM_JOB_ID)
        return None if job is None else job.next_run_time

    async def trigger_memory_dream(self) -> None:
        if not self._enabled or self._memory_dream_handler is None:
            return
        target_date = self._now_market_time().date() - timedelta(days=1)
        await self._run_as_leader(
            lambda lease_check: self._trigger_memory_dream(target_date, lease_check),
            lease_key=SCHEDULER_MEMORY_DREAM_LEASE_KEY,
            task_name="aniu-memory-dream-scheduler-handler",
        )

    async def _trigger_memory_dream(
        self, target_date: date, lease_check: LeaseCheck
    ) -> None:
        if self._memory_dream_handler is None:
            return
        await self._memory_dream_handler(target_date, lease_check)

    async def sync_account_refresh_job(self) -> datetime | None:
        if not self._enabled:
            return None
        await self.start()
        self._scheduler.add_job(
            self.refresh_account_cache_now,
            trigger=CronTrigger(
                day_of_week="mon-fri",
                hour="9-15",
                minute="0",
                timezone=MARKET_TIMEZONE,
            ),
            id=ACCOUNT_REFRESH_JOB_ID,
            replace_existing=True,
            coalesce=True,
        )
        job = self._scheduler.get_job(ACCOUNT_REFRESH_JOB_ID)
        return None if job is None else job.next_run_time

    async def sync_schedule(self, schedule: StrategySchedule) -> None:
        if not self._enabled:
            return
        await self.start()
        await self._remove_schedule_jobs(schedule.schedule_id)
        if not schedule.enabled:
            return
        for index, schedule_time in enumerate(schedule.schedule_times):
            hour, minute = schedule_time.split(":")
            self._scheduler.add_job(
                self.trigger_schedule,
                trigger=CronTrigger(
                    day_of_week="mon-fri",
                    hour=int(hour),
                    minute=int(minute),
                    timezone=MARKET_TIMEZONE,
                ),
                args=[schedule.schedule_id],
                id=self._job_id(schedule.schedule_id, index),
                replace_existing=True,
                coalesce=True,
            )

    async def trigger_schedule(self, schedule_id: int) -> None:
        if not self._enabled:
            logger.info("scheduler disabled; skip schedule_id=%s", schedule_id)
            return
        await self._run_as_leader(
            lambda lease_check: self._trigger_schedule(schedule_id, lease_check),
            lease_key=f"{SCHEDULER_SCHEDULE_LEASE_PREFIX}{schedule_id}",
            task_name=f"aniu-schedule-handler:{schedule_id}",
        )

    async def _trigger_schedule(
        self, schedule_id: int, lease_check: LeaseCheck
    ) -> None:
        # Read the schedule in a short-lived session so we never hold a DB
        # connection while ``start_run`` waits on the process-wide run lock.
        from backend.infra.repositories import ScheduleRepository

        async with self._session_factory() as session:
            schedule = await ScheduleRepository(session).get_by_id(schedule_id)

        if schedule is None or not schedule.enabled:
            return

        if schedule.task_type == "market_analysis":
            await self._run_market_analysis(schedule, lease_check)
        else:
            # Fail closed: unsupported types must never silently no-op.
            logger.error(
                "unsupported schedule task_type refused: schedule_id=%s task_type=%s",
                schedule.schedule_id,
                schedule.task_type,
            )
            raise ValueError(f"unsupported schedule task_type: {schedule.task_type}")

    async def refresh_account_cache_now(self) -> None:
        if not self._enabled:
            logger.info("scheduler disabled; skip account cache refresh")
            return
        await self._run_as_leader(
            self._refresh_account_cache_now,
            lease_key=SCHEDULER_ACCOUNT_REFRESH_LEASE_KEY,
            task_name="aniu-account-refresh-scheduler-handler",
        )

    async def _refresh_account_cache_now(self, lease_check: LeaseCheck) -> None:
        if not is_trading_day(self._now_market_time().date()):
            logger.info("skip automatic account refresh on non-trading day")
            return

        if self._account_refresh_handler is None:
            logger.warning("account refresh handler is not configured; skip")
            return
        try:
            await self._account_refresh_handler(lease_check)
        except AccountRefreshThrottledError:
            logger.info("account cache refresh skipped by local frequency protection")
        except ServiceIntegrationError:
            logger.exception("account cache refresh failed")

    def _now_market_time(self) -> datetime:
        return datetime.now(tz=MARKET_TIMEZONE)

    async def _run_market_analysis(
        self, schedule: StrategySchedule, lease_check: LeaseCheck
    ) -> None:
        if self._market_analysis_handler is None:
            logger.warning(
                "market analysis handler is not configured; skip schedule_id=%s",
                schedule.schedule_id,
            )
            return
        await self._market_analysis_handler(schedule, lease_check)

    async def _remove_schedule_jobs(self, schedule_id: int) -> None:
        prefix = f"strategy-schedule:{schedule_id}:"
        for job in self._scheduler.get_jobs():
            if job.id == f"strategy-schedule:{schedule_id}" or job.id.startswith(
                prefix
            ):
                try:
                    self._scheduler.remove_job(job.id)
                except JobLookupError:
                    continue

    def _job_id(self, schedule_id: int, index: int) -> str:
        return f"strategy-schedule:{schedule_id}:{index}"
