"""Schedule/account handlers wired into the infrastructure JobRunner."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import date

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.business.account.service import AccountAppService
from backend.business.dreams import DreamService, DreamStatus
from backend.business.runs.commands import StartRunCommand
from backend.business.runs.service import RunService
from backend.business.schedules import StrategySchedule
from backend.business.shared.enums import TriggerSource

LeaseCheck = Callable[[], Awaitable[None]]


def build_market_analysis_handler(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    run_service_factory: Callable[[AsyncSession], RunService],
    enqueue_run: Callable[[int], Awaitable[None]] | None = None,
) -> Callable[[StrategySchedule, LeaseCheck], Awaitable[None]]:
    """Submit a durable run job; execution is done by the lease worker."""

    async def handler(schedule: StrategySchedule, lease_check: LeaseCheck) -> None:
        async with session_factory() as session:
            service = run_service_factory(session)
            created = await service.create_run(
                StartRunCommand(
                    trigger_source=TriggerSource.SCHEDULED,
                    schedule_id=schedule.schedule_id,
                ),
                execution_guard=lease_check,
            )
        if enqueue_run is not None:
            await lease_check()
            await enqueue_run(created.run_id)

    return handler


def build_memory_dream_handler(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    dream_service_factory: Callable[[AsyncSession], DreamService],
    enqueue_dream: Callable[[int], Awaitable[None]] | None = None,
) -> Callable[[date, LeaseCheck], Awaitable[None]]:
    """Create one target-date dream task and wake its independent worker."""

    async def handler(target_date: date, lease_check: LeaseCheck) -> None:
        async with session_factory() as session:
            dream = await dream_service_factory(session).create_or_get(
                target_date, execution_guard=lease_check
            )
        if dream.status is DreamStatus.PENDING and enqueue_dream is not None:
            await lease_check()
            await enqueue_dream(dream.task_id)

    return handler


def build_account_refresh_handler(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    account_service_factory: Callable[[AsyncSession], Awaitable[AccountAppService]],
) -> Callable[..., Awaitable[None]]:
    async def handler(lease_check: LeaseCheck) -> None:
        async with session_factory() as session:
            service = await account_service_factory(session)
            await service.refresh_account_cache(execution_guard=lease_check)

    return handler
