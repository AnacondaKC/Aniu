"""API dependency adapters backed by the process runtime."""

from __future__ import annotations

from typing import Annotated, Protocol, cast

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.db import get_db_session, get_session_factory
from backend.business.account.service import AccountAppService
from backend.business.auth.service import AuthAppService
from backend.business.dreams.service import DreamService
from backend.business.market import MarketOverviewQueryPort
from backend.business.memories.service import MemoryService
from backend.business.runs.service import RunService
from backend.business.schedules.service import ScheduleAppService
from backend.business.settings.channels import ModelChannelService
from backend.business.settings.service import SettingsService
from backend.business.stock_api_logs.service import StockApiLogService

__all__ = ["get_db_session", "get_session_factory"]


class ApiRuntimePort(Protocol):
    run_worker: object | None
    dream_worker: object | None
    job_runner: object | None

    def run_service(self, session: AsyncSession) -> RunService: ...

    def auth_service(self, session: AsyncSession) -> AuthAppService: ...

    def memory_service(self, session: AsyncSession) -> MemoryService: ...

    def dream_query_service(self, session: AsyncSession) -> DreamService: ...

    async def account_service(self, session: AsyncSession) -> AccountAppService: ...

    def require_public_stock_data(self) -> object: ...

    def market_overview_query(self) -> MarketOverviewQueryPort: ...

    def settings_service(self, session: AsyncSession) -> SettingsService: ...

    def model_channel_service(self, session: AsyncSession) -> ModelChannelService: ...

    def schedule_service(self, session: AsyncSession) -> ScheduleAppService: ...

    def stock_api_log_service(self, session: AsyncSession) -> StockApiLogService: ...


def get_runtime(request: Request) -> ApiRuntimePort:
    runtime = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise RuntimeError("application runtime is not configured")
    return cast(ApiRuntimePort, runtime)


def get_run_worker(runtime: Annotated[ApiRuntimePort, Depends(get_runtime)]) -> object:
    if runtime.run_worker is None:
        raise RuntimeError("run worker is not initialized")
    return runtime.run_worker


def get_dream_worker(
    runtime: Annotated[ApiRuntimePort, Depends(get_runtime)],
) -> object:
    if runtime.dream_worker is None:
        raise RuntimeError("memory dream worker is not initialized")
    return runtime.dream_worker


def get_dream_service(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    runtime: Annotated[ApiRuntimePort, Depends(get_runtime)],
) -> DreamService:
    return runtime.dream_query_service(session)


def get_memory_service(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    runtime: Annotated[ApiRuntimePort, Depends(get_runtime)],
) -> MemoryService:
    return runtime.memory_service(session)


def get_run_service(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    runtime: Annotated[ApiRuntimePort, Depends(get_runtime)],
) -> RunService:
    return runtime.run_service(session)


async def get_account_app_service(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    runtime: Annotated[ApiRuntimePort, Depends(get_runtime)],
) -> AccountAppService:
    return await runtime.account_service(session)


def get_market_overview_service(
    runtime: Annotated[ApiRuntimePort, Depends(get_runtime)],
) -> MarketOverviewQueryPort:
    return runtime.market_overview_query()


def get_settings_service(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    runtime: Annotated[ApiRuntimePort, Depends(get_runtime)],
) -> SettingsService:
    return runtime.settings_service(session)


def get_model_channel_service(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    runtime: Annotated[ApiRuntimePort, Depends(get_runtime)],
) -> ModelChannelService:
    return runtime.model_channel_service(session)


def get_schedule_service(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    runtime: Annotated[ApiRuntimePort, Depends(get_runtime)],
) -> ScheduleAppService:
    return runtime.schedule_service(session)


def get_stock_api_log_service(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    runtime: Annotated[ApiRuntimePort, Depends(get_runtime)],
) -> StockApiLogService:
    return runtime.stock_api_log_service(session)
