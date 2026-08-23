"""Single process runtime and composition root."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from backend.bootstrap.runtime_config import RuntimeConfig
from backend.business.account.runtime import AccountRefreshGate
from backend.business.account.service import AccountAppService
from backend.business.auth.service import AuthAppService, AuthLoginThrottle
from backend.business.dreams.service import DreamService
from backend.business.market import MarketOverviewQueryPort
from backend.business.memories.service import MemoryService
from backend.business.runs.abort_registry import ActiveRunAbortRegistry
from backend.business.runs.executor import RunExecutor
from backend.business.runs.service import RunService
from backend.business.schedules.service import ScheduleAppService
from backend.business.settings.channels import ModelChannelService
from backend.business.settings.resolver import ModelSelectionResolver
from backend.business.settings.service import SettingsService
from backend.business.stock_api_logs.models import StockApiToolCall
from backend.business.stock_api_logs.service import StockApiLogService
from backend.infra.calendar import TradingCalendar2026, is_market_session_open
from backend.infra.integrations.agent_runner import AgentRunnerFactoryAdapter
from backend.infra.integrations.agent_runtime import AgentRuntimeFactory
from backend.infra.integrations.dream_agent import DreamAgentRunner
from backend.infra.repositories import (
    AccountCacheRepository,
    MemoryDreamRepository,
    MemoryRepository,
    ModelProfileRepository,
    RunJobRepository,
    RunRepository,
    ScheduleRepository,
    SelectedModelRepository,
    SettingsRepository,
    StockApiCallLogRecord,
    StockApiCallLogRepository,
)
from backend.infra.repositories.auth_repo import (
    AuthSessionRepository,
    LocalIdentityRepository,
)
from backend.infra.security.password_hasher import hash_password, verify_password
from backend.stock_api import MxClients
from backend.stock_api.public import PublicMarketOverviewQuery, StockMarketDataService

if TYPE_CHECKING:
    from backend.api.sse import StreamHub
    from backend.infra.integrations.models_dev_catalog import ModelsDevCatalog
    from backend.infra.scheduler import JobRunner
    from backend.infra.workers.memory_dream_worker import MemoryDreamWorker
    from backend.infra.workers.run_worker import RunWorker
    from backend.llm import LLMClient, ModelConnectivityTester


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AppRuntime:
    """All process-scoped state plus request-scope service factories."""

    config: RuntimeConfig
    abort_registry: ActiveRunAbortRegistry = field(
        default_factory=ActiveRunAbortRegistry
    )
    account_refresh_gate: AccountRefreshGate = field(default_factory=AccountRefreshGate)
    auth_login_throttle: AuthLoginThrottle = field(default_factory=AuthLoginThrottle)
    engine: AsyncEngine | None = None
    session_factory: async_sessionmaker[AsyncSession] | None = None
    stream_hub: StreamHub | None = None
    llm_client: LLMClient | None = None
    model_connectivity_tester: ModelConnectivityTester | None = None
    run_worker: RunWorker | None = None
    dream_worker: MemoryDreamWorker | None = None
    job_runner: JobRunner | None = None
    mx_http_client: httpx.AsyncClient | None = None
    mx_clients: MxClients | None = None
    public_stock_http_client: httpx.AsyncClient | None = None
    public_stock_data: StockMarketDataService | None = None
    stock_api_log_write_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        repr=False,
    )
    models_dev_catalog: ModelsDevCatalog | None = None

    def require_session_factory(self) -> async_sessionmaker[AsyncSession]:
        if self.session_factory is None:
            raise RuntimeError("database session factory is not initialized")
        return self.session_factory

    def require_stream_hub(self) -> StreamHub:
        if self.stream_hub is None:
            raise RuntimeError("stream hub is not initialized")
        return self.stream_hub

    def require_llm_client(self) -> LLMClient:
        if self.llm_client is None:
            raise RuntimeError("LLM client is not initialized")
        return self.llm_client

    def require_model_connectivity_tester(self) -> ModelConnectivityTester:
        if self.model_connectivity_tester is None:
            raise RuntimeError("model connectivity tester is not initialized")
        return self.model_connectivity_tester

    def require_job_runner(self) -> JobRunner:
        if self.job_runner is None:
            raise RuntimeError("job runner is not initialized")
        return self.job_runner

    def require_mx_http_client(self) -> httpx.AsyncClient:
        if self.mx_http_client is None:
            raise RuntimeError("MX HTTP client is not initialized")
        return self.mx_http_client

    async def _resolve_mx_api_key(self) -> str | None:
        async with self.require_session_factory()() as session:
            settings = await SettingsRepository(session).get()
            return settings.mx_api_key if settings is not None else None

    async def _record_stock_api_tool_call(self, call: StockApiToolCall) -> None:
        try:
            # SQLite has one writer. Agent tool calls can run in parallel, so
            # their log commits share one process-local writer gate.
            async with self.stock_api_log_write_lock:
                async with self.require_session_factory()() as session:
                    await StockApiCallLogRepository(session).append(
                        StockApiCallLogRecord(
                            tool_source=call.tool_source,
                            tool_id=call.tool_id,
                            parameters=call.parameters,
                            status=call.status,
                            duration_ms=call.duration_ms,
                            response_characters=call.response_characters,
                            error_category=call.error_category,
                            error_message=call.error_message,
                        )
                    )
                    await session.commit()
        except Exception:
            logger.exception(
                "failed to persist data-interface tool invocation log",
                extra={"tool_source": call.tool_source, "tool_id": call.tool_id},
            )

    def require_mx_clients(self) -> MxClients:
        if self.mx_clients is None:
            self.mx_clients = MxClients.create(
                api_key_resolver=self._resolve_mx_api_key,
                http_client=self.require_mx_http_client(),
            )
        return self.mx_clients

    def require_public_stock_data(self) -> StockMarketDataService:
        if self.public_stock_data is None:
            if self.public_stock_http_client is None:
                raise RuntimeError("public stock HTTP client is not initialized")
            self.public_stock_data = StockMarketDataService.create(
                http_client=self.public_stock_http_client,
            )
        return self.public_stock_data

    def market_overview_query(self) -> MarketOverviewQueryPort:
        return PublicMarketOverviewQuery(self.require_public_stock_data())

    def stock_api_log_service(self, session: AsyncSession) -> StockApiLogService:
        return StockApiLogService(StockApiCallLogRepository(session))

    def memory_service(self, session: AsyncSession) -> MemoryService:
        return MemoryService(MemoryRepository(session), committer=session)

    def auth_service(self, session: AsyncSession) -> AuthAppService:
        return AuthAppService(
            identity_repo=LocalIdentityRepository(session),
            session_repo=AuthSessionRepository(session),
            password_hasher=hash_password,
            password_verifier=verify_password,
            login_throttle=self.auth_login_throttle,
            committer=session,
        )

    def settings_service(self, session: AsyncSession) -> SettingsService:
        return SettingsService(
            settings_repo=SettingsRepository(session),
            model_profile_repo=ModelProfileRepository(session),
            selected_model_repo=SelectedModelRepository(session),
            committer=session,
        )

    def model_channel_service(self, session: AsyncSession) -> ModelChannelService:
        profiles = ModelProfileRepository(session)
        selected = SelectedModelRepository(session)
        return ModelChannelService(
            settings_repo=SettingsRepository(session),
            model_profile_repo=profiles,
            selected_model_repo=selected,
            model_resolver=ModelSelectionResolver(
                model_profile_repo=profiles,
                selected_model_repo=selected,
            ),
            model_connectivity_tester=self.require_model_connectivity_tester(),
            models_dev_catalog=self.models_dev_catalog,
            committer=session,
        )

    def schedule_service(self, session: AsyncSession) -> ScheduleAppService:
        return ScheduleAppService(
            schedule_repo=ScheduleRepository(session),
            committer=session,
            schedule_runner=self.require_job_runner(),
        )

    async def account_service(self, session: AsyncSession) -> AccountAppService:
        return AccountAppService(
            portfolio_client=self.require_mx_clients().portfolio,
            account_cache_repo=AccountCacheRepository(session),
            committer=session,
            trading_calendar=TradingCalendar2026(),
            refresh_gate=self.account_refresh_gate,
        )

    def run_service(self, session: AsyncSession) -> RunService:
        stream_hub = self.require_stream_hub()
        return RunService(
            run_repo=RunRepository(session),
            settings_repo=SettingsRepository(session),
            model_profile_repo=ModelProfileRepository(session),
            selected_model_repo=SelectedModelRepository(session),
            run_job_repo=RunJobRepository(session),
            abort_registry=self.abort_registry,
            committer=session,
            snapshot_publisher=stream_hub.publish_snapshot,
        )

    def dream_query_service(self, session: AsyncSession) -> DreamService:
        return DreamService(
            repository=MemoryDreamRepository(session),
            committer=session,
        )

    def dream_service(self, session: AsyncSession) -> DreamService:
        profiles = ModelProfileRepository(session)
        selected = SelectedModelRepository(session)
        return DreamService(
            repository=MemoryDreamRepository(session),
            agent=DreamAgentRunner(
                llm_client=self.require_llm_client(),
                runtime_factory=AgentRuntimeFactory(
                    model_profile_repo=profiles,
                    selected_model_repo=selected,
                    session_factory=self.require_session_factory(),
                ),
                settings_repo=SettingsRepository(session),
                session_factory=self.require_session_factory(),
            ),
            committer=session,
        )

    def run_executor(self, session: AsyncSession) -> RunExecutor:
        profiles = ModelProfileRepository(session)
        selected = SelectedModelRepository(session)
        stream_hub = self.require_stream_hub()
        clients = self.require_mx_clients()
        agent_factory = AgentRunnerFactoryAdapter(
            self.require_llm_client(),
            AgentRuntimeFactory(
                model_profile_repo=profiles,
                selected_model_repo=selected,
                mx_research_client=clients.research,
                mx_portfolio_client=clients.portfolio,
                mx_trading_client=clients.trading,
                public_stock_data=self.public_stock_data,
                session_factory=self.require_session_factory(),
            ),
            invocation_session_factory=self.require_session_factory(),
            stock_api_tool_call_logger=self._record_stock_api_tool_call,
        )
        return RunExecutor(
            run_repo=RunRepository(session),
            committer=session,
            snapshot_publisher=stream_hub.publish_snapshot,
            trace_step_delta_publisher=stream_hub.publish_trace_step_delta,
            agent_runner_factory=agent_factory,
            market_session_is_open=is_market_session_open,
            abort_registry=self.abort_registry,
        )
