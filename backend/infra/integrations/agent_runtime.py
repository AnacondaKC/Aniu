"""Assemble MX tools and LLM runtimes for one strategy run."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.agent.kernel.context_budget import (
    DEFAULT_CONTEXT_WINDOW_TOKENS,
    DEFAULT_MAX_OUTPUT_TOKENS,
)
from backend.agent.kernel.runtime_config import LlmRuntimeConfig
from backend.agent.tools import ToolRegistry
from backend.business.runs import StageModelSnapshot, StrategySnapshot
from backend.business.settings import STRATEGY_STAGE_IDS, StageSettings
from backend.infra.integrations.aggregate_stock_agent_tools import (
    register_aggregate_stock_tools,
)
from backend.infra.integrations.kline_agent_tool import QueryKlineTool
from backend.infra.integrations.memory_agent_tools import (
    MemoryReadTool,
    MemoryWriteTool,
)
from backend.infra.integrations.mx_agent_tools import register_mx_tools
from backend.infra.integrations.public_stock_agent_tools import (
    register_public_stock_tools,
)
from backend.llm import ModelProtocol, ModelProviderConfig
from backend.stock_api import MxMoniClient, MxPaperTradingClient, MxResearchClient
from backend.stock_api.public import StockMarketDataService


class AgentRuntimeFactory:
    """Build per-run MX tooling and model runtimes without owning lifecycle."""

    def __init__(
        self,
        *,
        model_profile_repo: object | None = None,
        selected_model_repo: object | None = None,
        mx_research_client: MxResearchClient | None = None,
        mx_portfolio_client: MxMoniClient | None = None,
        mx_trading_client: MxPaperTradingClient | None = None,
        public_stock_data: StockMarketDataService | None = None,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
    ) -> None:
        self._model_profile_repo = model_profile_repo
        self._selected_model_repo = selected_model_repo
        self._mx_research_client = mx_research_client
        self._mx_portfolio_client = mx_portfolio_client
        self._mx_trading_client = mx_trading_client
        self._public_stock_data = public_stock_data
        self._session_factory = session_factory

    async def build_tool_registry(self) -> ToolRegistry:
        registry = ToolRegistry()
        if self._session_factory is not None:
            registry.register(MemoryReadTool(self._session_factory))
            registry.register(MemoryWriteTool(self._session_factory))
        if self._public_stock_data is not None:
            register_public_stock_tools(registry, service=self._public_stock_data)
            registry.register(QueryKlineTool(public_service=self._public_stock_data))
            register_aggregate_stock_tools(
                registry,
                public_data=self._public_stock_data,
                research=self._mx_research_client,
                portfolio=self._mx_portfolio_client,
            )
        if (
            self._mx_research_client is not None
            and self._mx_portfolio_client is not None
            and self._mx_trading_client is not None
        ):
            register_mx_tools(
                registry,
                research=self._mx_research_client,
                portfolio=self._mx_portfolio_client,
                trading=self._mx_trading_client,
            )
        return registry

    async def build_stage_runtimes(
        self,
        snapshot: StrategySnapshot,
    ) -> dict[str, LlmRuntimeConfig]:
        """Resolve one model runtime per snapshot stage from selected channels."""

        runtimes: dict[str, LlmRuntimeConfig] = {}
        for stage_id in STRATEGY_STAGE_IDS:
            stage_settings = snapshot.stage_settings[stage_id]
            frozen_model = snapshot.stage_models.get(stage_id)
            runtime = (
                await self.build_stage_runtime_from_snapshot(
                    stage_settings,
                    frozen_model,
                )
                if frozen_model is not None
                else await self.build_stage_runtime(stage_settings)
            )
            runtimes[stage_id] = runtime
        return runtimes

    async def build_stage_runtime_from_snapshot(
        self,
        settings: StageSettings,
        model: StageModelSnapshot,
    ) -> LlmRuntimeConfig:
        """Use frozen routing values while resolving the current stored secret."""

        get_profile = getattr(self._model_profile_repo, "get_by_id", None)
        if not callable(get_profile):
            raise ValueError("model profile repository is not configured")
        profile = await get_profile(model.channel_profile_id)
        if (
            profile is None
            or profile.created_at.isoformat() != model.credential_owner_created_at
            or not profile.api_key
        ):
            raise ValueError(f"{settings.stage_id} model credential is unavailable")
        context_window_tokens = (
            model.context_window_tokens or DEFAULT_CONTEXT_WINDOW_TOKENS
        )
        return LlmRuntimeConfig(
            protocol=ModelProtocol(model.protocol),
            base_url=model.base_url,
            api_key=profile.api_key,
            model=model.model_name,
            provider_config=ModelProviderConfig.from_json(model.provider_config_json),
            temperature=settings.temperature,
            randomness=settings.top_p,
            thinking_effort=settings.thinking_effort,
            context_window_tokens=context_window_tokens,
            max_output_tokens=min(
                model.max_output_tokens or DEFAULT_MAX_OUTPUT_TOKENS,
                context_window_tokens,
            ),
        )

    async def build_stage_runtime(
        self,
        settings: StageSettings,
    ) -> LlmRuntimeConfig:
        if settings.model_selected_model_id is None:
            raise ValueError(f"{settings.stage_id} stage model is not configured")
        selected_repo = self._selected_model_repo
        profile_repo = self._model_profile_repo
        get_selected = getattr(selected_repo, "get_by_id", None)
        get_profile = getattr(profile_repo, "get_by_id", None)
        if not callable(get_selected) or not callable(get_profile):
            raise ValueError("model repositories are not configured")
        selected = await get_selected(settings.model_selected_model_id)
        if selected is None:
            raise ValueError(f"{settings.stage_id} selected model does not exist")
        profile = await get_profile(selected.channel_profile_id)
        if (
            profile is None
            or not profile.enabled
            or not profile.base_url
            or not profile.api_key
        ):
            raise ValueError(f"{settings.stage_id} model channel is unavailable")
        context_window_tokens = (
            selected.context_window_tokens or DEFAULT_CONTEXT_WINDOW_TOKENS
        )
        return LlmRuntimeConfig(
            protocol=ModelProtocol(profile.protocol),
            base_url=profile.base_url,
            api_key=profile.api_key,
            model=selected.model_name,
            provider_config=profile.provider_config,
            temperature=settings.temperature,
            randomness=settings.top_p,
            thinking_effort=settings.thinking_effort,
            context_window_tokens=context_window_tokens,
            max_output_tokens=min(
                selected.max_output_tokens or DEFAULT_MAX_OUTPUT_TOKENS,
                context_window_tokens,
            ),
        )
