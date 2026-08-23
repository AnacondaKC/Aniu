from __future__ import annotations

import pytest

from backend.business.runs import StageModelSnapshot, StrategySnapshot
from backend.business.settings import ModelProfile, SelectedModel, StageSettings
from backend.infra.integrations.agent_runtime import AgentRuntimeFactory
from backend.llm import ModelProtocol


class SelectedModels:
    def __init__(self) -> None:
        self.items = {
            1: SelectedModel(
                selected_model_id=1,
                channel_profile_id=10,
                model_name="run-model",
                label="Run",
                context_window_tokens=256_000,
                max_output_tokens=256_000,
            ),
            2: SelectedModel(
                selected_model_id=2,
                channel_profile_id=20,
                model_name="summary-model",
                label="Summary",
            ),
        }

    async def get_by_id(self, model_id: int) -> SelectedModel | None:
        return self.items.get(model_id)


class ModelProfiles:
    def __init__(self) -> None:
        self.items = {
            10: ModelProfile(
                profile_id=10,
                name="Run channel",
                protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
                model_name="run-model",
                base_url="https://run.example/v1",
                api_key="run-key",
            ),
            20: ModelProfile(
                profile_id=20,
                name="Summary channel",
                protocol=ModelProtocol.CLAUDE_API,
                model_name="summary-model",
                base_url="https://summary.example/v1",
                api_key="summary-key",
            ),
        }

    async def get_by_id(self, profile_id: int) -> ModelProfile | None:
        return self.items.get(profile_id)


@pytest.mark.asyncio
async def test_stage_runtime_uses_stage_model_parameters() -> None:
    factory = AgentRuntimeFactory(
        model_profile_repo=ModelProfiles(),
        selected_model_repo=SelectedModels(),
    )

    run_runtime = await factory.build_stage_runtime(
        StageSettings(
            stage_id="Run",
            model_selected_model_id=1,
            temperature=0.2,
            top_p=0.8,
            thinking_effort="high",
            prompt="分析需求",
        )
    )
    summary_runtime = await factory.build_stage_runtime(
        StageSettings(
            stage_id="Summary",
            model_selected_model_id=2,
            temperature=0.1,
            top_p=1,
            prompt="执行任务",
        )
    )

    assert (
        run_runtime.model,
        run_runtime.protocol,
        run_runtime.temperature,
        run_runtime.randomness,
    ) == (
        "run-model",
        ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        0.2,
        0.8,
    )
    assert run_runtime.thinking_effort == "high"
    assert run_runtime.context_window_tokens == 256_000
    assert run_runtime.max_output_tokens == 256_000
    assert (
        summary_runtime.model,
        summary_runtime.protocol,
    ) == (
        "summary-model",
        ModelProtocol.CLAUDE_API,
    )


@pytest.mark.asyncio
async def test_stage_runtime_uses_frozen_snapshot_routing() -> None:
    profiles = ModelProfiles()
    factory = AgentRuntimeFactory(
        model_profile_repo=profiles,
        selected_model_repo=SelectedModels(),
    )
    settings = StageSettings(
        stage_id="Run",
        model_selected_model_id=1,
        temperature=0.2,
        top_p=0.8,
        thinking_effort="high",
        prompt="分析需求",
    )
    snapshot = StrategySnapshot(
        prompt_version="v1",
        risk_rules_version="risk-v1",
        stage_settings={"Run": settings},
        stage_models={
            "Run": StageModelSnapshot(
                stage_id="Run",
                selected_model_id=1,
                channel_profile_id=10,
                channel_revision=1,
                credential_owner_created_at=profiles.items[10].created_at.isoformat(),
                protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS.value,
                base_url="https://frozen.example/v1",
                model_name="frozen-model",
                context_window_tokens=128_000,
                max_output_tokens=16_000,
            )
        },
    )

    profiles.items[10].api_key = "rotated-key"
    runtime = await factory.build_stage_runtime_from_snapshot(
        settings,
        snapshot.stage_models["Run"],
    )

    assert runtime.protocol is ModelProtocol.OPENAI_CHAT_COMPLETIONS
    assert runtime.base_url == "https://frozen.example/v1"
    assert runtime.model == "frozen-model"
    assert runtime.api_key == "rotated-key"
    assert runtime.context_window_tokens == 128_000
    assert runtime.max_output_tokens == 16_000


@pytest.mark.asyncio
async def test_runtime_factory_registers_only_direct_mx_tools() -> None:
    registry = await AgentRuntimeFactory(
        mx_research_client=object(),  # type: ignore[arg-type]
        mx_portfolio_client=object(),  # type: ignore[arg-type]
        mx_trading_client=object(),  # type: ignore[arg-type]
    ).build_tool_registry()

    assert set(registry.list_tool_names()) == {
        "query_market_data",
        "search_news",
        "select_stocks",
        "query_portfolio",
        "trade",
        "cancel",
    }


@pytest.mark.asyncio
async def test_runtime_factory_has_no_tools_without_integrations() -> None:
    registry = await AgentRuntimeFactory().build_tool_registry()

    assert registry.list_tool_names() == []
