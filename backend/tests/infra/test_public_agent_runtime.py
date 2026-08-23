"""Public stock-data registration within the Agent runtime factory."""

from __future__ import annotations

from typing import cast

import pytest

from backend.infra.integrations.agent_runner import _StageToolRegistry
from backend.infra.integrations.agent_runtime import AgentRuntimeFactory
from backend.stock_api.public import StockMarketDataService


@pytest.mark.asyncio
async def test_runtime_factory_registers_public_tools_without_mx_clients() -> None:
    registry = await AgentRuntimeFactory(
        public_stock_data=cast(StockMarketDataService, object())
    ).build_tool_registry()

    assert set(registry.list_tool_names()) == {
        "stock_quote",
        "query_kline",
        "stock_intraday",
        "stock_ranking",
        "stock_money_flow",
        "stock_fundamentals",
        "stock_research",
        "stock_news",
        "market_snapshot",
    }


@pytest.mark.asyncio
async def test_runtime_stage_tool_counts_stay_stable_after_kline_replacement() -> None:
    registry = await AgentRuntimeFactory(
        public_stock_data=cast(StockMarketDataService, object()),
        mx_research_client=object(),  # type: ignore[arg-type]
        mx_portfolio_client=object(),  # type: ignore[arg-type]
        mx_trading_client=object(),  # type: ignore[arg-type]
    ).build_tool_registry()

    kline_description = registry.get("query_kline").to_tool_definition()["description"]
    market_description = registry.get("query_market_data").to_tool_definition()[
        "description"
    ]
    assert "所有个股和指数 K 线查询" in kline_description
    assert "所有 K 线查询必须使用 query_kline" in market_description

    def names(stage: str) -> set[str]:
        stage_registry = _StageToolRegistry(
            registry,
            stage,
            run_id=20260816001,
            invocation_session_factory=None,
        )
        return {str(getattr(tool, "name", "")) for tool in stage_registry.list_tools()}

    run = names("Run")
    summary = names("Summary")

    assert len(run) == 18
    assert summary == set()
    assert "query_kline" in run
    assert {
        "market_snapshot",
        "portfolio_stock_snapshot",
        "stock_analysis",
        "industry_snapshot",
    } <= run
    assert "stock_kline" not in run
    assert {"trade", "cancel"} <= run
