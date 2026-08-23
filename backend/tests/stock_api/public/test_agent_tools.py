"""Agent contracts for the normalized public stock-data tools."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import cast

import pytest

from backend.agent.tools import ToolRegistry
from backend.infra.integrations.public_stock_agent_tools import (
    register_public_stock_tools,
)
from backend.infra.integrations.tool_policy import SideEffectLevel
from backend.llm.providers.extract import _claude_tools_spec, _openai_tool_spec
from backend.stock_api.public import (
    IntradayRequest,
    PublicStockRequest,
    QuoteSnapshotRequest,
    StockMarketDataService,
)


@dataclass
class RecordingService:
    requests: list[PublicStockRequest] = field(default_factory=list)

    async def execute(
        self, request: PublicStockRequest, _: object | None = None
    ) -> dict[str, object]:
        self.requests.append(request)
        return {"data": {"operation": request.operation}, "meta": {"source": "test"}}


def _registry() -> tuple[ToolRegistry, RecordingService]:
    service = RecordingService()
    registry = ToolRegistry()
    register_public_stock_tools(
        registry,
        service=cast(StockMarketDataService, service),
    )
    return registry, service


def test_public_stock_tools_have_closed_schemas_without_source_controls() -> None:
    registry, _ = _registry()

    assert set(registry.list_tool_names()) == {
        "stock_quote",
        "stock_intraday",
        "stock_ranking",
        "stock_money_flow",
        "stock_fundamentals",
        "stock_research",
        "stock_news",
    }
    for tool in registry.list_tools():
        definition = tool.to_tool_definition()
        parameters = definition["parameters"]
        branches = parameters.get("oneOf", [parameters])
        for branch in branches:
            assert branch["additionalProperties"] is False
            assert "source" not in branch["properties"]
        assert tool.side_effect_level is SideEffectLevel.READ
        assert tool.enabled_stages == ("Run",)


def test_schema_splits_incompatible_modes_and_keeps_symbol_contract() -> None:
    registry, _ = _registry()

    def branches(tool_name: str) -> list[dict[str, object]]:
        tool = next(tool for tool in registry.list_tools() if tool.name == tool_name)
        return tool.to_tool_definition()["parameters"]["oneOf"]

    fundamentals = branches("stock_fundamentals")
    valuation = next(
        branch
        for branch in fundamentals
        if branch["properties"]["action"].get("const") == "valuation"
    )
    assert "mode" not in valuation["properties"]
    assert not any(
        branch["properties"]["action"].get("const") == "financials"
        and branch["properties"].get("mode", {}).get("const") == "latest"
        and "page" in branch["properties"]
        for branch in fundamentals
    )

    research = branches("stock_research")
    assert not any(
        branch["properties"]["action"].get("const") == "forecast"
        and branch["properties"].get("mode", {}).get("const") == "summary"
        and "limit" in branch["properties"]
        for branch in research
    )
    full_report = next(
        branch
        for branch in research
        if branch["properties"].get("content", {}).get("const") == "full"
    )
    assert "report_id" in full_report["required"]

    symbol_pattern = next(
        tool for tool in registry.list_tools() if tool.name == "stock_intraday"
    ).to_tool_definition()["parameters"]["properties"]["symbol"]["pattern"]
    assert re.fullmatch(symbol_pattern, "600519")
    assert re.fullmatch(symbol_pattern, "000001")
    assert re.fullmatch(symbol_pattern, "000001.SZ")
    assert re.fullmatch(symbol_pattern, "000001.SH") is None


def test_action_branches_and_provider_converters_preserve_the_same_schema() -> None:
    registry, _ = _registry()
    definitions = [
        tool.to_tool_definition()
        for tool in registry.list_tools()
        if tool.name
        in {
            "stock_ranking",
            "stock_money_flow",
            "stock_fundamentals",
            "stock_research",
            "stock_news",
        }
    ]
    for definition in definitions:
        branches = definition["parameters"]["oneOf"]
        for branch in branches:
            assert branch["properties"]["action"].get("const") is not None
            assert branch["additionalProperties"] is False

    quote = next(tool for tool in registry.list_tools() if tool.name == "stock_quote")
    quote_definition = quote.to_tool_definition()
    openai = _openai_tool_spec(quote_definition)
    claude = _claude_tools_spec([quote_definition])[0]
    assert openai["function"]["parameters"] == quote_definition["parameters"]
    assert claude["input_schema"] == quote_definition["parameters"]


@pytest.mark.asyncio
async def test_intraday_remains_an_independent_agent_action() -> None:
    registry, service = _registry()

    intraday = await registry.call(
        "stock_intraday",
        symbol="600519.SH",
        days=1,
    )

    assert intraday == {
        "data": {"operation": "chart.intraday"},
        "meta": {"source": "test"},
    }
    assert isinstance(service.requests[0], IntradayRequest)
    assert [request.operation for request in service.requests] == ["chart.intraday"]


@pytest.mark.asyncio
async def test_agent_tools_complete_inferable_exchange_suffixes() -> None:
    registry, service = _registry()

    await registry.call(
        "stock_quote",
        symbols=["600519", "000001"],
        detail="full",
    )
    await registry.call("stock_intraday", symbol="688981", days=1)

    quote = service.requests[0]
    intraday = service.requests[1]
    assert isinstance(quote, QuoteSnapshotRequest)
    assert quote.symbols == ("600519.SH", "000001.SZ")
    assert isinstance(intraday, IntradayRequest)
    assert intraday.symbol == "688981.SH"


@pytest.mark.asyncio
async def test_action_tools_reject_parameters_for_a_different_action() -> None:
    registry, service = _registry()

    with pytest.raises(Exception, match="stock_ranking.sectors 不接受参数：market"):
        await registry.call(
            "stock_ranking",
            action="sectors",
            market="all_a",
        )

    with pytest.raises(
        Exception, match="stock_fundamentals.financials 不接受参数：page"
    ):
        await registry.call(
            "stock_fundamentals",
            action="financials",
            symbol="600519.SH",
            page=1,
        )

    with pytest.raises(Exception, match="stock_research.forecast 不接受参数：limit"):
        await registry.call(
            "stock_research",
            action="forecast",
            symbol="600519.SH",
            limit=5,
        )

    with pytest.raises(Exception, match="report_id"):
        await registry.call(
            "stock_research",
            action="stock_reports",
            symbol="600519.SH",
            content="full",
        )

    assert service.requests == []
