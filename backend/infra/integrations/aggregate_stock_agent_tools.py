"""Agent-facing composite stock research tools."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import cast

from backend.agent.tools.registry import ToolRegistry
from backend.infra.integrations.tool_policy import SideEffectLevel
from backend.llm import AbortSignal, ProviderJsonObject, ToolDefinition
from backend.stock_api import MxMoniClient, MxResearchClient
from backend.stock_api.aggregate import (
    IndustrySnapshotAggregator,
    MarketSnapshotAggregator,
    PortfolioStockSnapshotAggregator,
    StockAnalysisAggregator,
)
from backend.stock_api.public import StockMarketDataService

READ_STAGES = ("Run",)
_SYMBOL_PATTERN = (
    r"^(?:(?:600|601|603|605|688)\d{3}(?:\.SH)?|"
    r"(?:000|001|002|003|300|301)\d{3}(?:\.SZ)?)$"
)


def _schema(properties: dict[str, object], required: list[str]) -> ProviderJsonObject:
    return cast(
        ProviderJsonObject,
        {
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": False,
        },
    )


@dataclass(slots=True)
class MarketSnapshotTool:
    aggregator: MarketSnapshotAggregator
    name: str = "market_snapshot"
    enabled_stages: tuple[str, ...] = field(default=READ_STAGES)
    side_effect_level: SideEffectLevel = SideEffectLevel.READ
    execution_mode: str = "parallel"

    def to_tool_definition(self) -> ToolDefinition:
        return {
            "name": self.name,
            "description": (
                "聚合查询七大主要指数的实时行情、最近 5 根日 K 线和近期市场策略研报。"
                "需要整体市场研判时使用；单一指数或单一字段查询请使用细粒度工具。"
            ),
            "parameters": _schema({}, []),
        }

    async def run(self) -> object:
        return await self.aggregator.snapshot()

    async def run_with_abort(self, *, abort_signal: AbortSignal | None) -> object:
        return await self.aggregator.snapshot(abort_signal=abort_signal)


@dataclass(slots=True)
class PortfolioStockSnapshotTool:
    aggregator: PortfolioStockSnapshotAggregator
    name: str = "portfolio_stock_snapshot"
    enabled_stages: tuple[str, ...] = field(default=READ_STAGES)
    side_effect_level: SideEffectLevel = SideEffectLevel.READ
    execution_mode: str = "parallel"

    def to_tool_definition(self) -> ToolDefinition:
        return {
            "name": self.name,
            "description": (
                "按持仓市值分页聚合模拟组合的行情、K 线、资金流、财务和资讯；"
                "ETF 仅聚合行情与资讯。需要组合层面的持仓研判时使用。"
            ),
            "parameters": _schema(
                {
                    "page": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 1,
                        "description": "按持仓市值倒序，每页最多 8 只。",
                    }
                },
                [],
            ),
        }

    async def run(self, page: int = 1) -> object:
        return await self.aggregator.snapshot(page)

    async def run_with_abort(
        self, page: int = 1, *, abort_signal: AbortSignal | None
    ) -> object:
        return await self.aggregator.snapshot(page, abort_signal=abort_signal)


@dataclass(slots=True)
class StockAnalysisTool:
    aggregator: StockAnalysisAggregator
    name: str = "stock_analysis"
    enabled_stages: tuple[str, ...] = field(default=READ_STAGES)
    side_effect_level: SideEffectLevel = SideEffectLevel.READ
    execution_mode: str = "parallel"

    def to_tool_definition(self) -> ToolDefinition:
        return {
            "name": self.name,
            "description": (
                "聚合查询一只沪深 A 股的行情、10 日 K 线与资金流、财务、股东、"
                "估值、行业对比、研报预测、评级和资讯。需要完整个股研判时使用；"
                "单一字段查询请使用对应细粒度工具。"
            ),
            "parameters": _schema(
                {
                    "symbol": {
                        "type": "string",
                        "pattern": _SYMBOL_PATTERN,
                        "description": "沪深 A 股代码；省略交易所后缀时自动补齐。",
                    }
                },
                ["symbol"],
            ),
        }

    async def run(self, symbol: str) -> object:
        return await self.aggregator.snapshot(symbol)

    async def run_with_abort(
        self, symbol: str, *, abort_signal: AbortSignal | None
    ) -> object:
        return await self.aggregator.snapshot(symbol, abort_signal=abort_signal)


@dataclass(slots=True)
class IndustrySnapshotTool:
    aggregator: IndustrySnapshotAggregator
    name: str = "industry_snapshot"
    enabled_stages: tuple[str, ...] = field(default=READ_STAGES)
    side_effect_level: SideEffectLevel = SideEffectLevel.READ
    execution_mode: str = "parallel"

    def to_tool_definition(self) -> ToolDefinition:
        return {
            "name": self.name,
            "description": (
                "聚合查询行业和概念板块当日资金流前 10 名及市场要闻前 20 条。"
                "用于热度板块研判。"
            ),
            "parameters": _schema({}, []),
        }

    async def run(self) -> object:
        return await self.aggregator.snapshot()

    async def run_with_abort(self, *, abort_signal: AbortSignal | None) -> object:
        return await self.aggregator.snapshot(abort_signal=abort_signal)


def register_aggregate_stock_tools(
    registry: ToolRegistry,
    *,
    public_data: StockMarketDataService,
    research: MxResearchClient | None = None,
    portfolio: MxMoniClient | None = None,
) -> None:
    """Register each aggregate tool when its own dependencies are available."""

    registry.register(MarketSnapshotTool(MarketSnapshotAggregator(public_data)))
    if research is None or portfolio is None:
        return
    registry.register(
        PortfolioStockSnapshotTool(
            PortfolioStockSnapshotAggregator(
                public_data=public_data,
                research=research,
                portfolio=portfolio,
            )
        )
    )
    registry.register(StockAnalysisTool(StockAnalysisAggregator(public_data)))
    registry.register(IndustrySnapshotTool(IndustrySnapshotAggregator(public_data)))


__all__ = [
    "IndustrySnapshotTool",
    "MarketSnapshotTool",
    "PortfolioStockSnapshotTool",
    "StockAnalysisTool",
    "register_aggregate_stock_tools",
]
