"""Agent-facing tools for normalized public stock data."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, cast

from backend.agent.tools.registry import ToolRegistry
from backend.infra.integrations.tool_policy import SideEffectLevel
from backend.llm import AbortSignal, ProviderJsonObject, ToolDefinition
from backend.stock_api.public import (
    AnnouncementsRequest,
    ConnectMoneyFlowRequest,
    FinancialsRequest,
    ForecastRequest,
    IndustryComparisonRequest,
    IntradayRequest,
    MarketReportsRequest,
    NewsFeedRequest,
    NewsSearchRequest,
    OperatingIndicatorsRequest,
    PublicStockRequest,
    QuoteSnapshotRequest,
    RatingsRequest,
    SectorMoneyFlowRequest,
    SectorRankingRequest,
    ShareholdersRequest,
    StockMarketDataService,
    StockMoneyFlowHistoryRequest,
    StockMoneyFlowIntradayRequest,
    StockNewsRequest,
    StockRankingRequest,
    StockReportsRequest,
    UnsupportedStockRequest,
    ValuationRequest,
    bound_agent_result,
)

READ_STAGES = ("Run",)


_SYMBOL_PATTERN = (
    r"^(?:(?:600|601|603|605|688)\d{3}(?:\.SH)?|"
    r"(?:000|001|002|003|300|301)\d{3}(?:\.SZ)?)$"
)


def _symbol_property() -> dict[str, str]:
    return {
        "type": "string",
        "pattern": _SYMBOL_PATTERN,
        "description": "沪深 A 股代码；省略交易所后缀时自动补齐。",
    }


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


def _one_of(branches: list[ProviderJsonObject]) -> ProviderJsonObject:
    return cast(ProviderJsonObject, {"type": "object", "oneOf": branches})


def _const(value: object) -> dict[str, object]:
    return {"const": value}


def _integer_property(
    minimum: int, maximum: int, default: int | None = None
) -> dict[str, object]:
    property_: dict[str, object] = {
        "type": "integer",
        "minimum": minimum,
        "maximum": maximum,
    }
    if default is not None:
        property_["default"] = default
    return property_


def _enum_property(
    values: tuple[str, ...], default: str | None = None
) -> dict[str, object]:
    property_: dict[str, object] = {"type": "string", "enum": list(values)}
    if default is not None:
        property_["default"] = default
    return property_


def _action_arguments(
    values: dict[str, object], allowed: set[str], action: str
) -> dict[str, object]:
    extras = set(values) - allowed
    if extras:
        raise UnsupportedStockRequest(
            f"{action} 不接受参数：{'、'.join(sorted(extras))}。"
        )
    return values


def _build_request[T](factory: Callable[..., T], values: dict[str, object]) -> T:
    """Build a dataclass only after its action-specific input keys are closed."""

    return factory(**cast(Any, values))


def _with_symbol(symbol: str, values: dict[str, object]) -> dict[str, object]:
    return {"symbol": symbol, **values}


@dataclass(slots=True)
class _PublicStockTool:
    service: StockMarketDataService
    name: str
    enabled_stages: tuple[str, ...] = field(default=READ_STAGES)
    side_effect_level: SideEffectLevel = SideEffectLevel.READ
    execution_mode: str = "parallel"

    async def _execute(
        self,
        request: PublicStockRequest,
        abort_signal: AbortSignal | None,
    ) -> object:
        result = await self.service.execute(request, abort_signal)
        return bound_agent_result(result, request)


@dataclass(slots=True)
class StockQuoteTool(_PublicStockTool):
    name: str = "stock_quote"

    def to_tool_definition(self) -> ToolDefinition:
        return {
            "name": self.name,
            "description": "查询一只或多只沪深 A 股的标准化实时行情快照。",
            "parameters": _one_of(
                [
                    _schema(
                        {
                            "symbols": {
                                "type": "array",
                                "items": _symbol_property(),
                                "minItems": 1,
                                "maxItems": 60,
                            },
                            "detail": {"const": "basic", "default": "basic"},
                        },
                        ["symbols"],
                    ),
                    _schema(
                        {
                            "symbols": {
                                "type": "array",
                                "items": _symbol_property(),
                                "minItems": 1,
                                "maxItems": 20,
                            },
                            "detail": {"const": "full"},
                        },
                        ["symbols", "detail"],
                    ),
                ]
            ),
        }

    async def run(self, symbols: list[str], detail: str = "basic") -> object:
        return await self._execute(
            _build_request(
                QuoteSnapshotRequest,
                {"symbols": tuple(symbols), "detail": detail},
            ),
            None,
        )

    async def run_with_abort(
        self,
        symbols: list[str],
        detail: str = "basic",
        *,
        abort_signal: AbortSignal | None,
    ) -> object:
        return await self._execute(
            _build_request(
                QuoteSnapshotRequest,
                {"symbols": tuple(symbols), "detail": detail},
            ),
            abort_signal,
        )


@dataclass(slots=True)
class StockIntradayTool(_PublicStockTool):
    name: str = "stock_intraday"

    def to_tool_definition(self) -> ToolDefinition:
        return {
            "name": self.name,
            "description": "查询一只沪深 A 股的当日或近五日分时走势。",
            "parameters": _schema(
                {
                    "symbol": _symbol_property(),
                    "days": {"type": "integer", "enum": [1, 5], "default": 1},
                    "limit": _integer_property(1, 300, 120),
                },
                ["symbol"],
            ),
        }

    async def run(self, symbol: str, days: int = 1, limit: int = 120) -> object:
        return await self._execute(
            _build_request(
                IntradayRequest,
                {"symbol": symbol, "days": days, "limit": limit},
            ),
            None,
        )

    async def run_with_abort(
        self,
        symbol: str,
        days: int = 1,
        limit: int = 120,
        *,
        abort_signal: AbortSignal | None,
    ) -> object:
        return await self._execute(
            _build_request(
                IntradayRequest,
                {"symbol": symbol, "days": days, "limit": limit},
            ),
            abort_signal,
        )


@dataclass(slots=True)
class StockRankingTool(_PublicStockTool):
    name: str = "stock_ranking"

    def to_tool_definition(self) -> ToolDefinition:
        return {
            "name": self.name,
            "description": "查询沪深 A 股或行业、概念板块的标准化排行。",
            "parameters": _one_of(
                [
                    _schema(
                        {
                            "action": _const("stocks"),
                            "market": _enum_property(
                                ("all_a", "sh_a", "sz_a", "chinext", "star"), "all_a"
                            ),
                            "sort": _enum_property(
                                (
                                    "price",
                                    "change_percent",
                                    "volume",
                                    "amount",
                                    "turnover_rate",
                                    "net_inflow",
                                ),
                                "change_percent",
                            ),
                            "order": _enum_property(("asc", "desc"), "desc"),
                            "page": _integer_property(1, 100, 1),
                            "limit": _integer_property(1, 50, 20),
                        },
                        ["action"],
                    ),
                    _schema(
                        {
                            "action": _const("sectors"),
                            "sector_type": _enum_property(
                                ("industry", "concept"), "industry"
                            ),
                            "sort": _enum_property(
                                (
                                    "price",
                                    "change_percent",
                                    "volume",
                                    "amount",
                                    "turnover_rate",
                                    "net_inflow",
                                ),
                                "change_percent",
                            ),
                            "order": _enum_property(("asc", "desc"), "desc"),
                            "page": _integer_property(1, 100, 1),
                            "limit": _integer_property(1, 50, 20),
                        },
                        ["action"],
                    ),
                ]
            ),
        }

    async def run(self, action: str, **kwargs: object) -> object:
        return await self._run(action, kwargs, None)

    async def run_with_abort(
        self, action: str, *, abort_signal: AbortSignal | None, **kwargs: object
    ) -> object:
        return await self._run(action, kwargs, abort_signal)

    async def _run(
        self, action: str, kwargs: dict[str, object], abort_signal: AbortSignal | None
    ) -> object:
        request: PublicStockRequest
        if action == "stocks":
            values = _action_arguments(
                kwargs,
                {"market", "sort", "order", "page", "limit"},
                "stock_ranking.stocks",
            )
            request = _build_request(StockRankingRequest, values)
        elif action == "sectors":
            values = _action_arguments(
                kwargs,
                {"sector_type", "sort", "order", "page", "limit"},
                "stock_ranking.sectors",
            )
            request = _build_request(SectorRankingRequest, values)
        else:
            raise UnsupportedStockRequest(
                "stock_ranking.action 必须为 stocks 或 sectors。"
            )
        return await self._execute(request, abort_signal)


@dataclass(slots=True)
class StockMoneyFlowTool(_PublicStockTool):
    name: str = "stock_money_flow"

    def to_tool_definition(self) -> ToolDefinition:
        symbol = _symbol_property()
        return {
            "name": self.name,
            "description": "查询个股、板块或沪深港通的标准化资金流。",
            "parameters": _one_of(
                [
                    _schema(
                        {
                            "action": _const("stock_history"),
                            "symbol": symbol,
                            "page": _integer_property(1, 20, 1),
                            "limit": _integer_property(1, 50, 20),
                        },
                        ["action", "symbol"],
                    ),
                    _schema(
                        {
                            "action": _const("stock_intraday"),
                            "symbol": symbol,
                            "limit": _integer_property(1, 300, 120),
                        },
                        ["action", "symbol"],
                    ),
                    _schema(
                        {
                            "action": _const("sector"),
                            "sector_type": _enum_property(
                                ("industry", "concept"), "industry"
                            ),
                            "page": _integer_property(1, 100, 1),
                            "limit": _integer_property(1, 50, 20),
                        },
                        ["action"],
                    ),
                    _schema(
                        {
                            "action": _const("connect"),
                            "direction": _enum_property(
                                ("all", "northbound", "southbound"), "all"
                            ),
                            "limit": _integer_property(1, 300, 120),
                        },
                        ["action"],
                    ),
                ]
            ),
        }

    async def run(self, action: str, **kwargs: object) -> object:
        return await self._run(action, kwargs, None)

    async def run_with_abort(
        self, action: str, *, abort_signal: AbortSignal | None, **kwargs: object
    ) -> object:
        return await self._run(action, kwargs, abort_signal)

    async def _run(
        self, action: str, kwargs: dict[str, object], abort_signal: AbortSignal | None
    ) -> object:
        request: PublicStockRequest
        if action == "stock_history":
            request = _build_request(
                StockMoneyFlowHistoryRequest,
                _action_arguments(
                    kwargs,
                    {"symbol", "page", "limit"},
                    "stock_money_flow.stock_history",
                ),
            )
        elif action == "stock_intraday":
            request = _build_request(
                StockMoneyFlowIntradayRequest,
                _action_arguments(
                    kwargs, {"symbol", "limit"}, "stock_money_flow.stock_intraday"
                ),
            )
        elif action == "sector":
            request = _build_request(
                SectorMoneyFlowRequest,
                _action_arguments(
                    kwargs, {"sector_type", "page", "limit"}, "stock_money_flow.sector"
                ),
            )
        elif action == "connect":
            request = _build_request(
                ConnectMoneyFlowRequest,
                _action_arguments(
                    kwargs, {"direction", "limit"}, "stock_money_flow.connect"
                ),
            )
        else:
            raise UnsupportedStockRequest("stock_money_flow.action 无效。")
        return await self._execute(request, abort_signal)


@dataclass(slots=True)
class StockFundamentalsTool(_PublicStockTool):
    name: str = "stock_fundamentals"

    def to_tool_definition(self) -> ToolDefinition:
        symbol = _symbol_property()
        page = {"type": "integer", "minimum": 1, "maximum": 100, "default": 1}
        limit = {"type": "integer", "minimum": 1, "maximum": 50, "default": 20}
        return {
            "name": self.name,
            "description": "查询财务、股东、估值、行业对比或经营指标。",
            "parameters": _one_of(
                [
                    _schema(
                        {
                            "action": _const("financials"),
                            "symbol": symbol,
                            "mode": {"const": "latest", "default": "latest"},
                        },
                        ["action", "symbol"],
                    ),
                    _schema(
                        {
                            "action": _const("financials"),
                            "symbol": symbol,
                            "mode": _const("quarterly"),
                            "page": page,
                            "limit": limit,
                        },
                        ["action", "symbol", "mode"],
                    ),
                    _schema(
                        {
                            "action": _const("shareholders"),
                            "symbol": symbol,
                            "page": page,
                            "limit": limit,
                        },
                        ["action", "symbol"],
                    ),
                    _schema(
                        {"action": _const("valuation"), "symbol": symbol},
                        ["action", "symbol"],
                    ),
                    _schema(
                        {
                            "action": _const("industry_comparison"),
                            "symbol": symbol,
                            "page": page,
                            "limit": limit,
                        },
                        ["action", "symbol"],
                    ),
                    _schema(
                        {
                            "action": _const("operating_indicators"),
                            "symbol": symbol,
                            "page": page,
                            "limit": limit,
                        },
                        ["action", "symbol"],
                    ),
                ]
            ),
        }

    async def run(self, action: str, symbol: str, **kwargs: object) -> object:
        return await self._run(action, symbol, kwargs, None)

    async def run_with_abort(
        self,
        action: str,
        symbol: str,
        *,
        abort_signal: AbortSignal | None,
        **kwargs: object,
    ) -> object:
        return await self._run(action, symbol, kwargs, abort_signal)

    async def _run(
        self,
        action: str,
        symbol: str,
        kwargs: dict[str, object],
        abort_signal: AbortSignal | None,
    ) -> object:
        request: PublicStockRequest
        if action == "financials":
            mode = kwargs.get("mode", "latest")
            allowed = {"mode", "page", "limit"} if mode == "quarterly" else {"mode"}
            request = _build_request(
                FinancialsRequest,
                _with_symbol(
                    symbol,
                    _action_arguments(kwargs, allowed, "stock_fundamentals.financials"),
                ),
            )
        elif action == "shareholders":
            request = _build_request(
                ShareholdersRequest,
                _with_symbol(
                    symbol,
                    _action_arguments(
                        kwargs, {"page", "limit"}, "stock_fundamentals.shareholders"
                    ),
                ),
            )
        elif action == "valuation":
            request = _build_request(
                ValuationRequest,
                _with_symbol(
                    symbol,
                    _action_arguments(kwargs, set(), "stock_fundamentals.valuation"),
                ),
            )
        elif action == "industry_comparison":
            request = _build_request(
                IndustryComparisonRequest,
                _with_symbol(
                    symbol,
                    _action_arguments(
                        kwargs,
                        {"page", "limit"},
                        "stock_fundamentals.industry_comparison",
                    ),
                ),
            )
        elif action == "operating_indicators":
            request = _build_request(
                OperatingIndicatorsRequest,
                _with_symbol(
                    symbol,
                    _action_arguments(
                        kwargs,
                        {"page", "limit"},
                        "stock_fundamentals.operating_indicators",
                    ),
                ),
            )
        else:
            raise UnsupportedStockRequest("stock_fundamentals.action 无效。")
        return await self._execute(request, abort_signal)


@dataclass(slots=True)
class StockResearchTool(_PublicStockTool):
    name: str = "stock_research"

    def to_tool_definition(self) -> ToolDefinition:
        symbol = _symbol_property()
        page = {"type": "integer", "minimum": 1, "maximum": 100, "default": 1}
        return {
            "name": self.name,
            "description": "查询市场研报、个股研报、盈利预测或机构评级。",
            "parameters": _one_of(
                [
                    _schema(
                        {
                            "action": _const("market_reports"),
                            "category": _enum_property(
                                ("strategy", "macro", "broker", "industry"), "strategy"
                            ),
                            "days": _integer_property(0, 30, 1),
                            "top": _integer_property(1, 10, 3),
                            "index": _integer_property(1, 100),
                        },
                        ["action"],
                    ),
                    _schema(
                        {
                            "action": _const("stock_reports"),
                            "symbol": symbol,
                            "content": {"const": "summary", "default": "summary"},
                            "page": page,
                            "limit": _integer_property(1, 5, 5),
                        },
                        ["action", "symbol"],
                    ),
                    _schema(
                        {
                            "action": _const("stock_reports"),
                            "symbol": symbol,
                            "content": _const("full"),
                            "report_id": {
                                "type": "string",
                                "pattern": "^[A-Za-z0-9_-]{1,64}$",
                            },
                        },
                        ["action", "symbol", "content", "report_id"],
                    ),
                    _schema(
                        {
                            "action": _const("forecast"),
                            "symbol": symbol,
                            "mode": {"const": "summary", "default": "summary"},
                        },
                        ["action", "symbol"],
                    ),
                    _schema(
                        {
                            "action": _const("forecast"),
                            "symbol": symbol,
                            "mode": _const("institutions"),
                            "page": page,
                            "limit": _integer_property(1, 50, 20),
                        },
                        ["action", "symbol", "mode"],
                    ),
                    _schema(
                        {"action": _const("ratings"), "symbol": symbol},
                        ["action", "symbol"],
                    ),
                ]
            ),
        }

    async def run(self, action: str, **kwargs: object) -> object:
        return await self._run(action, kwargs, None)

    async def run_with_abort(
        self, action: str, *, abort_signal: AbortSignal | None, **kwargs: object
    ) -> object:
        return await self._run(action, kwargs, abort_signal)

    async def _run(
        self, action: str, kwargs: dict[str, object], abort_signal: AbortSignal | None
    ) -> object:
        request: PublicStockRequest
        if action == "market_reports":
            request = _build_request(
                MarketReportsRequest,
                _action_arguments(
                    kwargs,
                    {"category", "days", "top", "index"},
                    "stock_research.market_reports",
                ),
            )
        elif action == "stock_reports":
            content = kwargs.get("content", "summary")
            allowed = (
                {"symbol", "content", "report_id"}
                if content == "full"
                else {"symbol", "page", "limit", "content"}
            )
            request = _build_request(
                StockReportsRequest,
                _action_arguments(kwargs, allowed, "stock_research.stock_reports"),
            )
        elif action == "forecast":
            mode = kwargs.get("mode", "summary")
            allowed = (
                {"symbol", "mode", "page", "limit"}
                if mode == "institutions"
                else {"symbol", "mode"}
            )
            request = _build_request(
                ForecastRequest,
                _action_arguments(kwargs, allowed, "stock_research.forecast"),
            )
        elif action == "ratings":
            request = _build_request(
                RatingsRequest,
                _action_arguments(kwargs, {"symbol"}, "stock_research.ratings"),
            )
        else:
            raise UnsupportedStockRequest("stock_research.action 无效。")
        return await self._execute(request, abort_signal)


@dataclass(slots=True)
class StockNewsTool(_PublicStockTool):
    name: str = "stock_news"

    def to_tool_definition(self) -> ToolDefinition:
        symbol = _symbol_property()
        page = {"type": "integer", "minimum": 1, "maximum": 100, "default": 1}
        limit = {"type": "integer", "minimum": 1, "maximum": 50, "default": 10}
        return {
            "name": self.name,
            "description": "查询财经资讯流、个股新闻、官方公告或新闻搜索结果。",
            "parameters": _one_of(
                [
                    _schema(
                        {
                            "action": _const("feed"),
                            "feed": {"const": "headlines", "default": "headlines"},
                            "page": {"const": 1, "default": 1},
                            "limit": _integer_property(1, 20, 10),
                        },
                        ["action"],
                    ),
                    _schema(
                        {
                            "action": _const("feed"),
                            "feed": {"const": "flash"},
                            "page": {"const": 1, "default": 1},
                            "limit": _integer_property(1, 50, 30),
                        },
                        ["action", "feed"],
                    ),
                    _schema(
                        {
                            "action": _const("feed"),
                            "feed": _enum_property(
                                ("finance", "global", "stocks", "money")
                            ),
                            "page": page,
                            "limit": limit,
                        },
                        ["action", "feed"],
                    ),
                    _schema(
                        {
                            "action": _const("stock_news"),
                            "symbol": symbol,
                            "page": page,
                            "limit": _integer_property(1, 50, 10),
                        },
                        ["action", "symbol"],
                    ),
                    _schema(
                        {
                            "action": _const("announcements"),
                            "symbol": symbol,
                            "page": page,
                            "limit": _integer_property(1, 50, 20),
                        },
                        ["action", "symbol"],
                    ),
                    _schema(
                        {
                            "action": _const("search"),
                            "keyword": {
                                "type": "string",
                                "minLength": 1,
                                "maxLength": 100,
                            },
                            "page": page,
                            "limit": _integer_property(1, 20, 10),
                        },
                        ["action", "keyword"],
                    ),
                ]
            ),
        }

    async def run(self, action: str, **kwargs: object) -> object:
        return await self._run(action, kwargs, None)

    async def run_with_abort(
        self, action: str, *, abort_signal: AbortSignal | None, **kwargs: object
    ) -> object:
        return await self._run(action, kwargs, abort_signal)

    async def _run(
        self, action: str, kwargs: dict[str, object], abort_signal: AbortSignal | None
    ) -> object:
        request: PublicStockRequest
        if action == "feed":
            request = _build_request(
                NewsFeedRequest,
                _action_arguments(kwargs, {"feed", "page", "limit"}, "stock_news.feed"),
            )
        elif action == "stock_news":
            request = _build_request(
                StockNewsRequest,
                _action_arguments(
                    kwargs, {"symbol", "page", "limit"}, "stock_news.stock_news"
                ),
            )
        elif action == "announcements":
            request = _build_request(
                AnnouncementsRequest,
                _action_arguments(
                    kwargs, {"symbol", "page", "limit"}, "stock_news.announcements"
                ),
            )
        elif action == "search":
            request = _build_request(
                NewsSearchRequest,
                _action_arguments(
                    kwargs, {"keyword", "page", "limit"}, "stock_news.search"
                ),
            )
        else:
            raise UnsupportedStockRequest("stock_news.action 无效。")
        return await self._execute(request, abort_signal)


def register_public_stock_tools(
    registry: ToolRegistry, *, service: StockMarketDataService
) -> None:
    """Register the seven public-only Agent tools."""

    registry.register(StockQuoteTool(service))
    registry.register(StockIntradayTool(service))
    registry.register(StockRankingTool(service))
    registry.register(StockMoneyFlowTool(service))
    registry.register(StockFundamentalsTool(service))
    registry.register(StockResearchTool(service))
    registry.register(StockNewsTool(service))


__all__ = [
    "StockFundamentalsTool",
    "StockIntradayTool",
    "StockMoneyFlowTool",
    "StockNewsTool",
    "StockQuoteTool",
    "StockRankingTool",
    "StockResearchTool",
    "register_public_stock_tools",
]
