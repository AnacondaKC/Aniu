"""Agent tool contracts for the direct MX integration."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import ANY

import pytest

from backend.agent.tools import ToolRegistry
from backend.business.account import (
    AccountSnapshot,
    PortfolioOrderSnapshot,
    PositionSnapshot,
)
from backend.infra.integrations.mx_agent_tools import (
    CancelTool,
    QueryPortfolioTool,
    TradeTool,
    register_mx_tools,
)
from backend.infra.integrations.tool_policy import SideEffectLevel


@dataclass
class RecordingResearchClient:
    calls: list[tuple[str, str]]

    async def query_market_data(self, query: str) -> dict[str, str]:
        self.calls.append(("data", query))
        return {"query": query}

    async def search_news(self, query: str) -> dict[str, str]:
        self.calls.append(("news", query))
        return {"query": query}

    async def select_stocks(self, keyword: str) -> dict[str, str]:
        self.calls.append(("screening", keyword))
        return {"keyword": keyword}


@dataclass
class RecordingPortfolioClient:
    available_cash: float = 25_000
    positions: list[PositionSnapshot] | None = None
    orders: list[PortfolioOrderSnapshot] | None = None

    async def get_account_snapshot(self) -> AccountSnapshot:
        return AccountSnapshot(
            total_asset=100_000,
            available_cash=self.available_cash,
            frozen_cash=0,
            market_value=75_000,
            total_profit=5_000,
            daily_profit=300,
        )

    async def get_positions(self) -> list[PositionSnapshot]:
        return self.positions or []

    async def get_orders(self) -> list[PortfolioOrderSnapshot]:
        return self.orders or []


@dataclass
class RecordingTradingClient:
    trades: list[str]
    cancellations: list[str]

    async def trade(self, instruction: str) -> dict[str, str]:
        self.trades.append(instruction)
        return {"status": "submitted"}

    async def cancel(self, instruction: str) -> dict[str, str]:
        self.cancellations.append(instruction)
        return {"status": "cancelled"}


def _portfolio_order(index: int) -> PortfolioOrderSnapshot:
    return PortfolioOrderSnapshot(
        order_id=f"order-{index}",
        symbol="600519",
        stock_name="贵州茅台",
        direction="BUY",
        quantity=100,
        status="FILLED",
        filled_quantity=100,
        filled_price=1700,
    )


def make_registry() -> tuple[
    ToolRegistry,
    RecordingResearchClient,
    RecordingPortfolioClient,
    RecordingTradingClient,
]:
    research = RecordingResearchClient([])
    portfolio = RecordingPortfolioClient()
    trading = RecordingTradingClient([], [])
    registry = ToolRegistry()
    register_mx_tools(
        registry,
        research=research,  # type: ignore[arg-type]
        portfolio=portfolio,  # type: ignore[arg-type]
        trading=trading,  # type: ignore[arg-type]
    )
    return registry, research, portfolio, trading


def test_mx_registration_exposes_all_direct_tools_with_closed_schemas() -> None:
    registry, _, _, _ = make_registry()
    expected = {
        "query_market_data": (
            "query",
            SideEffectLevel.READ,
            ("Run",),
        ),
        "search_news": (
            "query",
            SideEffectLevel.READ,
            ("Run",),
        ),
        "select_stocks": (
            "keyword",
            SideEffectLevel.READ,
            ("Run",),
        ),
        "query_portfolio": (
            "instruction",
            SideEffectLevel.READ,
            ("Run",),
        ),
        "trade": ("instruction", SideEffectLevel.WRITE, ("Run",)),
        "cancel": ("instruction", SideEffectLevel.WRITE, ("Run",)),
    }

    assert set(registry.list_tool_names()) == set(expected)
    for name, (parameter, side_effect, stages) in expected.items():
        tool = registry.get(name)
        definition = tool.to_tool_definition()
        parameters = definition["parameters"]
        assert parameters["required"] == [parameter]
        assert parameters["additionalProperties"] is False
        assert tool.side_effect_level is side_effect
        assert tool.enabled_stages == stages

    portfolio_parameters = registry.get("query_portfolio").to_tool_definition()[
        "parameters"
    ]
    assert portfolio_parameters["properties"]["limit"] == {
        "type": "integer",
        "minimum": 1,
        "default": 50,
        "description": "委托、订单与成交返回最后 X 条；full=true 时忽略。",
    }
    assert portfolio_parameters["properties"]["full"] == {
        "type": "boolean",
        "default": False,
        "description": "true 时委托、订单与成交返回全量。",
    }


@pytest.mark.asyncio
async def test_direct_read_tools_call_the_mx_clients() -> None:
    registry, research, _, _ = make_registry()

    assert await registry.call("query_market_data", query="贵州茅台行情") == {
        "query": "贵州茅台行情"
    }
    assert await registry.call("search_news", query="半导体研报") == {
        "query": "半导体研报"
    }
    assert await registry.call("select_stocks", keyword="低估值高股息") == {
        "keyword": "低估值高股息"
    }
    assert research.calls == [
        ("data", "贵州茅台行情"),
        ("news", "半导体研报"),
        ("screening", "低估值高股息"),
    ]


@pytest.mark.asyncio
async def test_query_portfolio_returns_requested_balance() -> None:
    _, _, portfolio, _ = make_registry()

    result = await QueryPortfolioTool(portfolio).run("查询账户资金")

    assert result == {
        "status": "ok",
        "query": "查询账户资金",
        "intents": ["balance"],
        "result": {
            "total_asset": 100_000,
            "available_cash": 25_000,
            "frozen_cash": 0,
            "market_value": 75_000,
            "total_profit": 5_000,
            "daily_profit": 300,
            "operation_days": None,
            "open_date": None,
            "initial_capital": None,
            "net_value": None,
            "position_ratio": None,
            "captured_at": ANY,
        },
    }


@pytest.mark.asyncio
async def test_query_portfolio_limits_recent_orders_or_returns_full_history() -> None:
    portfolio = RecordingPortfolioClient(
        orders=[_portfolio_order(index) for index in range(55)]
    )
    tool = QueryPortfolioTool(portfolio)

    default_result = await tool.run("查询委托")
    default_orders = default_result["result"]
    assert isinstance(default_orders, list)
    assert [item["order_id"] for item in default_orders] == [
        f"order-{index}" for index in range(5, 55)
    ]
    assert default_result["truncation"] == {
        "orders": {
            "returned_count": 50,
            "total_count": 55,
            "truncated": True,
        }
    }

    limited_result = await tool.run("查询成交", limit=2)
    limited_orders = limited_result["result"]
    assert isinstance(limited_orders, list)
    assert [item["order_id"] for item in limited_orders] == ["order-53", "order-54"]
    assert limited_result["truncation"] == {
        "orders": {
            "returned_count": 2,
            "total_count": 55,
            "truncated": True,
        }
    }

    full_result = await tool.run("查询订单", limit=2, full=True)
    full_orders = full_result["result"]
    assert isinstance(full_orders, list)
    assert [item["order_id"] for item in full_orders] == [
        f"order-{index}" for index in range(55)
    ]
    assert "truncation" not in full_result


@pytest.mark.asyncio
async def test_query_portfolio_rejects_invalid_order_result_options() -> None:
    tool = QueryPortfolioTool(RecordingPortfolioClient())

    with pytest.raises(ValueError, match="limit 必须是大于 0 的整数"):
        await tool.run("查询委托", limit=0)
    with pytest.raises(ValueError, match="full 必须是布尔值"):
        await tool.run("查询委托", full="true")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_trade_and_cancel_apply_portfolio_preflight_checks() -> None:
    position = PositionSnapshot(
        symbol="600519",
        stock_name="贵州茅台",
        quantity=100,
        avg_cost=1600,
        current_price=1700,
        market_value=170_000,
        profit_ratio=0.0625,
    )
    order = PortfolioOrderSnapshot(
        order_id="order-1",
        symbol="600519",
        stock_name="贵州茅台",
        direction="BUY",
        quantity=100,
        status="PENDING",
        filled_quantity=0,
        filled_price=None,
    )
    portfolio = RecordingPortfolioClient(
        available_cash=1_000, positions=[position], orders=[order]
    )
    trading = RecordingTradingClient([], [])

    with pytest.raises(ValueError, match="超过当前可用资金"):
        await TradeTool(trading, portfolio).run("买入 600519 1700 100")
    with pytest.raises(ValueError, match="超过 600519 可卖数量"):
        await TradeTool(trading, portfolio).run("卖出 600519 1700 200")

    await TradeTool(trading, portfolio).run("卖出 600519 1700 100")
    await CancelTool(trading, portfolio).run("撤单 order-1 600519")

    assert trading.trades == ["卖出 600519 1700 100"]
    assert trading.cancellations == ["撤单 order-1 600519"]
