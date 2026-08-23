"""MX research and paper-trading interface contracts."""

from __future__ import annotations

import pytest

from backend.stock_api.mx.research import MxResearchClient
from backend.stock_api.mx.trading import MxPaperTradingClient


class RecordingTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def post_envelope(
        self, endpoint: str, body: dict[str, object]
    ) -> dict[str, object]:
        self.calls.append((endpoint, body))
        return {"success": True, "data": {"accepted": True}}


@pytest.mark.asyncio
async def test_research_interfaces_use_their_mx_endpoints() -> None:
    transport = RecordingTransport()
    client = MxResearchClient(transport)  # type: ignore[arg-type]

    await client.query_market_data("贵州茅台最新财务数据")
    await client.search_news("A股政策")
    await client.select_stocks("低估值高股息")

    assert transport.calls == [
        ("/api/claw/query", {"toolQuery": "贵州茅台最新财务数据"}),
        ("/api/claw/news-search", {"query": "A股政策"}),
        ("/api/claw/stock-screen", {"keyword": "低估值高股息"}),
    ]


@pytest.mark.asyncio
async def test_trading_client_parses_before_submitting() -> None:
    transport = RecordingTransport()
    client = MxPaperTradingClient(transport)  # type: ignore[arg-type]

    await client.trade("买入 600519 1700 100")
    await client.cancel("撤单 order-1 600519")
    await client.cancel("一键撤单")

    assert transport.calls == [
        (
            "/api/claw/mockTrading/trade",
            {
                "type": "buy",
                "stockCode": "600519",
                "price": 1700.0,
                "quantity": 100,
                "useMarketPrice": False,
            },
        ),
        (
            "/api/claw/mockTrading/cancel",
            {"type": "order", "orderId": "order-1", "stockCode": "600519"},
        ),
        ("/api/claw/mockTrading/cancel", {"type": "all"}),
    ]


@pytest.mark.asyncio
async def test_trade_rejects_cancel_and_market_order_instruction() -> None:
    client = MxPaperTradingClient(RecordingTransport())  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="buy or sell"):
        await client.trade("撤单 order-1 600519")
    with pytest.raises(ValueError, match="不支持市价委托"):
        await client.trade("市价卖出 600519 100")
