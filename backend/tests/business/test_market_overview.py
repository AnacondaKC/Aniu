"""Tests for the public market overview aggregation service."""

from __future__ import annotations

import pytest

from backend.stock_api.public.contracts import (
    IndexKlineRequest,
    IndexQuoteRequest,
    MarketBreadthRequest,
    NewsFeedRequest,
    SectorRankingRequest,
    StockRankingRequest,
)
from backend.stock_api.public.market_overview import PublicMarketOverviewQuery


class FakePublicStockData:
    def __init__(self) -> None:
        self.requests: list[object] = []

    async def execute(self, request: object) -> dict[str, object]:
        self.requests.append(request)
        if isinstance(request, IndexQuoteRequest):
            return {
                "data": {
                    "quotes": [
                        {
                            "symbol": "000001.SH",
                            "price": 3_280.5,
                            "previous_close": 3_260.0,
                            "change": 20.5,
                            "change_percent": 0.63,
                            "market_time": "2026-07-31T08:00:00Z",
                            "high": 3_290.0,
                            "low": 3_250.0,
                            "amount": 123_000_000_000,
                        },
                        {
                            "symbol": "399001.SZ",
                            "price": 10_000.0,
                            "previous_close": 9_900.0,
                            "change": 100.0,
                            "change_percent": 1.01,
                            "market_time": "2026-07-31T08:00:00Z",
                            "high": 10_100.0,
                            "low": 9_800.0,
                            "amount": 456_000_000_000,
                        },
                    ]
                }
            }
        if isinstance(request, MarketBreadthRequest):
            return {"data": {"rising": 1576, "falling": 3549, "flat": 158}}
        if isinstance(request, IndexKlineRequest):
            if request.symbol == "399006.SZ":
                raise RuntimeError("分时源暂时不可用")
            return {
                "data": {
                    "bars": [
                        {
                            "time": "2026-07-31T09:30:00Z",
                            "close": 100.0,
                            "amount": 12_000_000,
                        },
                        {
                            "time": "2026-07-31T10:00:00Z",
                            "close": 101.0,
                            "amount": 18_000_000,
                        },
                    ]
                }
            }
        if isinstance(request, StockRankingRequest):
            if request.order == "asc" and request.sort == "net_inflow":
                raise RuntimeError("资金流源暂时不可用")
            return {
                "data": {
                    "items": [
                        {
                            "name": f"{request.sort}-{request.order}-{index}",
                            "symbol": f"600{index:03d}.SH",
                            "price": 10.0,
                            "change_percent": 1.2,
                            "net_inflow": 100_000_000,
                        }
                        for index in range(request.limit)
                    ]
                }
            }
        if isinstance(request, SectorRankingRequest):
            return {
                "data": {
                    "items": [
                        {
                            "name": request.sector_type,
                            "change_percent": 2.5,
                        }
                    ]
                }
            }
        if isinstance(request, NewsFeedRequest):
            return {
                "data": {
                    "items": [
                        {
                            "id": request.feed,
                            "title": request.feed,
                            "time": "2026-07-31T08:00:00Z",
                        }
                    ]
                }
            }
        raise AssertionError(f"unexpected request: {request!r}")


@pytest.mark.asyncio
async def test_market_overview_keeps_partial_errors() -> None:
    public_data = FakePublicStockData()
    service = PublicMarketOverviewQuery(public_data)

    overview = await service.get_market_overview()

    assert len(public_data.requests) == 14
    assert [item["id"] for item in overview["indices"]] == ["sse", "szse"]
    assert overview["indices"][0]["name"] == "上证指数"
    assert len(overview["trends"]) == 3
    assert overview["trends"][0]["points"][0]["cumulative_amount"] == 12_000_000
    assert overview["turnover"] == {
        "today_amount": 579_000_000_000,
    }
    assert overview["breadth"] == {"rising": 1576, "falling": 3549, "flat": 158}
    assert len(overview["rankings"]["gainers"]) == 5
    assert len(overview["rankings"]["losers"]) == 5
    assert overview["rankings"]["gainers"][0]["name"] == "change_percent-desc-0"
    assert overview["rankings"]["net_outflow"] == []
    assert overview["hotspots"]["industry"][0]["name"] == "industry"
    assert overview["headlines"][0]["id"] == "headlines"
    assert overview["flash_news"][0]["id"] == "flash"
    errors = overview["errors"]
    assert isinstance(errors, list)
    assert {(error["resource"], error["item_id"]) for error in errors} == {
        ("trend", "chinext"),
        ("ranking", "net_outflow"),
    }


@pytest.mark.asyncio
async def test_market_overview_requests_all_expected_resource_groups() -> None:
    public_data = FakePublicStockData()
    service = PublicMarketOverviewQuery(public_data)

    await service.get_market_overview()

    operations = [
        getattr(request, "operation", None) for request in public_data.requests
    ]
    assert operations.count("index.quote") == 1
    assert operations.count("market.breadth") == 1
    assert operations.count("index.kline") == 4
    assert operations.count("ranking.stocks") == 4
    assert operations.count("ranking.sectors") == 2
    assert operations.count("news.feed") == 2
    assert any(
        isinstance(request, IndexQuoteRequest) for request in public_data.requests
    )
    assert all(
        isinstance(request, IndexKlineRequest) and request.limit == 300
        for request in public_data.requests
        if isinstance(request, IndexKlineRequest)
    )
    assert any(
        isinstance(request, StockRankingRequest) for request in public_data.requests
    )
    assert any(
        isinstance(request, SectorRankingRequest) for request in public_data.requests
    )
    assert any(isinstance(request, NewsFeedRequest) for request in public_data.requests)
    assert all(
        request.operation
        for request in public_data.requests
        if hasattr(request, "operation")
    )
