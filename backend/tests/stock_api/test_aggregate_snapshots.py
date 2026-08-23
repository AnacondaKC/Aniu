"""Regression coverage for current-source aggregate research snapshots."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import cast

import pytest

from backend.business.account import PositionSnapshot
from backend.business.runs.abort import RunAbortSignal
from backend.business.shared import RunAbortError
from backend.stock_api.aggregate.market_snapshot import MarketSnapshotAggregator
from backend.stock_api.aggregate.snapshots import (
    STOCK_ANALYSIS_MAX_RESULT_CHARACTERS,
    IndustrySnapshotAggregator,
    PortfolioStockSnapshotAggregator,
    StockAnalysisAggregator,
)
from backend.stock_api.public import (
    PublicStockRequest,
    StockMarketDataService,
    StockReportsRequest,
)


@dataclass
class RecordingPublicService:
    requests: list[PublicStockRequest] = field(default_factory=list)
    failing_operations: set[str] = field(default_factory=set)
    failing_symbols: set[str] = field(default_factory=set)
    missing_quote_symbols: set[str] = field(default_factory=set)
    huge_stock_news: bool = False
    wide_quote: bool = False
    sector_item_count: int = 1
    news_item_count: int = 1
    market_reports_data: dict[str, object] | None = None

    async def execute(
        self,
        request: PublicStockRequest,
        cancellation_token: object | None = None,
    ) -> dict[str, object]:
        del cancellation_token
        self.requests.append(request)
        if request.operation in self.failing_operations:
            raise RuntimeError(f"{request.operation} unavailable")
        symbol = getattr(request, "symbol", None)
        if symbol in self.failing_symbols:
            raise RuntimeError("index unavailable")
        if request.operation in {"quote.snapshot", "index.quote"}:
            symbols = getattr(request, "symbols", ())
            return {
                "data": {
                    "quotes": [
                        {
                            "symbol": symbol,
                            "last": 100.0,
                            "name": symbol,
                            **(
                                {f"field_{index}": "x" * 100 for index in range(500)}
                                if self.wide_quote
                                else {}
                            ),
                        }
                        for symbol in symbols
                        if symbol not in self.missing_quote_symbols
                    ]
                },
                "meta": {
                    "source": "tencent",
                    "attempted_sources": ["tencent"],
                    "fallback_used": False,
                    "warnings": [],
                },
            }
        if request.operation in {"chart.kline", "index.kline"}:
            return {
                "data": {"bars": [{"time": "2026-08-16", "close": 100.0}]},
                "meta": {
                    "source": "sina",
                    "attempted_sources": ["tencent", "sina"],
                    "fallback_used": True,
                    "warnings": [],
                },
            }
        if request.operation == "money_flow.stock_history":
            return {"data": {"items": [{"date": "2026-08-16", "net_inflow": 8}]}}
        if request.operation == "news.stock_news":
            title = "资讯" * 20_000 if self.huge_stock_news else "个股资讯"
            return {"data": {"items": [{"title": title}]}}
        if request.operation == "research.market_reports":
            return {
                "data": self.market_reports_data or {"items": [{"title": "市场策略"}]}
            }
        if request.operation == "news.feed":
            return {
                "data": {
                    "items": [
                        {"title": f"市场要闻 {index}"}
                        for index in range(self.news_item_count)
                    ]
                }
            }
        if request.operation == "money_flow.sector":
            sector_type = getattr(request, "sector_type", "industry")
            return {
                "data": {
                    "items": [
                        {
                            "name": f"{sector_type} {index}",
                            "net_inflow": 10,
                            "change_percent": 1.2,
                        }
                        for index in range(self.sector_item_count)
                    ]
                }
            }
        return {"data": {"operation": request.operation, "items": [{"value": 1}]}}


@dataclass
class BlockingPublicService:
    started: asyncio.Event = field(default_factory=asyncio.Event)

    async def execute(
        self,
        request: PublicStockRequest,
        cancellation_token: RunAbortSignal | None = None,
    ) -> dict[str, object]:
        del request
        self.started.set()
        assert cancellation_token is not None
        await cancellation_token.wait()
        cancellation_token.throw_if_aborted()
        raise AssertionError("unreachable")


@dataclass
class RecordingMxResearch:
    market_queries: list[str] = field(default_factory=list)
    news_queries: list[str] = field(default_factory=list)

    async def query_market_data(self, query: str) -> object:
        self.market_queries.append(query)
        return {"source": "mx", "query": query}

    async def search_news(self, query: str) -> object:
        self.news_queries.append(query)
        return {"source": "mx", "query": query}


@dataclass
class RecordingPortfolio:
    positions: list[PositionSnapshot]

    async def get_positions(self) -> list[PositionSnapshot]:
        return self.positions


def _position(
    symbol: str,
    name: str,
    market_value: float,
) -> PositionSnapshot:
    return PositionSnapshot(
        symbol=symbol,
        stock_name=name,
        quantity=100,
        avg_cost=10.0,
        current_price=10.0,
        market_value=market_value,
        profit_ratio=0.0,
    )


@pytest.mark.asyncio
async def test_market_snapshot_keeps_available_indices_when_one_index_fails() -> None:
    public = RecordingPublicService(
        failing_symbols={"000852.SH"},
        market_reports_data={
            "reports": [{"title": f"策略 {index}"} for index in range(16)],
            "contents": [
                {"title": f"策略 {index}", "content": "文" * 500} for index in range(4)
            ],
        },
    )
    result = await MarketSnapshotAggregator(
        public_data=cast(StockMarketDataService, public)
    ).snapshot()

    assert len(result["indices"]) == 6
    assert result["failed"] == ["000852.SH"]
    assert result["errors"] == {"000852.SH": "index unavailable"}
    strategy_reports = cast(dict[str, object], result["strategy_reports"])
    assert len(cast(list[object], strategy_reports["reports"])) == 15
    assert strategy_reports["reports_truncated"] is True
    contents = cast(list[dict[str, object]], strategy_reports["contents"])
    assert len(contents) == 3
    assert len(cast(str, contents[0]["content"])) == 500
    assert "content_truncated" not in contents[0]
    assert strategy_reports["contents_truncated"] is True
    operations = [request.operation for request in public.requests]
    assert operations.count("index.quote") == 1
    assert operations.count("index.kline") == 7
    assert operations.count("research.market_reports") == 1
    report_request = next(
        request
        for request in public.requests
        if request.operation == "research.market_reports"
    )
    assert getattr(report_request, "content_max_characters") is None


@pytest.mark.asyncio
async def test_market_snapshot_keeps_partial_batch_quotes() -> None:
    public = RecordingPublicService(missing_quote_symbols={"000852.SH"})

    result = await MarketSnapshotAggregator(
        public_data=cast(StockMarketDataService, public)
    ).snapshot()

    assert [item["symbol"] for item in result["indices"]] == [
        "000001.SH",
        "399001.SZ",
        "399006.SZ",
        "000688.SH",
        "000300.SH",
        "000905.SH",
    ]
    assert result["failed"] == ["000852.SH"]
    assert result["errors"] == {}


@pytest.mark.asyncio
async def test_market_snapshot_preserves_public_sources_and_reports() -> None:
    public = RecordingPublicService(
        market_reports_data={
            "reports": [{"title": "策略"}],
            "contents": [{"title": "策略", "content": "文" * 5_000}],
        }
    )

    result = await MarketSnapshotAggregator(
        public_data=cast(StockMarketDataService, public)
    ).snapshot()

    indices = cast(list[dict[str, object]], result["indices"])
    assert len(indices) == 7
    market_data = cast(dict[str, object], indices[0]["market_data"])
    quote = cast(dict[str, object], market_data["quote"])
    assert quote["symbol"] == "000001.SH"
    bars = cast(list[dict[str, object]], market_data["bars_5d"])
    assert bars == [{"time": "2026-08-16", "close": 100.0}]
    sources = cast(dict[str, dict[str, object]], market_data["sources"])
    assert sources["quote"]["source"] == "tencent"
    assert sources["bars_5d"]["source"] == "sina"
    assert sources["bars_5d"]["fallback_used"] is True

    strategy_reports = cast(dict[str, object], result["strategy_reports"])
    contents = cast(list[dict[str, object]], strategy_reports["contents"])
    assert len(cast(str, contents[0]["content"])) == 5_000


@pytest.mark.asyncio
async def test_market_snapshot_cancels_inflight_public_requests() -> None:
    public = BlockingPublicService()
    abort_signal = RunAbortSignal(run_id=20260816001)
    task = asyncio.create_task(
        MarketSnapshotAggregator(
            public_data=cast(StockMarketDataService, public)
        ).snapshot(abort_signal=abort_signal)
    )

    await public.started.wait()
    abort_signal.abort("test cancellation")

    with pytest.raises(RunAbortError, match="strategy run aborted"):
        await task


@pytest.mark.asyncio
async def test_portfolio_snapshot_pages_by_value_and_uses_mx_for_etfs() -> None:
    public = RecordingPublicService()
    research = RecordingMxResearch()
    portfolio = RecordingPortfolio(
        [
            _position("600519", "贵州茅台", 30.0),
            _position("510300", "沪深300ETF", 20.0),
            _position("200001", "不支持标的", 10.0),
        ]
    )

    result = await PortfolioStockSnapshotAggregator(
        public_data=cast(StockMarketDataService, public),
        research=cast(object, research),
        portfolio=cast(object, portfolio),
    ).snapshot()

    positions = cast(list[dict[str, object]], result["positions"])
    assert [item["instrument_type"] for item in positions] == ["a_share", "etf"]
    stock_holding = cast(dict[str, object], positions[0]["holding"])
    assert stock_holding["symbol"] == "600519"
    assert stock_holding["stock_name"] == "贵州茅台"
    assert stock_holding["market_value"] == 30.0
    assert positions[1]["unsupported_sections"] == [
        "bars_5d",
        "fund_flow_5d",
        "financial_summary",
    ]
    assert result["total_positions"] == 2
    assert result["total_a_share_positions"] == 1
    assert result["total_etf_positions"] == 1
    assert result["excluded_unsupported_positions"] == 1
    assert result["next_page"] is None
    assert len(research.market_queries) == 1
    assert research.news_queries == ["沪深300ETF ETF 最新资讯"]
    operations = [request.operation for request in public.requests]
    assert operations.count("quote.snapshot") == 1
    expected_operations = {
        "chart.kline",
        "money_flow.stock_history",
        "fundamentals.financials",
        "news.stock_news",
    }
    assert expected_operations <= set(operations)


@pytest.mark.asyncio
async def test_portfolio_snapshot_filters_unsupported_positions_before_paging() -> None:
    public = RecordingPublicService()
    portfolio = RecordingPortfolio(
        [
            _position("200001", "不支持标的", 100.0),
            _position("600519", "贵州茅台", 10.0),
        ]
    )

    result = await PortfolioStockSnapshotAggregator(
        public_data=cast(StockMarketDataService, public),
        research=cast(object, RecordingMxResearch()),
        portfolio=cast(object, portfolio),
        page_size=1,
    ).snapshot()

    positions = cast(list[dict[str, object]], result["positions"])
    holding = cast(dict[str, object], positions[0]["holding"])
    assert holding["symbol"] == "600519"
    assert result["excluded_unsupported_positions"] == 1
    assert result["next_page"] is None


@pytest.mark.asyncio
async def test_stock_analysis_keeps_partial_results() -> None:
    public = RecordingPublicService(failing_operations={"news.stock_news"})

    result = await StockAnalysisAggregator(
        public_data=cast(StockMarketDataService, public)
    ).snapshot("600519")

    assert result["symbol"] == "600519.SH"
    assert result["quote"] == {
        "symbol": "600519.SH",
        "last": 100.0,
        "name": "600519.SH",
    }
    assert result["errors"] == {"stock_news": "news.stock_news unavailable"}
    assert "unavailable_sections" not in result
    operations = [request.operation for request in public.requests]
    assert operations.count("fundamentals.financials") == 2
    assert "research.stock_reports" in operations
    report_request = next(
        request
        for request in public.requests
        if isinstance(request, StockReportsRequest)
    )
    assert report_request.limit == 3
    assert report_request.summary_max_characters is None
    assert "research.forecast" in operations
    assert "research.ratings" in operations


@pytest.mark.asyncio
async def test_stock_analysis_applies_its_32k_output_boundary() -> None:
    public = RecordingPublicService(huge_stock_news=True)

    result = await StockAnalysisAggregator(
        public_data=cast(StockMarketDataService, public)
    ).snapshot("000001")

    assert len(json.dumps(result, ensure_ascii=False, separators=(",", ":"))) <= (
        STOCK_ANALYSIS_MAX_RESULT_CHARACTERS
    )
    boundary = cast(dict[str, object], result["safety_boundary"])
    assert boundary["output_truncated"] is True
    assert "stock_news" in cast(list[str], boundary["compacted_sections"])


@pytest.mark.asyncio
async def test_stock_analysis_hard_caps_a_wide_response() -> None:
    public = RecordingPublicService(wide_quote=True)

    result = await StockAnalysisAggregator(
        public_data=cast(StockMarketDataService, public)
    ).snapshot("000001")

    assert len(json.dumps(result, ensure_ascii=False, separators=(",", ":"))) <= (
        STOCK_ANALYSIS_MAX_RESULT_CHARACTERS
    )
    assert isinstance(result["result_preview"], str)
    boundary = cast(dict[str, object], result["safety_boundary"])
    assert boundary["output_truncated"] is True
    assert (
        cast(int, boundary["original_characters"])
        > STOCK_ANALYSIS_MAX_RESULT_CHARACTERS
    )


@pytest.mark.asyncio
async def test_industry_snapshot_marks_rows_beyond_its_output_limits() -> None:
    public = RecordingPublicService(sector_item_count=11, news_item_count=21)

    result = await IndustrySnapshotAggregator(
        public_data=cast(StockMarketDataService, public)
    ).snapshot()

    industries = cast(dict[str, object], result["industries"])
    concepts = cast(dict[str, object], result["concepts"])
    assert len(cast(list[object], industries["money_flow"])) == 10
    assert industries["money_flow_truncated"] is True
    assert len(cast(list[object], concepts["money_flow"])) == 10
    assert concepts["money_flow_truncated"] is True
    assert len(cast(list[object], result["top_news"])) == 20
    assert result["top_news_truncated"] is True
    limits = [
        getattr(request, "limit", None)
        for request in public.requests
        if request.operation in {"money_flow.sector", "news.feed"}
    ]
    assert limits == [11, 11, 21]


@pytest.mark.asyncio
async def test_industry_snapshot_returns_current_sections() -> None:
    public = RecordingPublicService()

    result = await IndustrySnapshotAggregator(
        public_data=cast(StockMarketDataService, public)
    ).snapshot()

    assert result["industries"] == {
        "money_flow": [{"name": "industry 0", "net_inflow": 10, "change_percent": 1.2}],
        "money_flow_limit": 10,
        "money_flow_truncated": False,
    }
    assert result["concepts"] == {
        "money_flow": [{"name": "concept 0", "net_inflow": 10, "change_percent": 1.2}],
        "money_flow_limit": 10,
        "money_flow_truncated": False,
    }
    assert result["top_news"] == [{"title": "市场要闻 0"}]
    assert result["top_news_limit"] == 20
    assert result["top_news_truncated"] is False
    assert "unavailable_sections" not in result
    assert [request.operation for request in public.requests].count(
        "money_flow.sector"
    ) == 2
