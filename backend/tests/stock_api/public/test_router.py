"""Routing, cache, and observability behavior for public stock data."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import cast

import httpx
import pytest

import backend.stock_api.public.router as public_router_module
from backend.business.runs.abort import RunAbortSignal
from backend.business.shared import RunAbortError
from backend.business.shared.stock_api_source import stock_api_tool_context
from backend.stock_api.models import StockApiCall
from backend.stock_api.public.cancellation import CancellationToken
from backend.stock_api.public.contracts import (
    IndexKlineRequest,
    IndexQuoteRequest,
    IntradayRequest,
    KlineRequest,
    MarketBreadthRequest,
    PublicStockRequest,
    QuoteSnapshotRequest,
    SectorRankingRequest,
    StockMoneyFlowHistoryRequest,
    StockRankingRequest,
)
from backend.stock_api.public.errors import NoStockData, UpstreamUnavailable
from backend.stock_api.public.http import PublicHttpRequest, PublicHttpTransport
from backend.stock_api.public.providers.eastmoney import EastMoneyAdapter
from backend.stock_api.public.providers.sina import SinaAdapter
from backend.stock_api.public.providers.tencent import TencentAdapter
from backend.stock_api.public.router import PublicProviderAdapters, PublicStockRouter
from backend.stock_api.public.service import StockMarketDataService


@dataclass
class QuoteAdapter:
    outcome: object
    calls: list[str] = field(default_factory=list)

    async def quote_snapshot(
        self, request: QuoteSnapshotRequest, **_: object
    ) -> object:
        self.calls.append(request.operation)
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome

    async def breadth(self, request: MarketBreadthRequest, **_: object) -> object:
        self.calls.append(request.operation)
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


@dataclass
class KlineAdapter:
    outcome: object
    calls: int = 0

    async def kline(self, request: KlineRequest, **_: object) -> object:
        self.calls += 1
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


def _router(
    eastmoney: QuoteAdapter,
    tencent: QuoteAdapter,
    sina: QuoteAdapter,
) -> PublicStockRouter:
    return PublicStockRouter(
        PublicProviderAdapters(
            eastmoney=cast(EastMoneyAdapter, eastmoney),
            tencent=cast(TencentAdapter, tencent),
            sina=cast(SinaAdapter, sina),
        )
    )


def _tencent_quote(symbol: str) -> dict[str, object]:
    return {
        "quotes": [
            {
                "symbol": f"sh{symbol}",
                "name": "贵州茅台",
                "price": 1600,
                "previous_close": 1580,
                "change": 20,
                "change_percent": 1.27,
            }
        ]
    }


def _eastmoney_quote(symbol: str) -> dict[str, object]:
    return {
        "data": {
            "diff": [
                {
                    "f12": symbol,
                    "f13": 1,
                    "f14": "贵州茅台",
                    "f2": 1600,
                    "f3": 1.27,
                    "f4": 20,
                    "f5": 100,
                    "f6": 1_600_000,
                    "f15": 1605,
                    "f16": 1590,
                    "f17": 1600,
                    "f18": 1580,
                    "f86": 1786805308000,
                    "f124": 1200,
                    "f8": 2.5,
                }
            ]
        }
    }


@pytest.mark.asyncio
async def test_market_breadth_uses_eastmoney_summary_fields() -> None:
    result = await _router(
        QuoteAdapter({
            "data": {
                "diff": [
                    {"f12": "000001", "f104": 700, "f105": 1500, "f106": 50},
                    {"f12": "399001", "f104": 800, "f105": 1900, "f106": 100},
                ]
            }
        }),
        QuoteAdapter(None),
        QuoteAdapter(None),
    ).execute(MarketBreadthRequest())

    assert result["data"] == {"rising": 1500, "falling": 3400, "flat": 150}


@pytest.mark.asyncio
async def test_index_quote_falls_back_from_tencent_to_sina_before_eastmoney() -> None:
    tencent = QuoteAdapter(UpstreamUnavailable("腾讯暂时不可用。"))
    sina = QuoteAdapter(_tencent_quote("000001"))
    eastmoney = QuoteAdapter(_eastmoney_quote("000001"))

    result = await _router(eastmoney, tencent, sina).execute(
        IndexQuoteRequest(("000001.SH",))
    )

    assert result["meta"]["source"] == "sina"
    assert result["meta"]["attempted_sources"] == ["tencent", "sina"]
    assert eastmoney.calls == []


@pytest.mark.asyncio
async def test_index_quote_reaches_eastmoney_after_tencent_and_sina_fail() -> None:
    tencent = QuoteAdapter(UpstreamUnavailable("腾讯暂时不可用。"))
    sina = QuoteAdapter(UpstreamUnavailable("新浪暂时不可用。"))
    eastmoney = QuoteAdapter(_eastmoney_quote("000001"))

    result = await _router(eastmoney, tencent, sina).execute(
        IndexQuoteRequest(("000001.SH",))
    )

    assert result["meta"]["source"] == "eastmoney"
    assert result["meta"]["attempted_sources"] == [
        "tencent",
        "sina",
        "eastmoney",
    ]


@pytest.mark.asyncio
async def test_index_kline_reaches_eastmoney_after_tencent_and_sina_fail() -> None:
    tencent = KlineAdapter(UpstreamUnavailable("腾讯暂时不可用。"))
    sina = KlineAdapter(UpstreamUnavailable("新浪暂时不可用。"))
    eastmoney = KlineAdapter({"data": {"klines": ["2026-08-18,1,2,0.5,1.5,100,1000"]}})
    router = PublicStockRouter(
        PublicProviderAdapters(
            eastmoney=cast(EastMoneyAdapter, eastmoney),
            tencent=cast(TencentAdapter, tencent),
            sina=cast(SinaAdapter, sina),
        )
    )

    result = await router.execute(IndexKlineRequest("000001.SH"))

    assert result["meta"]["source"] == "eastmoney"
    assert result["meta"]["attempted_sources"] == [
        "tencent",
        "sina",
        "eastmoney",
    ]
    assert tencent.calls == 1
    assert sina.calls == 1
    assert eastmoney.calls == 1


def test_index_routes_use_tencent_sina_eastmoney_order() -> None:
    adapter = SimpleNamespace(
        quote_snapshot=lambda *_args, **_kwargs: None,
        kline=lambda *_args, **_kwargs: None,
    )
    router = PublicStockRouter(
        PublicProviderAdapters(
            eastmoney=cast(EastMoneyAdapter, adapter),
            tencent=cast(TencentAdapter, adapter),
            sina=cast(SinaAdapter, adapter),
        )
    )

    quote_sources = [
        candidate.provider
        for candidate in router.candidates_for(IndexQuoteRequest(("000001.SH",)))
    ]
    kline_sources = [
        candidate.provider
        for candidate in router.candidates_for(IndexKlineRequest("000001.SH"))
    ]
    assert quote_sources == ["tencent", "sina", "eastmoney"]
    assert kline_sources == ["tencent", "sina", "eastmoney"]


@pytest.mark.asyncio
async def test_router_uses_eastmoney_only_after_tencent_and_sina_fail() -> None:
    tencent = QuoteAdapter(UpstreamUnavailable("腾讯暂时不可用。"))
    sina = QuoteAdapter(UpstreamUnavailable("新浪暂时不可用。"))
    eastmoney = QuoteAdapter(_eastmoney_quote("600519"))

    result = await _router(eastmoney, tencent, sina).execute(
        QuoteSnapshotRequest(("600519.SH",), detail="full")
    )

    assert result["meta"] == {
        "operation": "quote.snapshot",
        "source": "eastmoney",
        "attempted_sources": ["tencent", "sina", "eastmoney"],
        "fallback_used": True,
        "degraded": False,
        "fetched_at": result["meta"]["fetched_at"],
        "warnings": ["新浪财经暂时不可用，已使用东方财富。"],
    }
    data = cast(dict[str, object], result["data"])
    assert data["unavailable_symbols"] == []
    assert tencent.calls == ["quote.snapshot"]
    assert sina.calls == ["quote.snapshot"]
    assert eastmoney.calls == ["quote.snapshot"]


@pytest.mark.asyncio
async def test_batch_quote_keeps_one_source_and_marks_missing_symbols() -> None:
    tencent = QuoteAdapter(_tencent_quote("600519"))
    eastmoney = QuoteAdapter(_eastmoney_quote("000001"))
    sina = QuoteAdapter(_tencent_quote("000001"))

    result = await _router(eastmoney, tencent, sina).execute(
        QuoteSnapshotRequest(("600519.SH", "000001.SZ"))
    )

    data = cast(dict[str, object], result["data"])
    quotes = cast(list[dict[str, object]], data["quotes"])
    assert result["meta"]["source"] == "tencent"
    assert [quote["symbol"] for quote in quotes] == ["600519.SH"]
    assert data["unavailable_symbols"] == ["000001.SZ"]
    assert eastmoney.calls == []
    assert sina.calls == []


@pytest.mark.asyncio
async def test_quote_falls_back_after_semantic_no_data_when_candidate_allows_it() -> (
    None
):
    tencent = QuoteAdapter(NoStockData("该来源没有数据。"))
    eastmoney = QuoteAdapter(_eastmoney_quote("600519"))
    sina = QuoteAdapter(_tencent_quote("600519"))

    result = await _router(eastmoney, tencent, sina).execute(
        QuoteSnapshotRequest(("600519.SH",))
    )

    assert result["meta"]["source"] == "sina"
    assert result["meta"]["fallback_used"] is True
    assert eastmoney.calls == []
    assert sina.calls == ["quote.snapshot"]


@pytest.mark.asyncio
async def test_diagnostic_provider_never_uses_a_second_source() -> None:
    tencent = QuoteAdapter(UpstreamUnavailable("腾讯暂时不可用。"))
    eastmoney = QuoteAdapter(_eastmoney_quote("600519"))
    sina = QuoteAdapter(_tencent_quote("600519"))

    with pytest.raises(UpstreamUnavailable):
        await _router(eastmoney, tencent, sina).execute(
            QuoteSnapshotRequest(("600519.SH",)),
            diagnostic_provider="tencent",
        )

    assert eastmoney.calls == []
    assert sina.calls == []


@pytest.mark.asyncio
async def test_router_does_not_share_one_request_timeout_with_internal_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_timeout = 0.01
    monkeypatch.setitem(
        public_router_module._UPSTREAM_REQUEST_TIMEOUTS, "quote", request_timeout
    )
    monkeypatch.setitem(public_router_module._TOTAL_TIMEOUTS, "quote", 0.1)

    @dataclass
    class RetryingQuoteAdapter:
        observed_timeouts: list[float] = field(default_factory=list)

        async def quote_snapshot(
            self,
            _: QuoteSnapshotRequest,
            *,
            timeout_seconds: float,
            **__: object,
        ) -> object:
            self.observed_timeouts.append(timeout_seconds)
            # Simulate a primary request timing out before a fallback succeeds.
            await asyncio.sleep(timeout_seconds * 1.5)
            return _eastmoney_quote("600519")

    eastmoney = RetryingQuoteAdapter()
    router = PublicStockRouter(
        PublicProviderAdapters(
            eastmoney=cast(EastMoneyAdapter, eastmoney),
            tencent=cast(TencentAdapter, QuoteAdapter(_tencent_quote("600519"))),
            sina=cast(SinaAdapter, QuoteAdapter(_tencent_quote("600519"))),
        )
    )

    result = await router.execute(
        QuoteSnapshotRequest(("600519.SH",)), diagnostic_provider="eastmoney"
    )

    assert result["meta"]["source"] == "eastmoney"
    assert eastmoney.observed_timeouts == [request_timeout]


@pytest.mark.asyncio
async def test_router_still_enforces_the_operation_total_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(public_router_module._UPSTREAM_REQUEST_TIMEOUTS, "quote", 0.01)
    monkeypatch.setitem(public_router_module._TOTAL_TIMEOUTS, "quote", 0.03)

    @dataclass
    class BlockingQuoteAdapter:
        async def quote_snapshot(self, *_: object, **__: object) -> object:
            await asyncio.sleep(1)
            return _eastmoney_quote("600519")

    router = PublicStockRouter(
        PublicProviderAdapters(
            eastmoney=cast(EastMoneyAdapter, BlockingQuoteAdapter()),
            tencent=cast(TencentAdapter, QuoteAdapter(_tencent_quote("600519"))),
            sina=cast(SinaAdapter, QuoteAdapter(_tencent_quote("600519"))),
        )
    )

    with pytest.raises(UpstreamUnavailable, match="公开数据请求超过总超时"):
        await router.execute(
            QuoteSnapshotRequest(("600519.SH",)), diagnostic_provider="eastmoney"
        )


@pytest.mark.asyncio
async def test_running_request_propagates_cancellation_without_fallback() -> None:
    started = asyncio.Event()

    @dataclass
    class BlockingQuoteAdapter:
        calls: int = 0

        async def quote_snapshot(
            self,
            _: QuoteSnapshotRequest,
            *,
            cancellation_token: CancellationToken | None,
            **__: object,
        ) -> object:
            self.calls += 1
            assert cancellation_token is not None
            started.set()
            await cancellation_token.wait()
            cancellation_token.throw_if_aborted()
            raise AssertionError("unreachable")

    tencent = BlockingQuoteAdapter()
    eastmoney = QuoteAdapter(_eastmoney_quote("600519"))
    sina = QuoteAdapter(_tencent_quote("600519"))
    router = PublicStockRouter(
        PublicProviderAdapters(
            eastmoney=cast(EastMoneyAdapter, eastmoney),
            tencent=cast(TencentAdapter, tencent),
            sina=cast(SinaAdapter, sina),
        )
    )
    signal = RunAbortSignal(run_id=1)
    task = asyncio.create_task(
        router.execute(QuoteSnapshotRequest(("600519.SH",)), cancellation_token=signal)
    )
    await started.wait()
    signal.abort("user requested stop")

    with pytest.raises(RunAbortError):
        await task

    assert tencent.calls == 1
    assert eastmoney.calls == []
    assert sina.calls == []


def test_supported_market_routes_prefer_tencent_and_sina() -> None:
    async def noop(*_: object, **__: object) -> object:
        return {}

    adapter = SimpleNamespace(
        quote_snapshot=noop,
        kline=noop,
        intraday=noop,
        stock_ranking=noop,
        sector_ranking=noop,
        stock_money_flow_history=noop,
    )
    router = PublicStockRouter(
        PublicProviderAdapters(
            eastmoney=cast(EastMoneyAdapter, adapter),
            tencent=cast(TencentAdapter, adapter),
            sina=cast(SinaAdapter, adapter),
        )
    )

    def providers(request: PublicStockRequest) -> list[str]:
        return [candidate.provider for candidate in router.candidates_for(request)]

    assert providers(QuoteSnapshotRequest(("600519.SH",))) == [
        "tencent",
        "sina",
        "eastmoney",
    ]
    assert providers(KlineRequest("600519.SH", period="day", adjust="none")) == [
        "tencent",
        "sina",
        "eastmoney",
    ]
    assert providers(IntradayRequest("600519.SH", days=1)) == [
        "tencent",
        "eastmoney",
    ]
    assert providers(StockRankingRequest(sort="price")) == [
        "tencent",
        "sina",
        "eastmoney",
    ]
    assert providers(SectorRankingRequest(sort="change_percent")) == [
        "sina",
        "eastmoney",
    ]
    assert providers(StockMoneyFlowHistoryRequest("600519.SH")) == [
        "sina",
        "eastmoney",
    ]


def test_one_minute_kline_has_only_eastmoney_candidate() -> None:
    async def kline(*_: object, **__: object) -> object:
        return {}

    adapter = cast(EastMoneyAdapter, SimpleNamespace(kline=kline))
    router = PublicStockRouter(
        PublicProviderAdapters(
            eastmoney=adapter,
            tencent=cast(TencentAdapter, SimpleNamespace()),
            sina=cast(SinaAdapter, SimpleNamespace()),
        )
    )

    candidates = router.candidates_for(KlineRequest("600519.SH", period="1m"))

    assert [candidate.provider for candidate in candidates] == ["eastmoney"]


def test_index_minute_routes_match_provider_capabilities() -> None:
    async def kline(*_: object, **__: object) -> object:
        return {}

    adapter = SimpleNamespace(kline=kline, index_kline_1m=kline)
    router = PublicStockRouter(
        PublicProviderAdapters(
            eastmoney=cast(EastMoneyAdapter, adapter),
            tencent=cast(TencentAdapter, adapter),
            sina=cast(SinaAdapter, adapter),
        )
    )

    five_minute = router.candidates_for(IndexKlineRequest("000001.SH", period="5m"))
    one_minute = router.candidates_for(IndexKlineRequest("000001.SH", period="1m"))
    ranged_minute = router.candidates_for(
        IndexKlineRequest("000001.SH", period="5m", start_date="2026-08-01")
    )

    assert [candidate.provider for candidate in five_minute] == [
        "sina",
        "eastmoney",
    ]
    assert [candidate.endpoint for candidate in one_minute] == ["em_index_intraday"]
    assert [candidate.provider for candidate in ranged_minute] == ["eastmoney"]


@pytest.mark.asyncio
async def test_service_cache_returns_a_deep_copy_without_a_second_route() -> None:
    @dataclass
    class CountingRouter:
        calls: int = 0

        async def execute(self, *_: object, **__: object) -> dict[str, object]:
            self.calls += 1
            return {
                "data": {"quotes": [{"symbol": "600519.SH", "price": 1600}]},
                "meta": {"source": "tencent"},
            }

    router = CountingRouter()
    service = StockMarketDataService(cast(PublicStockRouter, router))
    request = QuoteSnapshotRequest(("600519.SH",))

    first = await service.execute(request)
    cast(list[dict[str, object]], cast(dict[str, object], first["data"])["quotes"])[0][
        "price"
    ] = 1
    second = await service.execute(request)

    assert router.calls == 1
    assert cast(dict[str, object], second["data"])["quotes"] == [
        {"symbol": "600519.SH", "price": 1600}
    ]


@pytest.mark.asyncio
async def test_transport_retries_and_hides_endpoint_from_agent_trace() -> None:
    upstream_attempts = 0
    logs: list[StockApiCall] = []

    async def record(call: StockApiCall) -> None:
        logs.append(call)

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal upstream_attempts
        upstream_attempts += 1
        if upstream_attempts == 1:
            return httpx.Response(
                503, json={"message": "https://private.example.com/busy"}
            )
        return httpx.Response(200, json={"data": {"ok": True}})

    transport = PublicHttpTransport(
        transport=httpx.MockTransport(handler),
        call_logger=record,
    )
    transport._gates["tencent"].minimum_start_interval = 0
    request = PublicHttpRequest(
        provider="tencent",
        operation="em_private_endpoint",
        endpoint="private_provider_endpoint",
        url="https://example.test/quote",
        parameters={"symbol": "600519.SH"},
        headers={},
    )

    with stock_api_tool_context(
        run_id=1,
        stage_name="Research",
        tool_call_id="tool-1",
        tool_name="stock_quote",
    ) as trace_calls:
        result = await transport.request_json(request, timeout_seconds=1)

    assert result == {"data": {"ok": True}}
    assert upstream_attempts == 2
    assert [call.status for call in logs] == ["failed", "success"]
    assert all(call.parameters == {} for call in logs)
    assert all(call.operation_id == "public.request" for call in logs)
    assert logs[0].error_message == "公开数据源请求失败。"
    assert "private.example.com" not in (logs[0].error_message or "")
    assert trace_calls[0]["interface_name"] == ""
    assert trace_calls[0]["interface_identifier"] == ""
    assert trace_calls[0]["operation_id"] == "public.request"


@pytest.mark.asyncio
async def test_kline_empty_tencent_result_falls_back_to_eastmoney() -> None:
    eastmoney = KlineAdapter({"data": {"klines": ["2026-08-18,1,2,0.5,1.5,100,1000"]}})
    tencent = KlineAdapter({"code": 0, "data": {"sh600519": {"qfqday": []}}})
    sina = cast(SinaAdapter, SimpleNamespace())
    router = PublicStockRouter(
        PublicProviderAdapters(
            eastmoney=cast(EastMoneyAdapter, eastmoney),
            tencent=cast(TencentAdapter, tencent),
            sina=sina,
        )
    )

    result = await router.execute(KlineRequest("600519.SH", period="day"))

    assert result["meta"]["source"] == "eastmoney"
    assert result["meta"]["fallback_used"] is True
    assert result["meta"]["warnings"] == ["腾讯财经未返回有效数据，已使用东方财富。"]
    assert tencent.calls == 1
    assert eastmoney.calls == 1


@pytest.mark.asyncio
async def test_kline_empty_candidates_end_as_no_stock_data() -> None:
    eastmoney = KlineAdapter({"data": {"klines": []}})
    tencent = KlineAdapter({"code": 0, "data": {"sh600519": {"qfqday": []}}})
    router = PublicStockRouter(
        PublicProviderAdapters(
            eastmoney=cast(EastMoneyAdapter, eastmoney),
            tencent=cast(TencentAdapter, tencent),
            sina=cast(SinaAdapter, SimpleNamespace()),
        )
    )
    with pytest.raises(NoStockData):
        await router.execute(KlineRequest("600519.SH", period="day"))
    assert eastmoney.calls == 1
    assert tencent.calls == 1


def test_tencent_sector_candidate_only_exists_for_price_sort() -> None:
    eastmoney = cast(EastMoneyAdapter, SimpleNamespace(sector_ranking=lambda **_: None))
    tencent = cast(TencentAdapter, SimpleNamespace(sector_ranking=lambda **_: None))
    sina = cast(SinaAdapter, SimpleNamespace(sector_ranking=lambda **_: None))
    router = PublicStockRouter(
        PublicProviderAdapters(eastmoney=eastmoney, tencent=tencent, sina=sina)
    )
    change_candidates = router.candidates_for(
        SectorRankingRequest(sort="change_percent")
    )
    price_candidates = router.candidates_for(SectorRankingRequest(sort="price"))
    assert [candidate.provider for candidate in change_candidates] == [
        "sina",
        "eastmoney",
    ]
    assert [candidate.provider for candidate in price_candidates] == [
        "tencent",
        "sina",
        "eastmoney",
    ]
