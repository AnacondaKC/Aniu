from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import httpx
import pytest

from backend.stock_api.public.contracts import (
    FinancialsRequest,
    ForecastRequest,
    IndustryComparisonRequest,
    IntradayRequest,
    KlineRequest,
    OperatingIndicatorsRequest,
    QuoteSnapshotRequest,
    RatingsRequest,
    ShareholdersRequest,
    StockRankingRequest,
    StockReportsRequest,
    ValuationRequest,
)
from backend.stock_api.public.http import PublicHttpTransport
from backend.stock_api.public.normalizers.content import (
    normalize_financials,
    normalize_forecast,
    normalize_industry_comparison,
    normalize_operating_indicators,
    normalize_ratings,
    normalize_shareholders,
    normalize_valuation,
)
from backend.stock_api.public.normalizers.market import (
    normalize_intraday,
    normalize_quotes,
)
from backend.stock_api.public.providers.eastmoney import EastMoneyAdapter
from backend.stock_api.public.providers.sina import SinaAdapter
from backend.stock_api.public.providers.tencent import TencentAdapter

_FIXTURES = Path(__file__).parent / "fixtures"


def fixture(name: str) -> object:
    return json.loads((_FIXTURES / name).read_text(encoding="utf-8"))


def f10_transport(
    responses: dict[str, object],
) -> tuple[PublicHttpTransport, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        report_name = request.url.params.get("reportName", "")
        return httpx.Response(200, json=responses[report_name])

    transport = PublicHttpTransport(transport=httpx.MockTransport(handler))
    transport._gates["eastmoney_f10"].minimum_start_interval = 0
    return transport, seen


@pytest.mark.asyncio
async def test_p0_f10_fixtures_use_confirmed_fields() -> None:
    responses = {
        name: fixture(name)
        for name in (
            "financial_latest.json",
            "financial_quarterly.json",
            "shareholders.json",
            "valuation.json",
            "industry_comparison.json",
            "operating_indicators.json",
            "forecast_institutions.json",
            "web_respredict.json",
        )
    }
    transport, seen = f10_transport(
        {
            "RPT_PCF10_FINANCEMAINFINADATA": responses["financial_latest.json"],
            "RPT_F10_QTR_MAINFINADATA": responses["financial_quarterly.json"],
            "RPT_F10_EH_HOLDERNUM": responses["shareholders.json"],
            "RPT_STOCKVALUATIONTANTILE": responses["valuation.json"],
            "RPT_F10_INDUSTRY_COMPARED": responses["industry_comparison.json"],
            "RPTA_DATA_IF_INDICATOR": responses["operating_indicators.json"],
            "RPT_HSF10_RES_ORGPREDICT": responses["forecast_institutions.json"],
            "RPT_WEB_RESPREDICT": responses["web_respredict.json"],
        }
    )
    adapter = EastMoneyAdapter(transport)
    kwargs = {"timeout_seconds": 2, "cancellation_token": None}

    latest = await adapter.financials(FinancialsRequest("600519.SH"), **kwargs)
    quarterly = await adapter.financials(
        FinancialsRequest("600519.SH", mode="quarterly", page=1, limit=20), **kwargs
    )
    shareholders = await adapter.shareholders(
        ShareholdersRequest("600519.SH"), **kwargs
    )
    valuation = await adapter.valuation(ValuationRequest("600519.SH"), **kwargs)
    industry = await adapter.industry_comparison(
        IndustryComparisonRequest("600519.SH"), **kwargs
    )
    indicators = await adapter.operating_indicators(
        OperatingIndicatorsRequest("600519.SH"), **kwargs
    )
    institutions = await adapter.forecast(
        ForecastRequest("600519.SH", mode="institutions", page=1, limit=20), **kwargs
    )
    summary = await adapter.forecast(ForecastRequest("600519.SH"), **kwargs)
    ratings = await adapter.ratings(RatingsRequest("600519.SH"), **kwargs)

    assert cast(dict[str, object], latest)["items"]
    assert cast(dict[str, object], quarterly)["items"]
    assert cast(dict[str, object], shareholders)["items"]
    assert len(cast(list[object], cast(dict[str, object], valuation)["items"])) == 16
    assert cast(dict[str, object], industry)["items"]
    assert cast(dict[str, object], indicators)["items"]
    assert cast(dict[str, object], institutions)["items"]
    assert cast(dict[str, object], summary)["item"]
    assert cast(dict[str, object], ratings)["item"]
    assert {request.url.params.get("reportName") for request in seen} == {
        "RPT_PCF10_FINANCEMAINFINADATA",
        "RPT_F10_QTR_MAINFINADATA",
        "RPT_F10_EH_HOLDERNUM",
        "RPT_STOCKVALUATIONTANTILE",
        "RPT_F10_INDUSTRY_COMPARED",
        "RPTA_DATA_IF_INDICATOR",
        "RPT_HSF10_RES_ORGPREDICT",
        "RPT_WEB_RESPREDICT",
    }

    assert (
        normalize_financials(
            {"items": cast(dict[str, object], latest)["items"]},
            FinancialsRequest("600519.SH"),
        ).data["item"]["revenue"]
        == 123456789.0
    )
    assert normalize_financials(
        {"items": cast(dict[str, object], quarterly)["items"]},
        FinancialsRequest("600519.SH", mode="quarterly", page=1, limit=20),
    ).data["items"]
    assert normalize_shareholders(
        {"items": cast(dict[str, object], shareholders)["items"]},
        ShareholdersRequest("600519.SH"),
    ).data["items"]
    assert (
        len(
            normalize_valuation(
                {"items": cast(dict[str, object], valuation)["items"]},
                ValuationRequest("600519.SH"),
            ).data["items"]
        )
        == 16
    )
    assert normalize_industry_comparison(
        {"items": cast(dict[str, object], industry)["items"]},
        IndustryComparisonRequest("600519.SH"),
    ).data["items"]
    assert normalize_operating_indicators(
        {"items": cast(dict[str, object], indicators)["items"]},
        OperatingIndicatorsRequest("600519.SH"),
    ).data["items"]
    assert normalize_forecast(
        cast(dict[str, object], summary), ForecastRequest("600519.SH")
    ).data["estimates"]
    assert normalize_forecast(
        cast(dict[str, object], institutions),
        ForecastRequest("600519.SH", mode="institutions", page=1, limit=20),
    ).data["items"]
    assert (
        normalize_ratings(
            cast(dict[str, object], ratings), RatingsRequest("600519.SH")
        ).data["buy"]
        == 8
    )


@pytest.mark.asyncio
async def test_stock_reports_support_three_untruncated_summaries() -> None:
    seen: list[httpx.Request] = []
    contents = {
        "RPT_1": "甲" * 1_200,
        "RPT_2": "乙" * 1_500,
        "RPT_3": "丙" * 1_800,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path.endswith("/api/security/rep"):
            return httpx.Response(
                200,
                json={
                    "data": {
                        "total_hits": 3,
                        "list": [
                            {"art_code": report_id, "title": report_id}
                            for report_id in contents
                        ],
                    }
                },
            )
        if request.url.path.endswith("/api/content/rep"):
            report_id = request.url.params["art_code"]
            return httpx.Response(
                200,
                json={"data": {"notice_content": contents[report_id]}},
            )
        raise AssertionError(f"unexpected path: {request.url.path}")

    transport = PublicHttpTransport(transport=httpx.MockTransport(handler))
    adapter = EastMoneyAdapter(transport)
    raw = await adapter.stock_reports(
        StockReportsRequest("600519.SH", limit=3, summary_max_characters=None),
        timeout_seconds=2,
        cancellation_token=None,
    )

    reports = cast(list[dict[str, object]], raw["reports"])
    assert [len(cast(str, report["summary"])) for report in reports] == [
        1_200,
        1_500,
        1_800,
    ]
    assert all(report["summary_truncated"] is False for report in reports)
    list_request = next(
        request for request in seen if request.url.path.endswith("/api/security/rep")
    )
    assert list_request.url.params["page_size"] == "3"
    await transport.aclose()


@pytest.mark.asyncio
async def test_fallback_provider_parameters_and_parsing_without_network() -> None:
    seen: list[httpx.Request] = []
    tencent_quote = (_FIXTURES / "tencent_quote.txt").read_text(encoding="utf-8")

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        path = request.url.path
        if request.url.host == "qt.gtimg.cn":
            return httpx.Response(200, content=tencent_quote.encode("gbk"))
        if path.endswith("/fqkline/get"):
            return httpx.Response(200, json=fixture("tencent_kline.json"))
        if path.endswith("/minute/query"):
            return httpx.Response(200, json=fixture("tencent_intraday.json"))
        if "getBoardRankList" in path:
            return httpx.Response(200, json=fixture("tencent_ranking.json"))
        if "getHQNodeData" in path:
            return httpx.Response(200, json=fixture("sina_ranking.json"))
        if path.endswith("/stock/get"):
            payload = cast(dict[str, object], fixture("eastmoney_single_quote.json"))
            return httpx.Response(200, json={"rc": 0, **payload})
        if path.endswith("/ulist.np/get"):
            payload = cast(dict[str, object], fixture("eastmoney_batch_quote.json"))
            return httpx.Response(200, json={"rc": 0, **payload})
        raise AssertionError(f"unexpected path: {path}")

    transport = PublicHttpTransport(transport=httpx.MockTransport(handler))
    transport._gates["eastmoney_push"].minimum_start_interval = 0
    transport._gates["tencent"].minimum_start_interval = 0
    transport._gates["sina"].minimum_start_interval = 0
    eastmoney = EastMoneyAdapter(transport)
    tencent = TencentAdapter(transport)
    sina = SinaAdapter(transport)
    kwargs = {"timeout_seconds": 2, "cancellation_token": None}

    quote_request = QuoteSnapshotRequest(("600519.SH", "000001.SZ"), detail="full")
    quote_raw = await eastmoney.quote_snapshot(quote_request, **kwargs)
    single_raw = await eastmoney.quote_snapshot(
        QuoteSnapshotRequest(("600519.SH",), detail="full"), **kwargs
    )
    kline_raw = await tencent.kline(
        KlineRequest("600519.SH", period="day", limit=10),
        **kwargs,
    )
    intraday_raw = await tencent.intraday(IntradayRequest("600519.SH"), **kwargs)
    tencent_rank_raw = await tencent.stock_ranking(
        StockRankingRequest(sort="amount", page=1, limit=10), **kwargs
    )
    sina_rank_raw = await sina.stock_ranking(
        StockRankingRequest(sort="change_percent", page=1, limit=10), **kwargs
    )

    normalized_quote = normalize_quotes("eastmoney", quote_raw, quote_request)
    single_quote = normalize_quotes(
        "eastmoney",
        single_raw,
        QuoteSnapshotRequest(("600519.SH",), detail="full"),
    ).data["quotes"][0]
    assert single_quote["market_time"] == "2026-08-15T14:48:28+00:00"
    assert single_quote["high"] == 1605
    assert single_quote["low"] == 1590
    assert single_quote["open"] == 1600
    assert single_quote["turnover_rate"] == 2.5
    assert single_quote["volume_shares"] == 10_000
    assert single_quote["amount"] == 160_000
    assert normalized_quote.data["quotes"]
    quote = cast(list[dict[str, object]], normalized_quote.data["quotes"])[0]
    assert quote["high"] == 1605
    assert quote["low"] == 1590
    assert quote["open"] == 1600
    assert quote["previous_close"] == 1580
    assert quote["turnover_rate"] == 2.5
    assert quote["market_time"] == "2026-08-15T14:48:28+00:00"
    assert cast(dict[str, object], kline_raw)["data"]
    assert cast(dict[str, object], intraday_raw)["previous_close"] == 1580.0
    intraday = normalize_intraday(
        "tencent", intraday_raw, IntradayRequest("600519.SH", limit=2)
    )
    points = cast(list[dict[str, object]], intraday.data["points"])
    assert intraday.data["previous_close"] == 1580.0
    assert points[-1]["average_price"] == 1601.0
    assert cast(dict[str, object], tencent_rank_raw)["data"]
    assert isinstance(sina_rank_raw, list)

    urls = [str(request.url) for request in seen]
    assert any("secids=1.600519" in url for url in urls)
    assert any("param=sh600519%2Cday" in url for url in urls)
    assert any("sort_type=turnover" in url for url in urls)
    assert any("sort=changepercent" in url for url in urls)
