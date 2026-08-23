from __future__ import annotations

import pytest

from backend.stock_api.public.contracts import (
    FinancialsRequest,
    ForecastRequest,
    IndexKlineRequest,
    IndexQuoteRequest,
    IndustryComparisonRequest,
    KlineRequest,
    MarketReportsRequest,
    NewsFeedRequest,
    OperatingIndicatorsRequest,
    RatingsRequest,
    SectorRankingRequest,
    ShareholdersRequest,
    StockNewsRequest,
    StockRankingRequest,
    StockReportsRequest,
    ValuationRequest,
)
from backend.stock_api.public.errors import NoStockData, UpstreamUnavailable
from backend.stock_api.public.normalizers.content import (
    normalize_financials,
    normalize_forecast,
    normalize_industry_comparison,
    normalize_market_reports,
    normalize_news_feed,
    normalize_operating_indicators,
    normalize_ratings,
    normalize_shareholders,
    normalize_stock_news,
    normalize_stock_reports,
    normalize_valuation,
)
from backend.stock_api.public.normalizers.market import (
    normalize_breadth,
    normalize_index_quotes,
    normalize_kline,
    normalize_ranking,
)


def test_index_quotes_accept_index_symbols_and_keep_partial_batches() -> None:
    request = IndexQuoteRequest(("000001.SH", "399006.SZ"))
    raw = {
        "quotes": [
            {
                "symbol": "sh000001",
                "name": "上证指数",
                "price": 3982.65,
                "previous_close": 3927.18,
                "volume_shares": 489_834_027,
            },
            {
                "symbol": "sz399006",
                "name": "创业板指",
                "price": 3210.0,
                "previous_close": 3180.0,
                "volume_shares": 100,
            },
        ]
    }

    result = normalize_index_quotes("sina", raw, request)
    quotes = result.data["quotes"]
    assert isinstance(quotes, list)
    assert [quote["symbol"] for quote in quotes] == ["000001.SH", "399006.SZ"]
    assert quotes[0]["volume_shares"] == 48_983_402_700

    partial = normalize_index_quotes("sina", {"quotes": raw["quotes"][:1]}, request)

    assert partial.data["unavailable_symbols"] == ["399006.SZ"]
    assert partial.degraded is True
    assert "未返回有效数据：399006.SZ。" in partial.warnings


def test_eastmoney_index_trends_normalize_as_one_minute_kline() -> None:
    result = normalize_kline(
        "eastmoney",
        {
            "data": {
                "trends": [
                    (
                        "2026-08-17 09:31,3931.77,3925.88,3931.84,3925.88,"
                        "13440436,28114665984.00,3925.554"
                    )
                ]
            }
        },
        IndexKlineRequest("000001.SH", period="1m", limit=5),
    )

    assert result.data["bars"] == [
        {
            "time": "2026-08-17 09:31",
            "open": 3931.77,
            "high": 3931.84,
            "low": 3925.88,
            "close": 3925.88,
            "volume_shares": 13_440_436.0,
            "amount": 28_114_665_984.0,
        }
    ]


def test_financials_validate_business_fields_and_keep_zero_or_negative_values() -> None:
    valid = {
        "REPORT_DATE": "20260816",
        "TOTAL_OPERATEINCOME": 0,
        "PARENT_NETPROFIT": -1,
    }
    result = normalize_financials({"items": [valid]}, FinancialsRequest("600519.SH"))
    item = result.data["item"]
    assert item["report_period"] == "2026-08-16"
    assert item["revenue"] == 0
    assert item["net_profit"] == -1

    with pytest.raises(UpstreamUnavailable):
        normalize_financials(
            {"items": [{"REPORT_DATE": "--", "TOTAL_OPERATEINCOME": "--"}]},
            FinancialsRequest("600519.SH"),
        )

    quarterly = normalize_financials(
        {
            "total": 2,
            "items": [
                {
                    "REPORT_DATE": "2026-03-31",
                    "TOTALOPERATEREVE": 100,
                    "PARENTNETPROFIT": 20,
                },
                {
                    "REPORT_DATE": "--",
                    "TOTALOPERATEREVE": "--",
                    "PARENTNETPROFIT": "--",
                },
            ],
        },
        FinancialsRequest("600519.SH", mode="quarterly", page=1, limit=20),
    )
    assert len(quarterly.data["items"]) == 1
    assert quarterly.degraded is True
    assert "TOTALOPERATEREVE" not in quarterly.warnings[0]

    with pytest.raises(NoStockData):
        normalize_financials({"items": []}, FinancialsRequest("600519.SH"))


def test_valuation_distinguishes_null_applicability_from_missing_fields() -> None:
    null_row = {
        "SECUCODE": "600519.SH",
        "INDEX_TYPE": 1,
        "STATISTICS_CYCLE": 1,
        "PERCENTILE_THIRTY": None,
        "PERCENTILE_FIFTY": None,
        "PERCENTILE_SEVENTY": None,
    }
    with pytest.raises(NoStockData):
        normalize_valuation({"items": [null_row]}, ValuationRequest("600519.SH"))

    with pytest.raises(UpstreamUnavailable):
        normalize_valuation(
            {
                "items": [
                    {
                        "SECUCODE": "600519.SH",
                        "INDEX_TYPE": 1,
                        "STATISTICS_CYCLE": 1,
                        "PERCENTILE_THIRTY": 20,
                    }
                ]
            },
            ValuationRequest("600519.SH"),
        )

    mixed = normalize_valuation(
        {
            "items": [
                {
                    "SECUCODE": "600519.SH",
                    "INDEX_TYPE": 1,
                    "STATISTICS_CYCLE": 1,
                    "PERCENTILE_THIRTY": 20,
                    "PERCENTILE_FIFTY": 25,
                    "PERCENTILE_SEVENTY": 30,
                },
                null_row | {"STATISTICS_CYCLE": 2},
            ]
        },
        ValuationRequest("600519.SH"),
    )
    assert mixed.degraded is True
    assert mixed.data["items"][0]["metric"] == "pe"


def test_forecast_and_ratings_use_confirmed_shapes_without_fabricated_values() -> None:
    summary = normalize_forecast(
        {
            "item": {
                "RATING_ORG_NUM": 0,
                "DEC_AIMPRICEMIN": None,
                "DEC_AIMPRICEMAX": None,
                "YEAR1": 2026,
                "YEAR_MARK1": "X",
                "EPS1": 0,
            }
        },
        ForecastRequest("600519.SH"),
    )
    assert summary.data["institution_count"] == 0
    assert summary.data["target_price_min"] is None
    assert summary.data["estimates"] == [{"year": 2026, "kind": "unknown", "eps": 0}]
    assert "pe" not in summary.data["estimates"][0]

    institutions = normalize_forecast(
        {
            "items": [
                {
                    "ORG_NAME_ABBR": "有效机构",
                    "PUBLISH_DATE": None,
                    "YEAR1": 2026,
                    "YEAR_MARK1": "E",
                    "EPS1": 1.2,
                    "PE1": 10,
                },
                {
                    "ORG_NAME_ABBR": "坏机构",
                    "PUBLISH_DATE": "2026-08-16",
                    "YEAR1": 2026,
                    "EPS1": None,
                },
            ]
        },
        ForecastRequest("600519.SH", mode="institutions", page=1, limit=20),
    )
    assert len(institutions.data["items"]) == 1
    assert institutions.data["items"][0]["estimates"][0]["pe"] == 10
    assert institutions.degraded is True

    ratings = normalize_ratings(
        {
            "item": {
                "RATING_ORG_NUM": 0,
                "RATING_BUY_NUM": 0,
                "RATING_ADD_NUM": None,
                "RATING_NEUTRAL_NUM": None,
                "RATING_REDUCE_NUM": None,
                "RATING_SALE_NUM": None,
                "RATING_LONG_NUM": 0,
                "DEC_AIMPRICEMIN": None,
                "DEC_AIMPRICEMAX": None,
            }
        },
        RatingsRequest("600519.SH"),
    )
    assert ratings.data["buy"] == 0
    assert ratings.data["increase"] is None
    assert "items" not in ratings.data


def test_research_outputs_clean_urls_and_only_business_fields() -> None:
    summary = normalize_stock_reports(
        {
            "reports": [
                {
                    "art_code": "RPT_1",
                    "title": "研报",
                    "publish_date": "2026-08-16 00:00:00",
                    "summary": "正文 https://example.test/report",
                    "author_items": [{"author_name": "甲"}, {"author_name": "乙"}],
                    "rating": 1,
                    "rating_change": 1,
                }
            ],
            "total": 1,
        },
        StockReportsRequest("600519.SH"),
    )
    report = summary.data["reports"][0]
    assert report["summary"] == "正文 [链接已省略]"
    assert report["author"] == "甲、乙"
    assert report["rating_change"] is None
    assert "url" not in report and "attach_url" not in report

    complete_summary = normalize_stock_reports(
        {
            "reports": [
                {
                    "art_code": "RPT_2",
                    "title": "完整研报",
                    "publish_date": "2026-08-16 00:00:00",
                    "summary": "研" * 1_500,
                }
            ],
            "total": 1,
        },
        StockReportsRequest("600519.SH", limit=3, summary_max_characters=None),
    ).data["reports"][0]
    assert len(complete_summary["summary"]) == 1_500
    assert complete_summary["summary_truncated"] is False

    full = normalize_stock_reports(
        {
            "found": True,
            "report_id": "RPT_1",
            "content": "全文 http://example.test/a",
        },
        StockReportsRequest("600519.SH", content="full", report_id="RPT_1"),
    )
    assert full.data["report"]["content"] == "全文 [链接已省略]"
    with pytest.raises(NoStockData):
        normalize_stock_reports(
            {"found": "false", "content": "not usable"},
            StockReportsRequest("600519.SH", content="full", report_id="RPT_1"),
        )


def test_news_normalizer_drops_urls_and_rejects_empty_records() -> None:
    result = normalize_news_feed(
        "eastmoney",
        {
            "items": [
                {
                    "title": "标题",
                    "published_at": "2026-08-16 09:00:00",
                    "source": "来源",
                    "url": "https://example.test/news",
                    "summary": "摘要",
                }
            ],
            "total": 1,
        },
        NewsFeedRequest(feed="headlines", page=1, limit=5),
    )
    item = result.data["items"][0]
    assert item["title"] == "标题"
    assert "url" not in item
    assert "https://" not in str(result.data)

    with pytest.raises(NoStockData):
        normalize_news_feed("eastmoney", {"items": []}, NewsFeedRequest())


def test_content_lists_require_identity_and_measurements() -> None:
    industry = normalize_industry_comparison(
        {
            "items": [
                {
                    "CORRE_SECUCODE": "600519.SH",
                    "CORRE_SECURITY_NAME": "同行",
                    "TOTALOPERATEREVE": 0,
                    "INDUSTRY": "白酒",
                },
                {"CORRE_SECURITY_NAME": "缺代码", "TOTALOPERATEREVE": 10},
            ],
            "total": 2,
        },
        IndustryComparisonRequest("600519.SH", page=1, limit=20),
    )
    assert industry.data["items"][0]["revenue"] == 0
    assert industry.degraded is True

    operating = normalize_operating_indicators(
        {
            "items": [
                {"INDICATOR_NAME": "指标", "VALUE": "0"},
                {"INDICATOR_NAME": "无效", "VALUE": "--"},
            ]
        },
        OperatingIndicatorsRequest("600519.SH", page=1, limit=20),
    )
    assert operating.data["items"][0]["name"] == "指标"
    assert operating.data["items"][0]["value"] == 0
    assert operating.degraded is True


def test_breadth_sums_shanghai_and_shenzhen_totals() -> None:
    result = normalize_breadth(
        "eastmoney",
        {
            "data": {
                "diff": [
                    {"f12": "000001", "f104": 718, "f105": 1574, "f106": 59},
                    {"f12": "399001", "f104": 858, "f105": 1975, "f106": 99},
                ]
            }
        },
    )
    assert result.data == {"rising": 1576, "falling": 3549, "flat": 158}


def test_ranking_requires_a_valid_requested_sort_field() -> None:
    request = StockRankingRequest(
        market="all_a", sort="change_percent", order="desc", page=1, limit=20
    )
    with pytest.raises(UpstreamUnavailable):
        normalize_ranking(
            "eastmoney",
            {"data": {"diff": [{"f12": "600519", "f14": "茅台", "f2": None}]}},
            request,
            sectors=False,
            paginate_locally=False,
        )

    sector_request = SectorRankingRequest(
        sector_type="industry", sort="price", order="asc", page=1, limit=20
    )
    result = normalize_ranking(
        "sina",
        [{"name": "行业", "price": 10}],
        sector_request,
        sectors=True,
        paginate_locally=True,
    )
    assert result.data["items"][0]["price"] == 10
    volume_result = normalize_ranking(
        "sina",
        [
            {"name": "大", "volume": 20},
            {"name": "缺失", "volume": None},
            {"name": "小", "volume": 10},
        ],
        SectorRankingRequest(
            sector_type="industry", sort="volume", order="asc", page=1, limit=20
        ),
        sectors=True,
        paginate_locally=True,
    )
    volume_items = volume_result.data["items"]
    assert [item["name"] for item in volume_items] == ["小", "大", "缺失"]


def test_shareholder_zero_is_valid_and_confirmed_change_field_is_used() -> None:
    result = normalize_shareholders(
        {
            "items": [
                {
                    "END_DATE": "2026-06-30",
                    "HOLDER_TOTAL_NUM": 0,
                    "AVG_FREE_SHARES": 0,
                    "HOLDER_TOTAL_NUMCHANGE": 0,
                    "CHANGEWITHLAST": 999,
                }
            ]
        },
        ShareholdersRequest("600519.SH", page=1, limit=20),
    )
    assert result.data["items"][0]["shareholder_count"] == 0
    assert result.data["items"][0]["change"] == 0


def test_stock_news_and_generic_news_preserve_pagination_identity() -> None:
    request = StockNewsRequest("600519.SH", page=2, limit=5)
    result = normalize_stock_news(
        "eastmoney",
        {
            "items": [
                {
                    "title": "个股新闻",
                    "published_at": "2026-08-16",
                    "source": "来源",
                    "url": "https://example.test",
                }
            ],
            "total": 6,
        },
        request,
    )
    assert result.data["symbol"] == "600519.SH"
    assert result.data["page"] == 2
    assert result.data["limit"] == 5


def test_eastmoney_kline_distinguishes_malformed_and_empty_envelopes() -> None:
    request = KlineRequest("600519.SH")
    with pytest.raises(UpstreamUnavailable):
        normalize_kline("eastmoney", {"data": {}}, request)
    with pytest.raises(UpstreamUnavailable):
        normalize_kline("eastmoney", {}, request)
    with pytest.raises(NoStockData):
        normalize_kline("eastmoney", {"data": {"klines": []}}, request)


def test_market_reports_can_preserve_full_content_for_aggregate_queries() -> None:
    content = "研报正文" * 1_001
    raw = {
        "reports": [{"title": "策略研报"}],
        "contents": [{"title": "策略研报", "content": content}],
    }

    bounded = normalize_market_reports(raw, MarketReportsRequest())
    unbounded = normalize_market_reports(
        raw,
        MarketReportsRequest(content_max_characters=None),
    )

    bounded_content = bounded.data["contents"][0]
    unbounded_content = unbounded.data["contents"][0]
    assert len(bounded_content["content"]) == 4_000
    assert bounded_content["content_truncated"] is True
    assert unbounded_content["content"] == content
    assert unbounded_content["content_truncated"] is False
