from __future__ import annotations

import pytest

from backend.stock_api.public.contracts import (
    FinancialsRequest,
    ForecastRequest,
    IndexKlineRequest,
    IndexQuoteRequest,
    InvalidStockRequest,
    KlineRequest,
    NewsFeedRequest,
    QuoteSnapshotRequest,
    RatingsRequest,
    StockReportsRequest,
    ValuationRequest,
    normalize_symbol,
)


def test_headline_feed_allows_one_internal_overfetch_row() -> None:
    assert NewsFeedRequest(feed="headlines", limit=21).limit == 21
    with pytest.raises(InvalidStockRequest):
        NewsFeedRequest(feed="headlines", limit=22)


def test_valuation_and_ratings_have_no_pagination_fields() -> None:
    assert ValuationRequest("600519.SH").cache_payload() == {
        "operation": "fundamentals.valuation",
        "symbol": "600519.SH",
    }
    assert RatingsRequest("600519.SH").cache_payload() == {
        "operation": "research.ratings",
        "symbol": "600519.SH",
    }
    with pytest.raises(TypeError):
        ValuationRequest("600519.SH", page=1)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        RatingsRequest("600519.SH", limit=20)  # type: ignore[call-arg]


def test_mutually_exclusive_financial_and_forecast_modes_are_closed() -> None:
    assert FinancialsRequest("600519.SH").page is None
    quarterly = FinancialsRequest("600519.SH", mode="quarterly")
    assert (quarterly.page, quarterly.limit) == (1, 20)
    with pytest.raises(InvalidStockRequest):
        FinancialsRequest("600519.SH", page=1)
    assert ForecastRequest("600519.SH").page is None
    institutions = ForecastRequest("600519.SH", mode="institutions")
    assert (institutions.page, institutions.limit) == (1, 20)
    with pytest.raises(InvalidStockRequest):
        ForecastRequest("600519.SH", page=1)


def test_stock_report_summary_and_full_modes_require_different_fields() -> None:
    summary = StockReportsRequest("600519.SH")
    assert (summary.content, summary.page, summary.limit) == ("summary", 1, 5)
    full = StockReportsRequest("600519.SH", content="full", report_id="RPT_123")
    assert full.page is None and full.limit is None
    expanded = StockReportsRequest("600519.SH", limit=3, summary_max_characters=None)
    assert expanded.limit == 3
    assert expanded.summary_max_characters is None
    with pytest.raises(InvalidStockRequest):
        StockReportsRequest("600519.SH", summary_max_characters=0)
    with pytest.raises(InvalidStockRequest):
        StockReportsRequest("600519.SH", content="full")
    with pytest.raises(InvalidStockRequest):
        StockReportsRequest("600519.SH", report_id="RPT_123")
    with pytest.raises(InvalidStockRequest):
        StockReportsRequest("600519.SH", content="full", report_id="RPT_123", page=1)


def test_symbol_contract_completes_suffixes_and_rejects_indices() -> None:
    assert normalize_symbol("600519") == "600519.SH"
    assert normalize_symbol("000001") == "000001.SZ"
    assert normalize_symbol("600519.sh") == "600519.SH"
    with pytest.raises(InvalidStockRequest, match="沪深 A 股"):
        normalize_symbol("000001.SH")
    with pytest.raises(InvalidStockRequest):
        KlineRequest("600519.SH", start_date="2026-02-30")
    with pytest.raises(InvalidStockRequest):
        KlineRequest("600519.SH", start_date="2026-08-02", end_date="2026-08-01")


def test_index_contracts_require_explicit_exchange_and_reject_stocks() -> None:
    request = IndexQuoteRequest(("000001.SH", "399006.sz"))
    assert request.symbols == ("000001.SH", "399006.SZ")
    assert request.cache_payload() == {
        "operation": "index.quote",
        "symbols": ("000001.SH", "399006.SZ"),
    }
    kline = IndexKlineRequest("000300.SH", period="day", limit=5)
    assert kline.symbol == "000300.SH"
    assert kline.adjust == "none"
    minute = IndexKlineRequest("399006.SZ", period="1m", limit=300)
    assert minute.period == "1m"
    with pytest.raises(InvalidStockRequest, match="period"):
        IndexKlineRequest("000300.SH", period="2m")  # type: ignore[arg-type]
    with pytest.raises(InvalidStockRequest, match="格式"):
        IndexQuoteRequest(("000001",))
    with pytest.raises(InvalidStockRequest, match="沪深指数"):
        IndexKlineRequest("600519.SH")
    with pytest.raises(InvalidStockRequest):
        IndexQuoteRequest(("000001.SH", "000001.SH"))


def test_quote_limits_are_selected_by_detail() -> None:
    basic_symbols = tuple("600519.SH" for _ in range(1))
    assert QuoteSnapshotRequest(basic_symbols).detail == "basic"
    assert QuoteSnapshotRequest(("600519", "000001")).symbols == (
        "600519.SH",
        "000001.SZ",
    )
    with pytest.raises(InvalidStockRequest):
        QuoteSnapshotRequest(tuple("600519.SH" for _ in range(21)), detail="full")
    with pytest.raises(InvalidStockRequest):
        QuoteSnapshotRequest(("600519.SH", "600519.SH"))
    with pytest.raises(InvalidStockRequest):
        QuoteSnapshotRequest(("600519", "600519.SH"))
