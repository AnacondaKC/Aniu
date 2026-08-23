"""Catalog coverage for data-interface tool invocation logging."""

from __future__ import annotations

from backend.business.stock_api_logs.catalog import (
    STOCK_API_TOOL_CATALOG,
    stock_api_tool_descriptor,
)


def test_tool_log_catalog_matches_the_eight_plus_four_plus_four_directory_cards() -> (
    None
):
    assert len(STOCK_API_TOOL_CATALOG) == 16
    assert sum(item.tool_source == "public" for item in STOCK_API_TOOL_CATALOG) == 8
    assert sum(item.tool_source == "aggregate" for item in STOCK_API_TOOL_CATALOG) == 4
    assert sum(item.tool_source == "mx" for item in STOCK_API_TOOL_CATALOG) == 4

    query_kline = stock_api_tool_descriptor("query_kline")
    market_snapshot = stock_api_tool_descriptor("market_snapshot")
    portfolio_snapshot = stock_api_tool_descriptor("portfolio_stock_snapshot")
    portfolio = stock_api_tool_descriptor("query_portfolio")
    trade = stock_api_tool_descriptor("trade")
    cancel = stock_api_tool_descriptor("cancel")

    assert query_kline is not None
    assert (
        query_kline.tool_source,
        query_kline.tool_id,
        query_kline.tool_name,
    ) == ("public", "query_kline", "K 线走势")
    assert market_snapshot is not None
    assert (
        market_snapshot.tool_source,
        market_snapshot.tool_id,
        market_snapshot.tool_name,
    ) == ("aggregate", "market_snapshot", "行情查询")
    assert portfolio_snapshot is not None
    assert (
        portfolio_snapshot.tool_source,
        portfolio_snapshot.tool_id,
        portfolio_snapshot.tool_name,
    ) == ("aggregate", "portfolio_stock_snapshot", "持仓查询")
    assert portfolio is not None
    assert (portfolio.tool_source, portfolio.tool_id, portfolio.tool_name) == (
        "mx",
        "portfolio",
        "模拟交易",
    )
    assert trade == portfolio
    assert cancel == portfolio
