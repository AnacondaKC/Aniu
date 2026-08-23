"""Read-only catalog for the built-in public A-share Agent tools."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

type PublicStockProvider = Literal["eastmoney", "tencent", "sina", "mx"]

AGGREGATE_PUBLIC_STOCK_TOOL_NAMES = frozenset(
    {
        "market_snapshot",
        "portfolio_stock_snapshot",
        "stock_analysis",
        "industry_snapshot",
    }
)


@dataclass(frozen=True, slots=True)
class PublicStockToolCatalogItem:
    tool_name: str
    name: str
    summary: str
    actions: tuple[str, ...]
    providers: tuple[PublicStockProvider, ...]


PUBLIC_STOCK_DATA_NAME = "公开数据"
PUBLIC_STOCK_DATA_SUMMARY = (
    "个股与指数行情无需密钥并自动选择腾讯、新浪和东方财富；"
    "聚合研判工具按业务场景组合标准化公开数据。"
)
PUBLIC_STOCK_DATA_PROVIDERS: tuple[PublicStockProvider, ...] = (
    "tencent",
    "sina",
    "eastmoney",
)
PUBLIC_STOCK_DATA_FEATURES = (
    "实时行情",
    "K 线与分时",
    "资金流向",
    "基本数据",
    "研报预测",
    "资讯公告",
    "聚合研判",
)
PUBLIC_STOCK_TOOL_CATALOG = (
    PublicStockToolCatalogItem(
        tool_name="stock_quote",
        name="实时行情",
        summary="查询一只或多只沪深 A 股的标准化实时行情快照。",
        actions=("行情快照",),
        providers=("tencent", "sina", "eastmoney"),
    ),
    PublicStockToolCatalogItem(
        tool_name="query_kline",
        name="K 线走势",
        summary="统一查询个股与指数的日、周、月及分钟 K 线。",
        actions=("个股 K 线", "指数 K 线"),
        providers=("tencent", "sina", "eastmoney"),
    ),
    PublicStockToolCatalogItem(
        tool_name="stock_intraday",
        name="分时走势",
        summary="查询当日或近五日的分时价格、均价与累计成交数据。",
        actions=("分时",),
        providers=("tencent", "eastmoney"),
    ),
    PublicStockToolCatalogItem(
        tool_name="stock_ranking",
        name="市场排行",
        summary="查询个股和行业、概念板块的排序结果。",
        actions=("个股排行", "板块排行"),
        providers=("tencent", "sina", "eastmoney"),
    ),
    PublicStockToolCatalogItem(
        tool_name="stock_money_flow",
        name="资金流向",
        summary="查询个股、板块及沪深港通资金流。",
        actions=("个股历史", "个股分时", "板块", "沪深港通"),
        providers=("sina", "eastmoney"),
    ),
    PublicStockToolCatalogItem(
        tool_name="stock_fundamentals",
        name="基本数据",
        summary="查询财务、股东、估值、行业对比和经营指标。",
        actions=("财务", "股东", "估值", "行业对比", "经营指标"),
        providers=("eastmoney",),
    ),
    PublicStockToolCatalogItem(
        tool_name="stock_research",
        name="研报预测",
        summary="查询市场研报、个股研报、盈利预测和机构评级。",
        actions=("市场研报", "个股研报", "盈利预测", "机构评级"),
        providers=("eastmoney",),
    ),
    PublicStockToolCatalogItem(
        tool_name="stock_news",
        name="资讯公告",
        summary="查询资讯流、个股新闻、公司公告和新闻搜索。",
        actions=("资讯流", "个股新闻", "公告", "新闻搜索"),
        providers=("eastmoney",),
    ),
    PublicStockToolCatalogItem(
        tool_name="market_snapshot",
        name="行情查询",
        summary="聚合七大指数实时行情、近 5 根日 K 线与近期市场策略研报。",
        actions=("七大指数", "日 K 线", "策略研报"),
        providers=("tencent", "sina", "eastmoney"),
    ),
    PublicStockToolCatalogItem(
        tool_name="portfolio_stock_snapshot",
        name="持仓查询",
        summary="按持仓市值分页聚合模拟组合的行情、K 线、资金流、财务和资讯。",
        actions=("持仓分页", "个股研判", "ETF 行情资讯"),
        providers=("tencent", "sina", "eastmoney", "mx"),
    ),
    PublicStockToolCatalogItem(
        tool_name="stock_analysis",
        name="个股查询",
        summary="聚合单只 A 股的行情、资金、基本数据、研报预测和资讯。",
        actions=("行情 K 线", "资金财务", "研报资讯"),
        providers=("tencent", "sina", "eastmoney"),
    ),
    PublicStockToolCatalogItem(
        tool_name="industry_snapshot",
        name="热度板块",
        summary="聚合行业和概念板块当日资金流前 10 名与市场要闻前 20 条。",
        actions=("行业资金流", "概念资金流", "市场要闻"),
        providers=("eastmoney",),
    ),
)


__all__ = [
    "AGGREGATE_PUBLIC_STOCK_TOOL_NAMES",
    "PUBLIC_STOCK_DATA_FEATURES",
    "PUBLIC_STOCK_DATA_NAME",
    "PUBLIC_STOCK_DATA_PROVIDERS",
    "PUBLIC_STOCK_DATA_SUMMARY",
    "PUBLIC_STOCK_TOOL_CATALOG",
    "PublicStockToolCatalogItem",
    "PublicStockProvider",
]
