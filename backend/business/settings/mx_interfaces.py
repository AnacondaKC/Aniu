"""Stable MX interface catalog."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MxInterfaceDescriptor:
    """One user-visible MX interface backed by one or more upstream operations."""

    interface_id: str
    name: str
    summary: str
    features: tuple[str, ...]
    examples: tuple[str, ...]
    access_modes: tuple[str, ...]
    operation_ids: tuple[str, ...]


MX_INTERFACE_CATALOG: tuple[MxInterfaceDescriptor, ...] = (
    MxInterfaceDescriptor(
        interface_id="news",
        name="资讯搜索",
        summary="检索最新财经新闻、公司公告、研报和政策信息，辅助研究阶段的信息收集。",
        features=("财经新闻", "公司公告", "机构研报", "政策信息"),
        examples=("半导体行业最新研报", "隆基绿能发布的公告", "新能源汽车补贴政策"),
        access_modes=("read",),
        operation_ids=("search_news",),
    ),
    MxInterfaceDescriptor(
        interface_id="data",
        name="金融数据查询",
        summary="用自然语言查询股票、指数、行情、财务和股权数据，返回结构化研究结果。",
        features=("股票行情", "指数行情", "财务数据", "股权结构"),
        examples=("贵州茅台最新股价", "上证指数今日行情", "宁德时代最近一期财报"),
        access_modes=("read",),
        operation_ids=("query_market_data",),
    ),
    MxInterfaceDescriptor(
        interface_id="screening",
        name="智能选股",
        summary="按自然语言条件筛选 A 股或板块成分股，输出符合条件的候选标的列表。",
        features=("条件选股", "板块成分筛选"),
        examples=("市盈率低于 15 的消费股", "最近三日主力资金净流入的股票"),
        access_modes=("read",),
        operation_ids=("select_stocks",),
    ),
    MxInterfaceDescriptor(
        interface_id="portfolio",
        name="模拟交易",
        summary=(
            "查询模拟组合并提交交易或撤单；委托、订单与成交默认返回最后 50 条，"
            "full 可返回全量。"
        ),
        features=(
            "资金与资产",
            "持仓明细",
            "委托、订单与成交",
            "买入与卖出",
            "撤单",
        ),
        examples=(
            "查询账户资金",
            "查询我的持仓",
            "查询最近 50 条委托",
            "查询全部成交",
            "买入 600519 1700 100",
            "撤单 123456",
        ),
        access_modes=("read", "write"),
        operation_ids=(
            "query_portfolio.balance",
            "query_portfolio.positions",
            "query_portfolio.orders",
            "trade",
            "cancel",
        ),
    ),
)


__all__ = [
    "MX_INTERFACE_CATALOG",
    "MxInterfaceDescriptor",
]
