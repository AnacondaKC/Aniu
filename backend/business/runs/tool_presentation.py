"""Trace presentation helpers for direct MX tools."""

from __future__ import annotations

import json
from typing import Any

from backend.business.settings.public_stock_interfaces import (
    AGGREGATE_PUBLIC_STOCK_TOOL_NAMES,
)

TOOL_STEP_TITLE_BY_NAME: dict[str, str] = {
    "query_market_data": "金融数据查询",
    "search_news": "资讯搜索",
    "select_stocks": "智能选股",
    "query_portfolio": "组合查询",
    "trade": "模拟交易",
    "cancel": "撤销委托",
    "stock_quote": "实时行情",
    "query_kline": "K 线走势",
    "stock_intraday": "分时走势",
    "stock_ranking": "市场排行",
    "stock_money_flow": "资金流向",
    "stock_fundamentals": "基本数据",
    "stock_research": "研报预测",
    "stock_news": "资讯公告",
    "market_snapshot": "行情查询",
    "portfolio_stock_snapshot": "持仓查询",
    "stock_analysis": "个股查询",
    "industry_snapshot": "热度板块",
    "memory_read": "记忆查询",
    "memory_write": "记忆写入",
}

MX_TOOL_NAMES = frozenset(
    {
        "query_market_data",
        "search_news",
        "select_stocks",
        "query_portfolio",
        "trade",
        "cancel",
    }
)

PUBLIC_STOCK_TOOL_NAMES = frozenset(
    {
        "stock_quote",
        "query_kline",
        "stock_intraday",
        "stock_ranking",
        "stock_money_flow",
        "stock_fundamentals",
        "stock_research",
        "stock_news",
        "market_snapshot",
        "portfolio_stock_snapshot",
        "stock_analysis",
        "industry_snapshot",
    }
)

TRACE_TOOL_ARGUMENT_KEYS: dict[str, tuple[str, ...]] = {
    "query_market_data": ("query",),
    "search_news": ("query",),
    "select_stocks": ("keyword",),
    "query_portfolio": ("instruction", "limit", "full"),
    "trade": ("instruction",),
    "cancel": ("instruction",),
    "stock_quote": ("symbols", "detail"),
    "query_kline": (
        "instrument",
        "period",
        "adjust",
        "start_date",
        "end_date",
        "limit",
    ),
    "stock_intraday": ("symbol", "days", "limit"),
    "stock_ranking": (
        "action",
        "market",
        "sector_type",
        "sort",
        "order",
        "page",
        "limit",
    ),
    "stock_money_flow": (
        "action",
        "symbol",
        "sector_type",
        "direction",
        "page",
        "limit",
    ),
    "stock_fundamentals": ("action", "symbol", "mode", "page", "limit"),
    "stock_research": (
        "action",
        "category",
        "symbol",
        "days",
        "top",
        "index",
        "page",
        "limit",
        "content",
        "report_id",
        "mode",
    ),
    "stock_news": ("action", "feed", "symbol", "keyword", "page", "limit"),
    "market_snapshot": (),
    "portfolio_stock_snapshot": ("page",),
    "stock_analysis": ("symbol",),
    "industry_snapshot": (),
    "memory_read": ("keywords", "limit"),
    "memory_write": ("operation", "memory_id", "content", "reason"),
}


def tool_step_title(tool_name: str) -> str:
    return TOOL_STEP_TITLE_BY_NAME.get(tool_name, "工具调用")


def tool_source(tool_name: str) -> str:
    if tool_name in MX_TOOL_NAMES:
        return "mx"
    if tool_name in AGGREGATE_PUBLIC_STOCK_TOOL_NAMES:
        return "aggregate"
    if tool_name in PUBLIC_STOCK_TOOL_NAMES:
        return "public"
    return "internal"


def tool_display_name(tool_name: str) -> str:
    return TOOL_STEP_TITLE_BY_NAME.get(tool_name, tool_name or "工具调用")


def summarize_tool_arguments(arguments: dict[str, Any] | None) -> str | None:
    if not arguments:
        return None
    for key in ("query", "keyword", "instruction"):
        value = arguments.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def summarize_public_tool_arguments(
    tool_name: str, arguments: dict[str, Any] | None
) -> str | None:
    """Render only the allow-listed arguments for the public run trace."""

    if not arguments:
        return None
    allowed = TRACE_TOOL_ARGUMENT_KEYS.get(tool_name, ())
    parts: list[str] = []
    for key in allowed:
        if key not in arguments or arguments[key] is None:
            continue
        value = _format_public_argument(arguments[key])
        if value:
            parts.append(f"{key}={value}")
    if not parts:
        return None
    rendered = " · ".join(parts)
    return rendered[:240].rstrip() + ("…" if len(rendered) > 240 else "")


def _format_public_argument(value: object) -> str:
    if isinstance(value, str):
        return " ".join(value.split())[:120]
    if isinstance(value, (dict, list, tuple)):
        try:
            rendered = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        except (TypeError, ValueError):
            rendered = str(value)
        return rendered[:140].rstrip() + ("…" if len(rendered) > 140 else "")
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def summarize_tool_result(
    result: object | None,
    error: str | None,
    fallback: str | None,
) -> str | None:
    if error:
        return error
    if isinstance(result, str) and result.strip():
        return result.strip()[:120]
    if isinstance(result, dict):
        for key in ("summary", "title", "stdout", "stock_name", "symbol", "query"):
            value = result.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()[:120]
    return fallback
