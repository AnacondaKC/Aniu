from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from app.services.mx_service import (
    MXClient,
    extract_available_balance,
    extract_candidates,
)


_ORDER_STATUS_MAP: dict[str, str] = {
    "0": "未知", "1": "已报", "2": "已报", "3": "已撤单",
    "4": "已成交", "8": "未成交", "9": "已撤单",
    "100": "处理中", "200": "已完成", "206": "已撤单",
}

_ERROR_HINTS: tuple[tuple[str, str], ...] = (
    ("401", "API Key 可能错误、失效或未正确配置，请检查 MX_APIKEY。"),
    ("API密钥不存在", "API Key 可能错误、失效或未正确配置，请检查 MX_APIKEY。"),
    ("code=113", "今日调用次数可能已达上限，请前往妙想 Skills 页面获取更多调用次数。"),
    ("今日调用次数已达上限", "今日调用次数可能已达上限，请前往妙想 Skills 页面获取更多调用次数。"),
    ("Connection refused", "当前网络可能无法访问东方财富妙想接口，请检查网络或稍后重试。"),
    ("connect:", "当前网络可能无法访问东方财富妙想接口，请检查网络或稍后重试。"),
    ("未绑定模拟组合账户", "当前账户可能尚未绑定模拟组合，请先在妙想 Skills 页面创建并绑定模拟账户。"),
    ("code=404", "当前账户可能尚未绑定模拟组合，请先在妙想 Skills 页面创建并绑定模拟账户。"),
    ("No dataTable found", "本次查询没有返回可用数据表，请放宽查询条件或到东方财富妙想 AI 页面确认查询方式。"),
    ("筛选结果为空", "本次筛选没有匹配到股票，请放宽选股条件。"),
)

_QUERY_TEMPLATES: dict[str, list[str]] = {
    "mx_query_market": [
        "上证指数今日走势和成交额",
        "半导体板块今日涨跌和主力资金流向",
        "贵州茅台近三年净利润和营业收入",
    ],
    "mx_search_news": [
        "今日A股市场热点新闻",
        "人工智能板块近期新闻",
        "美联储加息对A股影响分析",
    ],
    "mx_screen_stocks": [
        "今日涨幅大于2%的A股",
        "净利润增长率大于30%的股票",
        "新能源板块市盈率小于30的股票",
    ],
    "mx_manage_self_select": [
        "把贵州茅台加入自选股",
        "把东方财富从自选中删除",
    ],
}

_TOOL_PROFILES: dict[str, set[str]] = {
    "analysis": {
        "mx_query_market",
        "mx_search_news",
        "mx_screen_stocks",
        "mx_get_positions",
        "mx_get_balance",
        "mx_get_orders",
        "mx_get_self_selects",
        "mx_manage_self_select",
    },
    "trade": {
        "mx_query_market",
        "mx_search_news",
        "mx_screen_stocks",
        "mx_get_positions",
        "mx_get_balance",
        "mx_get_orders",
        "mx_get_self_selects",
        "mx_manage_self_select",
        "mx_moni_trade",
        "mx_moni_cancel",
    },
}


def _order_status_text(value: Any) -> str:
    return _ORDER_STATUS_MAP.get(str(value or "").strip(), str(value or "未知"))


def _empty_parameters() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    }


def _query_parameters(description: str) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": description,
            }
        },
        "required": ["query"],
        "additionalProperties": False,
    }


@dataclass(frozen=True)
class MXToolSpec:
    name: str
    description: str
    parameters: dict[str, Any]
    category: str
    mutation: bool = False

    def to_openai_tool(self) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


class MXSkillService:
    def __init__(self) -> None:
        self._tool_specs = [
            MXToolSpec(
                name="mx_query_market",
                description=(
                    "基于东方财富权威数据库及最新行情底层数据查询结构化金融数据，"
                    "适合需要权威、及时金融数据的任务，避免模型基于过时知识作答。"
                    "支持三类能力：\n"
                    "1. 行情类：股票、行业、板块、指数、基金、债券的实时行情、主力资金流向、估值等；\n"
                    "2. 财务类：上市公司与非上市公司的基本信息、财务指标、高管信息、主营业务、股东结构、融资情况等；\n"
                    "3. 关系与经营类：股票、非上市公司、股东及高管之间的关联关系，以及企业经营相关数据。\n"
                    "query 示例：'东方财富最新价'、'贵州茅台近三年净利润 营业收入'、"
                    "'宁德时代主力资金流向'、'沪深300指数最新点位 涨跌幅'、'比亚迪十大股东'。\n"
                    "注意：避免查询过大时间范围的高频或长周期日频数据，例如某只股票多年每日价格，"
                    "否则返回内容会过大。"
                ),
                parameters=_query_parameters(
                    "查询语句，例如上证指数今天走势和市场概况。"
                ),
                category="data",
            ),
            MXToolSpec(
                name="mx_search_news",
                description=(
                    "基于东方财富妙想搜索能力和金融场景信源智能筛选，查询时效性金融资讯。"
                    "适用于新闻、公告、研报、政策、交易规则、具体事件、影响分析以及需要检索外部数据的非常识信息，"
                    "可避免引用非权威或过时信息。"
                    "query 示例：'贵州茅台最新研报'、'人工智能板块近期新闻'、"
                    "'美联储加息对A股影响分析'、'科创板交易涨跌幅限制'、'今日大盘异动原因分析'。"
                ),
                parameters=_query_parameters("资讯查询语句。"),
                category="search",
            ),
            MXToolSpec(
                name="mx_screen_stocks",
                description=(
                    "基于东方财富官方选股接口，按自然语言解析选股条件并筛选股票。"
                    "支持行情指标、财务指标、行业/板块范围、指数成分股范围，以及股票/上市公司/板块推荐等任务，"
                    "避免大模型在选股时使用过时信息。"
                    "query 示例：'今日涨幅大于2%的A股'、'净利润增长率大于30%的股票'、"
                    "'新能源板块市盈率小于30的股票'、'沪深300成分股中分红率最高的10只股票'。"
                ),
                parameters=_query_parameters("选股查询语句。"),
                category="xuangu",
            ),
            MXToolSpec(
                name="mx_get_positions",
                description=(
                    "查询当前A股模拟组合持仓。返回持仓股票代码、名称、数量、可用数量、成本价、现价、市值、盈亏等信息。"
                    "适合盘点当前持仓结构、单票盈亏、可卖数量和仓位分布。"
                    "仅适用于已绑定的模拟组合账户，不涉及真实资金交易。"
                ),
                parameters=_empty_parameters(),
                category="moni",
            ),
            MXToolSpec(
                name="mx_get_balance",
                description=(
                    "查询当前A股模拟组合资金。返回可用资金、总资产、持仓市值等核心资金信息。"
                    "适合判断可开仓资金、组合仓位和账户总规模。"
                    "仅适用于已绑定的模拟组合账户，不涉及真实资金交易。"
                ),
                parameters=_empty_parameters(),
                category="moni",
            ),
            MXToolSpec(
                name="mx_get_orders",
                description=(
                    "查询当前A股模拟组合委托记录。返回委托方向、委托状态、委托价格、委托数量、成交数量、成交价格等。"
                    "适合确认未成交/已成交/已撤单委托、查看历史委托，或在撤单前获取委托编号。"
                    "仅适用于已绑定的模拟组合账户，不涉及真实资金交易。"
                ),
                parameters=_empty_parameters(),
                category="moni",
            ),
            MXToolSpec(
                name="mx_get_self_selects",
                description=(
                    "查询当前账户的自选股列表。"
                    "适合获取长期关注或待跟踪标的清单，并结合行情、资讯、选股工具继续分析。"
                ),
                parameters=_empty_parameters(),
                category="zixuan",
            ),
            MXToolSpec(
                name="mx_manage_self_select",
                description=(
                    "通过自然语言添加或删除自选股。"
                    "适合把待跟踪标的加入自选，或从自选中移除。"
                    "query 示例：'把贵州茅台加入自选股'、'把东方财富从自选中删除'。"
                ),
                parameters=_query_parameters(
                    "自然语言管理指令，例如把贵州茅台添加到我的自选股列表。"
                ),
                category="zixuan",
                mutation=True,
            ),
            MXToolSpec(
                name="mx_moni_trade",
                description=(
                    "执行A股模拟交易买入或卖出，仅用于模拟组合练习和策略验证，不涉及真实资金。"
                    "支持市价和限价委托；股票代码必须是6位A股代码，数量必须是100的整数倍。"
                    "适合模拟建仓、减仓、调仓。"
                ),
                parameters={
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["BUY", "SELL"],
                            "description": "交易方向。",
                        },
                        "symbol": {
                            "type": "string",
                            "description": "6位A股股票代码，例如 600519、300059。",
                        },
                        "name": {
                            "type": "string",
                            "description": "股票名称，可选；已知时一并传入，便于运行记录展示。",
                        },
                        "quantity": {
                            "type": "integer",
                            "description": "委托数量，必须为100的整数倍，例如100、200、300。",
                        },
                        "price_type": {
                            "type": "string",
                            "enum": ["MARKET", "LIMIT"],
                            "description": "委托方式：MARKET 为市价，LIMIT 为限价。",
                        },
                        "price": {
                            "type": ["number", "null"],
                            "description": "限价委托价格；市价时可为空。沪市价格通常不超过2位小数，深市通常不超过3位小数。",
                        },
                        "reason": {
                            "type": "string",
                            "description": "执行这笔交易的原因，会保存在运行记录中。",
                        },
                    },
                    "required": ["action", "symbol", "quantity", "price_type"],
                    "additionalProperties": False,
                },
                category="moni",
                mutation=True,
            ),
            MXToolSpec(
                name="mx_moni_cancel",
                description=(
                    "撤销A股模拟交易委托，仅用于模拟组合，不涉及真实资金。"
                    "支持一键撤销当日所有可撤委托，或按委托编号撤单。"
                    "按委托编号撤单前，通常应先调用 mx_get_orders 确认 order_id 和当前状态。"
                ),
                parameters={
                    "type": "object",
                    "properties": {
                        "cancel_type": {
                            "type": "string",
                            "enum": ["all", "order"],
                            "description": "all 表示一键撤销当日所有可撤委托，order 表示按委托编号撤单。",
                        },
                        "order_id": {
                            "type": ["string", "null"],
                            "description": "按委托编号撤单时必填，应来自 mx_get_orders 返回的委托编号。",
                        },
                        "stock_code": {
                            "type": ["string", "null"],
                            "description": "可选，按委托编号撤单时可补充股票代码。",
                        },
                        "reason": {
                            "type": "string",
                            "description": "撤单原因，用于记录。",
                        },
                    },
                    "required": ["cancel_type"],
                    "additionalProperties": False,
                },
                category="moni",
                mutation=True,
            ),
        ]
        self._handlers: dict[str, Callable[..., dict[str, Any]]] = {
            "mx_query_market": self._handle_query_market,
            "mx_search_news": self._handle_search_news,
            "mx_screen_stocks": self._handle_screen_stocks,
            "mx_get_positions": self._handle_get_positions,
            "mx_get_balance": self._handle_get_balance,
            "mx_get_orders": self._handle_get_orders,
            "mx_get_self_selects": self._handle_get_self_selects,
            "mx_manage_self_select": self._handle_manage_self_select,
            "mx_moni_trade": self._handle_moni_trade,
            "mx_moni_cancel": self._handle_moni_cancel,
        }

    def build_tools(self, run_type: str | None = None) -> list[dict[str, Any]]:
        allowed = _TOOL_PROFILES.get(str(run_type or "analysis").strip(), None)
        tool_specs = self._tool_specs
        if allowed is not None:
            tool_specs = [spec for spec in self._tool_specs if spec.name in allowed]
        return [tool_spec.to_openai_tool() for tool_spec in tool_specs]

    def execute_tool(
        self,
        *,
        client: MXClient,
        app_settings: Any,
        tool_name: str,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        handler = self._handlers.get(tool_name)
        if handler is None:
            return {
                "ok": False,
                "tool_name": tool_name,
                "error": f"未知工具调用: {tool_name}",
            }

        try:
            return handler(
                client=client, app_settings=app_settings, arguments=arguments
            )
        except Exception as exc:
            guidance = self._build_error_guidance(str(exc))
            return {
                "ok": False,
                "tool_name": tool_name,
                "error": f"{str(exc)}{guidance}",
                "normalized": {
                    "query_templates": _QUERY_TEMPLATES.get(tool_name, []),
                },
            }

    def _handle_query_market(
        self, *, client: MXClient, app_settings: Any, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        query = self._resolve_query(arguments, app_settings)
        result = client.query_market(query)
        tables = self._extract_market_tables(result)
        return {
            "ok": True,
            "tool_name": "mx_query_market",
            "summary": f"已查询市场数据：{query}，返回 {len(tables)} 张数据表。",
            "normalized": {
                "query": query,
                "query_templates": _QUERY_TEMPLATES["mx_query_market"],
                "tables": tables,
            },
            "result": result,
        }

    def _handle_search_news(
        self, *, client: MXClient, app_settings: Any, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        query = self._resolve_query(arguments, app_settings)
        result = client.search_news(query)
        items = self._extract_news_items(result)
        return {
            "ok": True,
            "tool_name": "mx_search_news",
            "summary": f"已查询资讯：{query}，返回 {len(items)} 条摘要结果。",
            "normalized": {
                "query": query,
                "query_templates": _QUERY_TEMPLATES["mx_search_news"],
                "items": items,
            },
            "result": result,
        }

    def _handle_screen_stocks(
        self, *, client: MXClient, app_settings: Any, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        query = self._resolve_query(arguments, app_settings)
        result = client.screen_stocks(query)
        candidates = self._extract_screen_candidates(result, limit=20)
        total = self._extract_screen_total(result)
        return {
            "ok": True,
            "tool_name": "mx_screen_stocks",
            "summary": f"已执行选股：{query}，候选股总数 {total if total is not None else '未知'}。",
            "normalized": {
                "query": query,
                "query_templates": _QUERY_TEMPLATES["mx_screen_stocks"],
                "total": total,
                "candidates": candidates,
            },
            "result": result,
        }

    def _handle_get_positions(
        self, *, client: MXClient, app_settings: Any, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        del app_settings, arguments
        result = client.get_positions()
        rows = self._extract_common_rows(result)
        positions = [
            {
                "symbol": row.get("stockCode") or row.get("SECURITY_CODE"),
                "name": row.get("stockName") or row.get("SECURITY_SHORT_NAME"),
                "volume": row.get("count"),
                "available": row.get("availCount"),
                "cost_price": row.get("costPrice"),
                "current_price": row.get("currentPrice"),
                "market_value": row.get("marketValue"),
                "profit": row.get("income"),
                "profit_ratio": row.get("incomeRate"),
            }
            for row in rows[:20]
            if isinstance(row, dict)
        ]
        return {
            "ok": True,
            "tool_name": "mx_get_positions",
            "summary": f"已查询持仓，当前返回 {len(positions)} 条持仓记录。",
            "normalized": {"count": len(positions), "positions": positions},
            "result": result,
        }

    def _handle_get_balance(
        self, *, client: MXClient, app_settings: Any, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        del app_settings, arguments
        result = client.get_balance()
        data = result.get("data") if isinstance(result, dict) else {}
        normalized = {
            "available_balance": extract_available_balance(result),
            "total_assets": self._extract_first_number(
                data,
                "totalAsset",
                "totalAssets",
                "asset",
                "totalMoney",
            ),
            "market_value": self._extract_first_number(
                data,
                "marketValue",
                "stockMarketValue",
                "positionValue",
                "totalPosValue",
            ),
        }
        return {
            "ok": True,
            "tool_name": "mx_get_balance",
            "summary": "已查询账户资金。",
            "normalized": normalized,
            "result": result,
        }

    def _handle_get_orders(
        self, *, client: MXClient, app_settings: Any, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        del app_settings, arguments
        result = client.get_orders()
        rows = self._extract_common_rows(result)
        orders = [
            {
                "order_id": row.get("orderId"),
                "symbol": row.get("stockCode") or row.get("SECURITY_CODE"),
                "name": row.get("stockName") or row.get("SECURITY_SHORT_NAME"),
                "side": "买入" if str(row.get("orderDrt") or "") == "1" else "卖出",
                "status": _order_status_text(row.get("orderStatus")),
                "order_price": row.get("orderPrice"),
                "order_qty": row.get("orderCount"),
                "filled_qty": row.get("dealCount"),
                "filled_price": row.get("dealPrice"),
            }
            for row in rows[:20]
            if isinstance(row, dict)
        ]
        return {
            "ok": True,
            "tool_name": "mx_get_orders",
            "summary": f"已查询委托记录，当前返回 {len(orders)} 条。",
            "normalized": {"count": len(orders), "orders": orders},
            "result": result,
        }

    def _handle_get_self_selects(
        self, *, client: MXClient, app_settings: Any, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        del app_settings, arguments
        result = client.get_self_selects()
        stocks = self._extract_self_select_rows(result)
        return {
            "ok": True,
            "tool_name": "mx_get_self_selects",
            "summary": f"已查询自选股列表，当前返回 {len(stocks)} 只股票。",
            "normalized": {
                "count": len(stocks),
                "stocks": stocks[:20],
            },
            "result": result,
        }

    def _handle_manage_self_select(
        self, *, client: MXClient, app_settings: Any, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        query = self._resolve_query(arguments, app_settings)
        result = client.manage_self_select(query)
        return {
            "ok": True,
            "tool_name": "mx_manage_self_select",
            "summary": f"已执行自选股操作：{query}",
            "normalized": {
                "query": query,
                "query_templates": _QUERY_TEMPLATES["mx_manage_self_select"],
            },
            "result": result,
            "executed_action": {
                "action": "MANAGE_SELF_SELECT",
                "query": query,
            },
        }

    def _handle_moni_trade(
        self, *, client: MXClient, app_settings: Any, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        del app_settings
        action = str(arguments.get("action") or "").upper()
        symbol = str(arguments.get("symbol") or "").strip()
        price_type = str(arguments.get("price_type") or "MARKET").upper()
        quantity = int(arguments.get("quantity") or 0)
        price = arguments.get("price")
        reason = str(arguments.get("reason") or "").strip()

        if action not in {"BUY", "SELL"}:
            raise RuntimeError("模拟交易工具的 action 只能是 BUY 或 SELL。")
        if not symbol:
            raise RuntimeError("模拟交易工具缺少股票代码。")
        if quantity <= 0:
            raise RuntimeError("模拟交易工具的 quantity 必须大于 0。")
        if quantity % 100 != 0:
            raise RuntimeError("A 股交易数量必须是 100 的整数倍。")
        if price_type not in {"MARKET", "LIMIT"}:
            raise RuntimeError("price_type 只能是 MARKET 或 LIMIT。")
        if price_type == "LIMIT":
            try:
                normalized_price = float(price)
            except (TypeError, ValueError) as exc:
                raise RuntimeError("LIMIT 委托必须提供有效价格。") from exc
            if normalized_price <= 0:
                raise RuntimeError("LIMIT 委托价格必须大于 0。")
            price = normalized_price
        elif price is not None:
            try:
                price = float(price)
            except (TypeError, ValueError):
                price = None

        result = client.trade(
            action=action,
            symbol=symbol,
            quantity=quantity,
            price_type=price_type,
            price=price,
        )
        return {
            "ok": True,
            "tool_name": "mx_moni_trade",
            "summary": f"已提交{action}委托：{symbol} {quantity} 股。",
            "normalized": {
                "symbol": symbol,
                "action": action,
                "quantity": quantity,
                "price_type": price_type,
                "price": price,
            },
            "result": result,
            "executed_action": {
                "symbol": symbol,
                "name": str(arguments.get("name") or "").strip(),
                "action": action,
                "quantity": quantity,
                "price_type": price_type,
                "price": price,
                "reason": reason,
            },
        }

    def _handle_moni_cancel(
        self, *, client: MXClient, app_settings: Any, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        del app_settings
        cancel_type = str(arguments.get("cancel_type") or "").strip().lower()
        order_id = str(arguments.get("order_id") or "").strip() or None
        stock_code = str(arguments.get("stock_code") or "").strip() or None
        reason = str(arguments.get("reason") or "").strip()

        if cancel_type not in {"all", "order"}:
            raise RuntimeError("cancel_type 只能是 all 或 order。")
        if cancel_type == "order" and not order_id:
            raise RuntimeError("按委托编号撤单时必须提供 order_id。")

        result = client.cancel_order(
            cancel_type=cancel_type,
            order_id=order_id,
            stock_code=stock_code,
        )
        return {
            "ok": True,
            "tool_name": "mx_moni_cancel",
            "summary": "已提交撤单请求。"
            if cancel_type == "all"
            else f"已提交撤单请求：{order_id}",
            "normalized": {
                "cancel_type": cancel_type,
                "order_id": order_id,
                "stock_code": stock_code,
            },
            "result": result,
            "executed_action": {
                "action": "CANCEL",
                "cancel_type": cancel_type,
                "order_id": order_id,
                "stock_code": stock_code,
                "reason": reason,
            },
        }

    def _resolve_query(self, arguments: dict[str, Any], app_settings: Any) -> str:
        query = str(arguments.get("query") or "").strip()
        if query:
            return query
        fallback = str(getattr(app_settings, "task_prompt", "") or "").strip()
        if fallback:
            return fallback
        raise RuntimeError("缺少 query 参数。")

    def _extract_market_tables(
        self, payload: dict[str, Any]
    ) -> list[dict[str, Any]]:
        inner = (
            ((payload.get("data") or {}).get("data") or {})
        )
        sdr = inner.get("searchDataResultDTO") or {}
        raw_tables = sdr.get("dataTableDTOList") or []
        tables: list[dict[str, Any]] = []
        for tbl in raw_tables:
            if not isinstance(tbl, dict):
                continue
            name_map: dict[str, str] = tbl.get("nameMap") or {}
            indicator_order: list[str] = tbl.get("indicatorOrder") or []
            # Use indicatorOrder to define column sequence; fall back to nameMap keys
            col_keys = [k for k in indicator_order if k in name_map] or list(name_map.keys())
            columns = [name_map[k] for k in col_keys]

            tbl_data = tbl.get("table") or {}
            raw_rows: list[Any] = []
            if isinstance(tbl_data, dict):
                raw_rows = tbl_data.get("dataList") or tbl_data.get("rows") or []

            rows: list[dict[str, Any]] = []
            for row in raw_rows[:20]:
                if not isinstance(row, dict):
                    continue
                rows.append({name_map.get(k, k): row.get(k) for k in col_keys if k in row})

            tables.append(
                {
                    "title": tbl.get("frontendTitle") or tbl.get("title"),
                    "entity": tbl.get("frontendEntityName"),
                    "columns": columns,
                    "rows": rows,
                }
            )
        return tables

    def _extract_news_items(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        data = ((payload.get("data") or {}).get("data") or {}).get(
            "llmSearchResponse"
        ) or {}
        rows = data.get("data") or []
        items: list[dict[str, Any]] = []
        for row in rows[:10]:
            if not isinstance(row, dict):
                continue
            items.append(
                {
                    "title": row.get("title"),
                    "date": row.get("date"),
                    "type": row.get("informationType"),
                    "entity": row.get("entityFullName"),
                    "institution": row.get("insName"),
                    "rating": row.get("rating"),
                }
            )
        return items

    def _extract_screen_candidates(
        self, payload: dict[str, Any], limit: int = 20
    ) -> list[dict[str, Any]]:
        res = (
            ((payload.get("data") or {}).get("data") or {})
            .get("allResults") or {}
        ).get("result") or {}
        columns: list[dict[str, Any]] = res.get("columns") or []
        rows: list[Any] = res.get("dataList") or []

        # Build key→title map from columns definition
        key_title: dict[str, str] = {
            col["key"]: col.get("title", col["key"])
            for col in columns
            if isinstance(col, dict) and col.get("key")
        }

        # Core fields to always include when present
        _CORE = {
            "SECURITY_CODE", "SECURITY_SHORT_NAME", "NEWEST_PRICE", "CHG",
        }
        # Additional enrichment fields (dynamic keys with date suffixes)
        _ENRICHMENT_PREFIXES = (
            "010000_CIRCULATION_MARKET_VALUE",
            "010000_PB",
            "010000_TURNOVER_RATE",
            "010000_LIANGBI",
            "010000_RPT_F10_ORG_BASICINFO_BOARD_NAME_TOTAL",
        )

        selected_keys = [
            k for k in key_title
            if k in _CORE
            or any(k.startswith(p) for p in _ENRICHMENT_PREFIXES)
        ]

        candidates: list[dict[str, Any]] = []
        for row in rows[:limit]:
            if not isinstance(row, dict):
                continue
            entry: dict[str, Any] = {}
            for k in selected_keys:
                if k in row:
                    entry[key_title.get(k, k)] = row[k]
            if entry:
                candidates.append(entry)
        return candidates

    def _extract_screen_total(self, payload: dict[str, Any]) -> int | None:
        result = (
            ((payload.get("data") or {}).get("data") or {}).get("allResults") or {}
        ).get("result") or {}
        total = result.get("total") or result.get("totalRecordCount")
        try:
            return int(total) if total is not None else None
        except (TypeError, ValueError):
            return None

    def _extract_common_rows(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        data = payload.get("data")
        if isinstance(data, dict):
            rows = data.get("data") or data.get("rows") or data.get("list") or []
            return [row for row in rows if isinstance(row, dict)]
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        return []

    def _extract_self_select_rows(
        self, payload: dict[str, Any]
    ) -> list[dict[str, Any]]:
        result = ((payload.get("data") or {}).get("allResults") or {}).get(
            "result"
        ) or {}
        rows = result.get("dataList") or []
        stocks: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            stocks.append(
                {
                    "symbol": row.get("SECURITY_CODE"),
                    "name": row.get("SECURITY_SHORT_NAME"),
                    "latest_price": row.get("NEWEST_PRICE"),
                    "change_percent": row.get("CHG"),
                    "turnover_rate": row.get("010000_TURNOVER_RATE"),
                    "volume_ratio": row.get("010000_LIANGBI"),
                }
            )
        return stocks

    def _extract_first_number(self, payload: Any, *keys: str) -> float | None:
        if not isinstance(payload, dict):
            return None
        for key in keys:
            value = payload.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    def _build_error_guidance(self, message: str) -> str:
        text = str(message or "").strip()
        if not text:
            return ""
        for needle, hint in _ERROR_HINTS:
            if needle in text:
                return f"；建议：{hint}"
        return ""


mx_skill_service = MXSkillService()
