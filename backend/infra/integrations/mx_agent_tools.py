"""Direct Agent tools for MX research, portfolio, and paper trading."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, field

from backend.agent.tools.registry import ToolRegistry
from backend.infra.integrations.tool_policy import SideEffectLevel
from backend.llm import ToolDefinition
from backend.stock_api import (
    MxMoniClient,
    MxPaperTradingClient,
    MxResearchClient,
    json_safe,
)
from backend.stock_api.mx.trading_parser import (
    ParsedTradeIntent,
    parse_trade_instruction,
)

READ_STAGES = ("Run",)
TRADE_STAGES = ("Run",)
_DEFAULT_PORTFOLIO_ORDER_RESULT_LIMIT = 50
_PORTFOLIO_INTENTS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("positions", ("持仓", "positions", "position")),
    (
        "orders",
        (
            "委托",
            "订单",
            "orders",
            "order",
            "成交",
            "deal",
            "trade",
            "fill",
            "在途",
            "撤单",
            "废单",
        ),
    ),
    ("balance", ("资金", "余额", "资产", "balance", "cash", "账户", "bal")),
)


def _validated_order_result_options(limit: object, full: object) -> tuple[int, bool]:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("limit 必须是大于 0 的整数")
    if not isinstance(full, bool):
        raise ValueError("full 必须是布尔值")
    return limit, full


@dataclass(slots=True)
class QueryMarketDataTool:
    client: MxResearchClient
    name: str = "query_market_data"
    enabled_stages: tuple[str, ...] = field(default=READ_STAGES)
    side_effect_level: SideEffectLevel = SideEffectLevel.READ
    execution_mode: str = "sequential"

    def to_tool_definition(self) -> ToolDefinition:
        return {
            "name": self.name,
            "description": (
                "用自然语言查询妙想提供的股票、指数、市场概况、财务和股权数据。"
                "不用于日、周、月或分钟 K 线；所有 K 线查询必须使用 query_kline。"
            ),
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string", "minLength": 1}},
                "required": ["query"],
                "additionalProperties": False,
            },
        }

    async def run(self, query: str) -> object:
        return await self.client.query_market_data(query)


@dataclass(slots=True)
class SearchNewsTool:
    client: MxResearchClient
    name: str = "search_news"
    enabled_stages: tuple[str, ...] = field(default=READ_STAGES)
    side_effect_level: SideEffectLevel = SideEffectLevel.READ
    execution_mode: str = "sequential"

    def to_tool_definition(self) -> ToolDefinition:
        return {
            "name": self.name,
            "description": "检索最新财经新闻、公告、研报和政策信息。",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string", "minLength": 1}},
                "required": ["query"],
                "additionalProperties": False,
            },
        }

    async def run(self, query: str) -> object:
        return await self.client.search_news(query)


@dataclass(slots=True)
class SelectStocksTool:
    client: MxResearchClient
    name: str = "select_stocks"
    enabled_stages: tuple[str, ...] = field(default=READ_STAGES)
    side_effect_level: SideEffectLevel = SideEffectLevel.READ
    execution_mode: str = "parallel"

    def to_tool_definition(self) -> ToolDefinition:
        return {
            "name": self.name,
            "description": "按自然语言条件筛选 A 股或板块成分股。",
            "parameters": {
                "type": "object",
                "properties": {"keyword": {"type": "string", "minLength": 1}},
                "required": ["keyword"],
                "additionalProperties": False,
            },
        }

    async def run(self, keyword: str) -> object:
        return await self.client.select_stocks(keyword)


@dataclass(slots=True)
class QueryPortfolioTool:
    client: MxMoniClient
    name: str = "query_portfolio"
    enabled_stages: tuple[str, ...] = field(default=READ_STAGES)
    side_effect_level: SideEffectLevel = SideEffectLevel.READ
    execution_mode: str = "parallel"

    def to_tool_definition(self) -> ToolDefinition:
        return {
            "name": self.name,
            "description": (
                "查询已绑定模拟组合的资金、持仓、委托、订单或成交。"
                "委托、订单与成交默认返回最后 50 条；limit 可指定最后 X 条；"
                "full=true 返回全量。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "instruction": {"type": "string", "minLength": 1},
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "default": _DEFAULT_PORTFOLIO_ORDER_RESULT_LIMIT,
                        "description": (
                            "委托、订单与成交返回最后 X 条；full=true 时忽略。"
                        ),
                    },
                    "full": {
                        "type": "boolean",
                        "default": False,
                        "description": "true 时委托、订单与成交返回全量。",
                    },
                },
                "required": ["instruction"],
                "additionalProperties": False,
            },
        }

    async def run(
        self,
        instruction: str,
        limit: int = _DEFAULT_PORTFOLIO_ORDER_RESULT_LIMIT,
        full: bool = False,
    ) -> object:
        order_limit, include_all_orders = _validated_order_result_options(limit, full)
        intents = _parse_portfolio_intents(instruction)

        async def load(intent: str) -> object:
            if intent == "balance":
                return json_safe(asdict(await self.client.get_account_snapshot()))
            if intent == "positions":
                return [
                    json_safe(asdict(item))
                    for item in await self.client.get_positions()
                ]
            if intent == "orders":
                return await self.client.get_orders()
            raise ValueError(f"unsupported portfolio intent: {intent}")

        values = await asyncio.gather(*(load(intent) for intent in intents))
        results = dict(zip(intents, values, strict=True))
        orders = results.get("orders")
        order_truncation: dict[str, int | bool] | None = None
        if isinstance(orders, list):
            total_orders = len(orders)
            selected_orders = orders if include_all_orders else orders[-order_limit:]
            results["orders"] = [json_safe(asdict(item)) for item in selected_orders]
            if len(selected_orders) < total_orders:
                order_truncation = {
                    "returned_count": len(selected_orders),
                    "total_count": total_orders,
                    "truncated": True,
                }
        payload: dict[str, object] = {
            "status": "ok",
            "query": instruction.strip(),
            "intents": list(intents),
        }
        if len(intents) == 1:
            payload["result"] = results[intents[0]]
        else:
            payload["results"] = results
        if order_truncation is not None:
            payload["truncation"] = {"orders": order_truncation}
        return payload


@dataclass(slots=True)
class TradeTool:
    client: MxPaperTradingClient
    portfolio: MxMoniClient | None = None
    name: str = "trade"
    enabled_stages: tuple[str, ...] = field(default=TRADE_STAGES)
    side_effect_level: SideEffectLevel = SideEffectLevel.WRITE
    execution_mode: str = "sequential"
    requires_market_open: bool = True

    def is_write_call(self, arguments: object) -> bool:
        del arguments
        return True

    def to_tool_definition(self) -> ToolDefinition:
        return {
            "name": self.name,
            "description": (
                "在已绑定的 MX A 股模拟组合中提交限价买卖委托。"
                "只接受格式“买入/卖出 <六位代码> <价格> <数量>”，"
                "例如“买入 600519 1700 100”。"
                "数量必须为 100 的整数倍；不支持市价委托。"
                "执行前系统会复核卖出持仓数量和买入可用资金。"
            ),
            "parameters": {
                "type": "object",
                "properties": {"instruction": {"type": "string", "minLength": 1}},
                "required": ["instruction"],
                "additionalProperties": False,
            },
        }

    async def run(self, instruction: str) -> object:
        intent = parse_trade_instruction(instruction)
        if intent.name != "trade":
            raise ValueError("trade only accepts a limit buy or sell instruction")
        await _preflight_trade(self.portfolio, intent)
        return await self.client.trade(instruction)


@dataclass(slots=True)
class CancelTool:
    client: MxPaperTradingClient
    portfolio: MxMoniClient | None = None
    name: str = "cancel"
    enabled_stages: tuple[str, ...] = field(default=TRADE_STAGES)
    side_effect_level: SideEffectLevel = SideEffectLevel.WRITE
    execution_mode: str = "sequential"
    requires_market_open: bool = True

    def is_write_call(self, arguments: object) -> bool:
        del arguments
        return True

    def to_tool_definition(self) -> ToolDefinition:
        return {
            "name": self.name,
            "description": (
                "撤销模拟组合委托。指定撤单必须使用“撤单 <委托编号> <六位代码>”，"
                "例如“撤单 262154600000047682 515880”；"
                "需要撤销全部未成交委托时使用“一键撤单”。"
                "系统会先复核委托编号、股票代码和可撤状态。"
            ),
            "parameters": {
                "type": "object",
                "properties": {"instruction": {"type": "string", "minLength": 1}},
                "required": ["instruction"],
                "additionalProperties": False,
            },
        }

    async def run(self, instruction: str) -> object:
        intent = parse_trade_instruction(instruction)
        if intent.name not in {"cancel", "cancel_all"}:
            raise ValueError("cancel only accepts a cancel instruction")
        await _preflight_cancel(self.portfolio, intent)
        return await self.client.cancel(instruction)


def register_mx_tools(
    registry: ToolRegistry,
    *,
    research: MxResearchClient,
    portfolio: MxMoniClient,
    trading: MxPaperTradingClient,
) -> None:
    """Expose every supported MX capability as a direct Agent tool."""

    registry.register(QueryMarketDataTool(research))
    registry.register(SearchNewsTool(research))
    registry.register(SelectStocksTool(research))
    registry.register(QueryPortfolioTool(portfolio))
    registry.register(TradeTool(trading, portfolio))
    registry.register(CancelTool(trading, portfolio))


async def _preflight_trade(
    portfolio: MxMoniClient | None,
    intent: ParsedTradeIntent,
) -> None:
    if portfolio is None:
        return
    payload = intent.payload
    stock_code = str(payload["stockCode"])
    quantity_value = payload["quantity"]
    assert isinstance(quantity_value, int)
    quantity = quantity_value
    direction = str(payload["type"])
    if direction == "buy":
        price_value = payload["price"]
        assert isinstance(price_value, (int, float))
        price = float(price_value)
        snapshot = await portfolio.get_account_snapshot()
        required_cash = price * quantity
        if required_cash > snapshot.available_cash:
            raise ValueError(
                f"买入所需资金 {required_cash:.2f} 元超过当前可用资金 "
                f"{snapshot.available_cash:.2f} 元"
            )
        return

    positions = await portfolio.get_positions()
    position = next((item for item in positions if item.symbol == stock_code), None)
    if position is None:
        raise ValueError(f"卖出数量 {quantity} 超过 {stock_code} 可卖数量 0")
    available = position.available_quantity
    if available is None:
        orders = await portfolio.get_orders()
        occupied = 0
        unknown_remaining = False
        pending_statuses = {
            "PENDING",
            "PARTIAL",
            "PARTIAL_PENDING_CANCEL",
            "PENDING_CANCEL",
        }
        for order in orders:
            if order.symbol != stock_code or order.direction != "SELL":
                continue
            remaining = max(order.quantity - order.filled_quantity, 0)
            if order.status in pending_statuses:
                occupied += remaining
            elif order.status == "UNKNOWN" and remaining > 0:
                unknown_remaining = True
        if unknown_remaining:
            raise ValueError(
                f"无法确认 {stock_code} 可卖数量；请求卖出 {quantity} 股，"
                "请先确认未完成卖出委托"
            )
        available = max(position.quantity - occupied, 0)
    if quantity > available:
        raise ValueError(f"卖出数量 {quantity} 超过 {stock_code} 可卖数量 {available}")


async def _preflight_cancel(
    portfolio: MxMoniClient | None,
    intent: ParsedTradeIntent,
) -> None:
    if portfolio is None or intent.name == "cancel_all":
        return
    order_id = str(intent.payload["orderId"])
    stock_code = str(intent.payload["stockCode"])
    orders = await portfolio.get_orders()
    order = next((item for item in orders if item.order_id == order_id), None)
    if order is None:
        raise ValueError(f"未找到委托编号 {order_id}；请先查询委托或使用一键撤单")
    if order.symbol != stock_code:
        raise ValueError(
            f"委托编号 {order_id} 对应股票 {order.symbol}，与 {stock_code} 不一致"
        )
    if order.status not in {"PENDING", "PARTIAL"}:
        raise ValueError(f"委托编号 {order_id} 当前状态为 {order.status}，不可撤销")


def _parse_portfolio_intents(instruction: str) -> tuple[str, ...]:
    text = instruction.strip()
    if not text:
        raise ValueError("组合查询内容不能为空")
    lower = text.lower()
    intents = tuple(
        intent
        for intent, keywords in _PORTFOLIO_INTENTS
        if any(keyword in text or keyword in lower for keyword in keywords)
    )
    if not intents:
        raise ValueError("无法识别组合查询；支持资金、持仓、委托、订单和成交")
    return intents


__all__ = [
    "CancelTool",
    "QueryMarketDataTool",
    "QueryPortfolioTool",
    "SearchNewsTool",
    "SelectStocksTool",
    "TradeTool",
    "register_mx_tools",
]
