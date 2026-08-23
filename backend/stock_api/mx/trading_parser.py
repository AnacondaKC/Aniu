"""Strict, deterministic parsing for MX paper-trading commands.

Write tools deliberately accept only unambiguous, limit-order instructions.  A
malformed trade must fail before it reaches the upstream simulator.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Literal

TradeIntentName = Literal["trade", "cancel", "cancel_all"]


@dataclass(frozen=True, slots=True)
class ParsedTradeIntent:
    name: TradeIntentName
    payload: dict[str, object]


_CANCEL_ALL = re.compile(
    r"^(?:一键撤单|撤销所有|撤销全部|cancel\s+all)$",
    flags=re.IGNORECASE,
)
_CANCEL_PREFIX = re.compile(r"^(?:撤单|撤销|cancel)", flags=re.IGNORECASE)
_CANCEL_ORDER = re.compile(
    r"^(?:撤单|撤销|cancel)\s+(?P<first>[A-Za-z0-9_-]+)\s+(?P<second>\d{6})(?:\.(?:SH|SZ))?\s*$",
    flags=re.IGNORECASE,
)
_CANCEL_ORDER_REVERSED = re.compile(
    r"^(?:撤单|撤销|cancel)\s+(?P<stock_code>\d{6})(?:\.(?:SH|SZ))?\s+(?P<order_id>[A-Za-z0-9_-]+)\s*$",
    flags=re.IGNORECASE,
)
_LIMIT_TRADE = re.compile(
    r"(?P<direction>买入|买|buy|卖出|卖|sell)\s*"
    r"(?P<stock_code>\d{6}(?:\.(?:SH|SZ))?)\s+"
    r"(?P<price>[0-9]+(?:\.[0-9]+)?)\s+(?P<quantity>\d+)",
    flags=re.IGNORECASE,
)
_TRADE_CODE = re.compile(
    r"^(?P<code>\d{6})(?:\.(?P<market>SH|SZ))?$",
    flags=re.IGNORECASE,
)


def parse_trade_instruction(instruction: str) -> ParsedTradeIntent:
    """Return one validated MX write intent without guessing field meaning."""

    text = instruction.strip()
    if not text:
        raise ValueError("交易指令不能为空")

    lower = text.casefold()
    if _CANCEL_ALL.fullmatch(text):
        return ParsedTradeIntent("cancel_all", {})

    if _CANCEL_PREFIX.match(text):
        return _parse_cancel_instruction(text)

    if "市价" in text or "market price" in lower:
        raise ValueError(
            "不支持市价委托；请使用限价格式：买入/卖出 <六位代码> <价格> <数量>"
        )

    match = _LIMIT_TRADE.fullmatch(text)
    if match is None:
        raise ValueError(
            "无法识别限价交易指令。格式：买入/卖出 <六位代码> <价格> <数量>"
        )

    direction_token = match.group("direction").casefold()
    direction = "buy" if direction_token in {"买入", "买", "buy"} else "sell"
    stock_code = _normalize_trade_stock_code(match.group("stock_code"))
    price = float(match.group("price"))
    quantity = int(match.group("quantity"))
    if not math.isfinite(price) or price <= 0:
        raise ValueError("委托价格必须大于 0")
    if quantity <= 0 or quantity % 100 != 0:
        raise ValueError(
            "交易数量必须为 100 的整数倍；格式：买入/卖出 <六位代码> <价格> <数量>"
        )
    return ParsedTradeIntent(
        "trade",
        {
            "type": direction,
            "stockCode": stock_code,
            "price": price,
            "quantity": quantity,
            "useMarketPrice": False,
        },
    )


def _normalize_trade_stock_code(value: str) -> str:
    match = _TRADE_CODE.fullmatch(value.strip())
    if match is None:
        raise ValueError("股票代码必须是六位代码，可选 .SH 或 .SZ 市场后缀")
    code = match.group("code")
    suffix = match.group("market")
    inferred_market = (
        "SH"
        if code.startswith(("5", "6", "9"))
        else "SZ"
        if code.startswith(("0", "1", "2", "3"))
        else None
    )
    if suffix is not None and inferred_market is not None:
        if suffix.upper() != inferred_market:
            raise ValueError(f"股票代码 {code} 的市场后缀应为 .{inferred_market}")
    if code.startswith("159"):
        raise ValueError(
            f"MX 模拟盘暂不支持深市 ETF 代码 {code}；请使用 MX 可识别的沪市标的"
        )
    return code


def _parse_cancel_instruction(text: str) -> ParsedTradeIntent:
    reversed_match = _CANCEL_ORDER_REVERSED.fullmatch(text)
    if reversed_match is not None:
        return ParsedTradeIntent(
            "cancel",
            {
                "orderId": reversed_match.group("order_id"),
                "stockCode": reversed_match.group("stock_code"),
            },
        )

    match = _CANCEL_ORDER.fullmatch(text)
    if match is None:
        raise ValueError(
            "指定撤单必须提供委托编号和六位股票代码；格式：撤单 <委托编号> <六位代码>。"
            "如需撤销全部未成交委托，请使用“一键撤单”。"
        )

    first = match.group("first")
    stock_code = match.group("second")
    if re.fullmatch(r"\d{6}", first):
        raise ValueError(
            "指定撤单必须提供委托编号和六位股票代码；格式：撤单 <委托编号> <六位代码>。"
            "如需撤销全部未成交委托，请使用“一键撤单”。"
        )
    return ParsedTradeIntent(
        "cancel",
        {"orderId": first, "stockCode": stock_code},
    )


__all__ = ["ParsedTradeIntent", "parse_trade_instruction"]
