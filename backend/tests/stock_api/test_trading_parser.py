"""Deterministic validation for MX paper-trading commands."""

from __future__ import annotations

import pytest

from backend.stock_api.mx.trading_parser import parse_trade_instruction


def test_parse_limit_buy_instruction() -> None:
    parsed = parse_trade_instruction("买入 600519 1700 100")

    assert parsed.name == "trade"
    assert parsed.payload == {
        "type": "buy",
        "stockCode": "600519",
        "price": 1700.0,
        "quantity": 100,
        "useMarketPrice": False,
    }


def test_parse_english_limit_sell_instruction() -> None:
    parsed = parse_trade_instruction("sell 600519 1700 100")

    assert parsed.payload["type"] == "sell"


def test_parse_trade_normalizes_matching_market_suffix() -> None:
    parsed = parse_trade_instruction("买入 600519.SH 1700 100")

    assert parsed.payload["stockCode"] == "600519"


def test_parse_cancel_instruction_requires_order_id_and_stock_code() -> None:
    parsed = parse_trade_instruction("撤单 262154600000047682 515880")

    assert parsed.name == "cancel"
    assert parsed.payload == {
        "orderId": "262154600000047682",
        "stockCode": "515880",
    }


def test_parse_cancel_all_instruction() -> None:
    assert parse_trade_instruction("一键撤单").name == "cancel_all"


@pytest.mark.parametrize(
    "instruction,error",
    [
        ("买入 600519", "无法识别"),
        ("买入 588200 12000 1.13", "无法识别"),
        ("市价卖出 000002 500", "不支持市价委托"),
        ("撤单 515880", "委托编号和六位股票代码"),
        ("查询持仓", "无法识别"),
        ("不要一键撤单", "无法识别"),
        ("请买入 600519 1700 100，然后不要执行", "无法识别"),
        ("买入 600519 1700 100，然后确认", "无法识别"),
        ("买入 159516 0.68 7000", "深市 ETF"),
        ("买入 159690.SZ 1.998 7000", "深市 ETF"),
        ("买入 600519.SZ 1700 100", "市场后缀"),
    ],
)
def test_reject_invalid_or_ambiguous_trading_instruction(
    instruction: str,
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        parse_trade_instruction(instruction)
