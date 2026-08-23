"""Tests for trading domain validators."""

from __future__ import annotations

import pytest

from backend.business.shared.trading import validate_quantity, validate_symbol


def test_validate_symbol_accepts_six_digit_code() -> None:
    assert validate_symbol("600519") == "600519"


@pytest.mark.parametrize("symbol", ["60051", "600519X", "ABC123"])
def test_validate_symbol_rejects_invalid_symbol(symbol: str) -> None:
    with pytest.raises(ValueError, match="6-digit"):
        validate_symbol(symbol)


def test_validate_quantity_rejects_non_board_lot() -> None:
    with pytest.raises(ValueError, match="multiple of 100"):
        validate_quantity(250)


def test_validate_quantity_accepts_board_lot() -> None:
    assert validate_quantity(200) == 200
