"""Tests for the hardcoded 2026 trading calendar."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from backend.infra.calendar import (
    is_market_session_open,
    is_trading_day,
    last_trading_day_on_or_before,
)


def test_is_trading_day_returns_true_for_known_2026_trading_day() -> None:
    assert is_trading_day(date(2026, 4, 27)) is True


def test_is_trading_day_returns_false_for_known_2026_holiday() -> None:
    assert is_trading_day(date(2026, 2, 17)) is False


def test_is_trading_day_returns_false_outside_loaded_years() -> None:
    assert is_trading_day(date(2028, 1, 4)) is False


def test_is_market_session_open_checks_day_and_two_a_share_sessions() -> None:
    # 2026-04-27 is a loaded trading day; all moments below are UTC.
    assert is_market_session_open(datetime(2026, 4, 27, 1, 30, tzinfo=UTC)) is True
    assert is_market_session_open(datetime(2026, 4, 27, 3, 30, tzinfo=UTC)) is True
    assert is_market_session_open(datetime(2026, 4, 27, 4, 0, tzinfo=UTC)) is False
    assert is_market_session_open(datetime(2026, 4, 27, 5, 0, tzinfo=UTC)) is True
    assert is_market_session_open(datetime(2026, 4, 27, 7, 0, tzinfo=UTC)) is True
    assert is_market_session_open(datetime(2026, 4, 27, 7, 1, tzinfo=UTC)) is False
    assert is_market_session_open(datetime(2026, 4, 26, 2, 0, tzinfo=UTC)) is False


def test_last_trading_day_on_or_before_steps_back_to_friday() -> None:
    assert last_trading_day_on_or_before(date(2026, 4, 26)) == date(2026, 4, 24)


def test_last_trading_day_fail_closed_outside_loaded_years() -> None:
    with pytest.raises(ValueError, match="loaded years"):
        last_trading_day_on_or_before(date(2028, 1, 4))


def test_is_trading_day_supports_2027_loaded_calendar() -> None:
    assert is_trading_day(date(2027, 1, 4)) is True


def test_last_trading_day_supports_2027() -> None:
    assert last_trading_day_on_or_before(date(2027, 1, 4)) == date(2027, 1, 4)
