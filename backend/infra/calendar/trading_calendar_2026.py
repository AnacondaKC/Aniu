"""A-share trading calendar (data-backed, fail-closed for unknown years)."""

from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from backend.infra.calendar.loader import load_trading_days_by_year

MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")
MORNING_OPEN = time(9, 30)
MORNING_CLOSE = time(11, 30)
AFTERNOON_OPEN = time(13, 0)
AFTERNOON_CLOSE = time(15, 0)

TRADING_DAYS_BY_YEAR: dict[int, frozenset[str]] = load_trading_days_by_year()
SUPPORTED_CALENDAR_YEARS = frozenset(TRADING_DAYS_BY_YEAR)


def is_trading_day(day: date) -> bool:
    """Return whether the given date is a loaded trading day.

    Years outside the loaded calendar fail closed (non-trading).
    """

    days = TRADING_DAYS_BY_YEAR.get(day.year)
    if days is None:
        return False
    return day.isoformat() in days


def last_trading_day_on_or_before(day: date) -> date:
    """Return the latest trading day on or before the given day.

    Unknown years raise so callers cannot silently use weekend/holiday logic.
    """

    if day.year not in SUPPORTED_CALENDAR_YEARS:
        supported = ", ".join(str(y) for y in sorted(SUPPORTED_CALENDAR_YEARS))
        raise ValueError(
            "trading calendar has no data for year "
            f"{day.year}; loaded years: {supported}"
        )

    probe = day
    min_year = min(SUPPORTED_CALENDAR_YEARS)
    while probe.year >= min_year:
        if is_trading_day(probe):
            return probe
        probe = date.fromordinal(probe.toordinal() - 1)

    raise ValueError(
        "no trading day found on or before the given date in loaded calendar"
    )


def is_market_session_open(moment: datetime) -> bool:
    """Return whether the supplied time falls within trading sessions."""

    market_time = moment.astimezone(MARKET_TIMEZONE)
    if not is_trading_day(market_time.date()):
        return False
    current = market_time.time()
    return (
        MORNING_OPEN <= current <= MORNING_CLOSE
        or AFTERNOON_OPEN <= current <= AFTERNOON_CLOSE
    )


class TradingCalendar2026:
    """Small adapter exposing the multi-year trading calendar."""

    def last_trading_day_on_or_before(self, day: date) -> date:
        return last_trading_day_on_or_before(day)
