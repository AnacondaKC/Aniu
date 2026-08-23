"""Domain tests for custom schedule time normalization limits."""

from __future__ import annotations

import pytest

from backend.business.schedules.models import (
    MAX_CUSTOM_SCHEDULE_TIMES,
    StrategySchedule,
    derive_intraday_schedule_times,
    normalize_custom_schedule_times,
)


def test_normalize_custom_schedule_times_dedupes_keeps_order() -> None:
    assert normalize_custom_schedule_times(["14:00", "09:30", "09:30"]) == (
        "14:00",
        "09:30",
    )


def test_normalize_custom_schedule_times_empty_becomes_none() -> None:
    assert normalize_custom_schedule_times([]) is None
    assert normalize_custom_schedule_times(None) is None


def test_normalize_custom_schedule_times_strips_surrounding_whitespace() -> None:
    assert normalize_custom_schedule_times([" 09:30 "]) == ("09:30",)


def test_normalize_custom_schedule_times_rejects_bad_format() -> None:
    for value in (["9:30"], ["25:00"], ["09:60"], ["abc"], [930]):
        with pytest.raises(ValueError, match="invalid schedule time"):
            normalize_custom_schedule_times(value)


def test_normalize_custom_schedule_times_rejects_too_many() -> None:
    times = [f"{hour:02d}:{minute:02d}" for hour in range(24) for minute in (0, 30)]
    assert len(times) == MAX_CUSTOM_SCHEDULE_TIMES
    assert normalize_custom_schedule_times(times) is not None

    with pytest.raises(ValueError, match="too many schedule times"):
        normalize_custom_schedule_times(times + ["23:59"])


def test_strategy_schedule_validates_custom_times_on_construction() -> None:
    schedule = StrategySchedule(
        enabled=True,
        interval_minutes=30,
        custom_schedule_times=["09:30", "09:30", "14:00"],
    )
    assert schedule.custom_schedule_times == ("09:30", "14:00")
    assert schedule.schedule_times == ("09:30", "14:00")

    with pytest.raises(ValueError, match="too many schedule times"):
        StrategySchedule(
            enabled=True,
            interval_minutes=30,
            custom_schedule_times=[
                f"{hour:02d}:{minute:02d}" for hour in range(24) for minute in (0, 30)
            ]
            + ["23:59"],
        )


def test_strategy_schedule_apply_update_clears_custom_times() -> None:
    schedule = StrategySchedule(
        enabled=True,
        interval_minutes=30,
        custom_schedule_times=["09:30"],
    )
    schedule.apply_update(
        enabled=False, interval_minutes=30, custom_schedule_times=None
    )
    assert schedule.custom_schedule_times is None
    assert schedule.schedule_times == derive_intraday_schedule_times(30)
