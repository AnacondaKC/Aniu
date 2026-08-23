"""Schedule timing helpers and the persisted schedule entity."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime

from backend.business.settings.prompt import utc_now
from backend.business.shared.trading.value_objects import ensure_positive_int

MARKET_ANALYSIS_TASK_TYPE = "market_analysis"
ALLOWED_TASK_TYPES = {MARKET_ANALYSIS_TASK_TYPE}
MORNING_START_MINUTES = 9 * 60 + 30
MORNING_END_MINUTES = 11 * 60
AFTERNOON_START_MINUTES = 13 * 60
AFTERNOON_END_MINUTES = 14 * 60 + 30

_TIME_PATTERN = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
MAX_CUSTOM_SCHEDULE_TIMES = 48


def _minutes_to_time_string(total_minutes: int) -> str:
    hour, minute = divmod(total_minutes, 60)
    return f"{hour:02d}:{minute:02d}"


def derive_intraday_schedule_times(interval_minutes: int) -> tuple[str, ...]:
    """Derive weekday market-session trigger times from one interval."""

    if interval_minutes < 15:
        raise ValueError("interval_minutes must be >= 15")
    times: list[str] = []
    for start, end in (
        (MORNING_START_MINUTES, MORNING_END_MINUTES),
        (AFTERNOON_START_MINUTES, AFTERNOON_END_MINUTES),
    ):
        current = start
        while current <= end:
            times.append(_minutes_to_time_string(current))
            current += interval_minutes
    return tuple(times)


def normalize_custom_schedule_times(
    times: tuple[str, ...] | list[str] | None,
) -> tuple[str, ...] | None:
    """Validate and dedupe user-provided HH:MM trigger times (keep order)."""

    if times is None:
        return None
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in times:
        value = str(raw).strip()
        if not _TIME_PATTERN.match(value):
            raise ValueError(f"invalid schedule time: {value}")
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    if len(normalized) > MAX_CUSTOM_SCHEDULE_TIMES:
        raise ValueError(
            f"too many schedule times: at most {MAX_CUSTOM_SCHEDULE_TIMES} allowed"
        )
    return tuple(normalized) if normalized else None


@dataclass(slots=True)
class StrategySchedule:
    """Persisted schedule input plus scheduler synchronization state."""

    interval_minutes: int
    enabled: bool = True
    custom_schedule_times: tuple[str, ...] | None = None
    schedule_id: int = 0
    revision: int = 0
    runtime_synced_revision: int = 0
    sync_error: str | None = None
    updated_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        if self.schedule_id < 0:
            raise ValueError("schedule_id must be >= 0")
        self.interval_minutes = ensure_positive_int(
            self.interval_minutes, "interval_minutes"
        )
        if self.interval_minutes < 15:
            raise ValueError("interval_minutes must be >= 15")
        self.custom_schedule_times = normalize_custom_schedule_times(
            self.custom_schedule_times
        )

    @property
    def task_type(self) -> str:
        return MARKET_ANALYSIS_TASK_TYPE

    @property
    def schedule_times(self) -> tuple[str, ...]:
        """Effective weekday trigger times (custom when set, else interval-derived)."""

        if self.custom_schedule_times:
            return self.custom_schedule_times
        return derive_intraday_schedule_times(self.interval_minutes)

    def apply_update(
        self,
        *,
        enabled: bool,
        interval_minutes: int,
        custom_schedule_times: tuple[str, ...] | list[str] | None = None,
    ) -> None:
        self.enabled = enabled
        self.interval_minutes = interval_minutes
        self.custom_schedule_times = normalize_custom_schedule_times(
            custom_schedule_times
        )
        self.revision += 1
        self.sync_error = None
        self.updated_at = utc_now()
        self.__post_init__()
