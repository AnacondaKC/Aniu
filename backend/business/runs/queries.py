"""Queries for strategy runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from backend.business.shared.trading.value_objects import ensure_positive_int


@dataclass(frozen=True, slots=True)
class ListRunsQuery:
    """List strategy runs with pagination."""

    limit: int = 50
    offset: int = 0
    started_date: date | None = None

    def __post_init__(self) -> None:
        ensure_positive_int(self.limit, "limit")
        if self.offset < 0:
            raise ValueError("offset must be >= 0")


@dataclass(frozen=True, slots=True)
class GetRunDetailQuery:
    """Fetch one run detail by run id."""

    run_id: int

    def __post_init__(self) -> None:
        ensure_positive_int(self.run_id, "run_id")
