"""Read models for completed Run reports consumed by maintenance tasks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class RunReportRecord:
    run_id: int
    started_at: datetime
    completed_at: datetime | None
    content: str

    def as_payload(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "completed_at": (
                None if self.completed_at is None else self.completed_at.isoformat()
            ),
            "content": self.content,
        }


__all__ = ["RunReportRecord"]
