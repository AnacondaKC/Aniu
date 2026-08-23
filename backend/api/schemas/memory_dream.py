"""API schemas for nightly memory dreams."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from backend.api.schemas.common import ApiModel
from backend.api.schemas.memory import MemoryActivityResponse


class MemoryDreamResponse(ApiModel):
    task_id: int
    target_date: date
    status: Literal["pending", "running", "completed", "failed"]
    result: str | None
    failure_reason: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None


class MemoryDreamListResponse(ApiModel):
    items: list[MemoryDreamResponse]
    total: int
    latest: MemoryDreamResponse | None


class MemoryDreamDetailResponse(ApiModel):
    dream: MemoryDreamResponse
    activities: list[MemoryActivityResponse]
    activity_total: int
