"""Schedule API schemas."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from backend.api.schemas.common import ApiModel


class StrategyScheduleResponse(ApiModel):
    schedule_id: int
    enabled: bool
    task_type: str
    interval_minutes: int
    custom_schedule_times: list[str] | None = None
    schedule_times: list[str]
    revision: int
    runtime_synced_revision: int
    sync_error: str | None = None
    updated_at: datetime


class SaveScheduleFields(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_type: Literal["market_analysis"]
    interval_minutes: int = Field(ge=15)
    schedule_times: list[str] | None = None
    enabled: bool = True


class CreateScheduleRequest(SaveScheduleFields):
    """Create one market-analysis schedule."""


class UpdateScheduleRequest(SaveScheduleFields):
    expected_revision: int = Field(ge=0)
