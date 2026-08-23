from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from backend.api.schemas.common import ApiModel


class MemoryActivityResponse(ApiModel):
    id: int
    operation: Literal["read", "create", "update", "delete"]
    memory_id: int | None
    content: str
    result_count: int | None
    task_id: int
    created_at: datetime


class MemoryItemResponse(ApiModel):
    id: int
    content: str
    reason: str
    created_task_id: int
    updated_task_id: int
    version: int
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None


class MemoryOverviewResponse(ApiModel):
    activities: list[MemoryActivityResponse]
    activity_total: int
    items: list[MemoryItemResponse]
    item_total: int
    item_match_total: int
    generated_at: datetime


class CreateMemoryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    content: str = Field(min_length=1, max_length=2000)
    reason: str = Field(min_length=1, max_length=2000)


class UpdateMemoryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_version: int = Field(ge=1)
    content: str = Field(min_length=1, max_length=2000)
    reason: str = Field(min_length=1, max_length=2000)


class DeleteMemoryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_version: int = Field(ge=1)
