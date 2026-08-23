"""Domain models for simple, task-sourced agent memories."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class MemoryOperation(StrEnum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


class MemoryActivityOperation(StrEnum):
    READ = "read"
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


class MemoryMatchMode(StrEnum):
    AND = "and"
    OR = "or"


@dataclass(frozen=True, slots=True)
class MemoryItem:
    id: int
    content: str
    reason: str
    created_task_id: int
    updated_task_id: int
    version: int
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class MemoryActivity:
    id: int
    operation: MemoryActivityOperation
    memory_id: int | None
    content: str
    result_count: int | None
    task_id: int
    created_at: datetime


@dataclass(frozen=True, slots=True)
class MemoryOverview:
    activities: tuple[MemoryActivity, ...]
    activity_total: int
    items: tuple[MemoryItem, ...]
    item_total: int
    item_match_total: int
    generated_at: datetime


@dataclass(frozen=True, slots=True)
class MemoryWriteCommand:
    operation: MemoryOperation
    task_id: int
    memory_id: int | None = None
    content: str | None = None
    reason: str | None = None
    expected_version: int | None = None


__all__ = [
    "MemoryActivity",
    "MemoryActivityOperation",
    "MemoryItem",
    "MemoryMatchMode",
    "MemoryOperation",
    "MemoryOverview",
    "MemoryWriteCommand",
]
