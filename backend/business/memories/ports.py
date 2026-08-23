"""Ports for the memories feature."""

from __future__ import annotations

from typing import Protocol

from backend.business.memories.models import (
    MemoryActivity,
    MemoryItem,
    MemoryMatchMode,
    MemoryWriteCommand,
)


class MemoryRepositoryPort(Protocol):
    async def search(
        self,
        *,
        keywords: str,
        match_mode: MemoryMatchMode,
        limit: int,
    ) -> list[MemoryItem]: ...

    async def write(self, command: MemoryWriteCommand) -> MemoryItem: ...

    async def list_items(
        self, *, limit: int, offset: int, keywords: str = ""
    ) -> list[MemoryItem]: ...

    async def count_items(self, *, keywords: str = "") -> int: ...

    async def list_activities(
        self,
        *,
        limit: int,
        offset: int,
        task_id: int | None = None,
        operation: str | None = None,
    ) -> list[MemoryActivity]: ...

    async def count_activities(
        self,
        *,
        task_id: int | None = None,
        operation: str | None = None,
    ) -> int: ...

    async def record_read(
        self,
        *,
        keywords: str,
        result_count: int,
        task_id: int = 0,
    ) -> MemoryActivity: ...


__all__ = ["MemoryRepositoryPort"]
