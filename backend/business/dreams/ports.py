"""Ports for nightly memory-maintenance dreams."""

from __future__ import annotations

from datetime import date
from typing import Protocol

from backend.business.dreams.models import MemoryDream


class DreamRepositoryPort(Protocol):
    async def next_task_id(self, reference_date: date) -> int: ...

    async def get_by_id(self, task_id: int) -> MemoryDream | None: ...

    async def delete(self, task_id: int) -> bool: ...

    async def get_by_date(self, target_date: date) -> MemoryDream | None: ...

    async def add(self, dream: MemoryDream) -> MemoryDream: ...

    async def save(self, dream: MemoryDream) -> MemoryDream: ...

    async def list_recent(self, *, limit: int, offset: int) -> list[MemoryDream]: ...

    async def count(self) -> int: ...

    async def list_pending(self, *, limit: int = 256) -> list[MemoryDream]: ...

    async def list_running(self) -> list[MemoryDream]: ...


class DreamAgentPort(Protocol):
    async def run(self, dream: MemoryDream) -> str: ...


__all__ = ["DreamAgentPort", "DreamRepositoryPort"]
