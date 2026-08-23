"""Small process-local TTL cache for normalized public market responses."""

from __future__ import annotations

import asyncio
import copy
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass


@dataclass(slots=True)
class _CacheEntry:
    expires_at: float
    value: dict[str, object]


@dataclass(slots=True)
class _LockEntry:
    lock: asyncio.Lock
    users: int = 0


class PublicStockDataCache:
    """Bounded cache that preserves response metadata and coalesces misses."""

    def __init__(self, *, maximum_entries: int = 512) -> None:
        if maximum_entries < 1:
            raise ValueError("maximum_entries must be positive")
        self._maximum_entries = maximum_entries
        self._entries: dict[str, _CacheEntry] = {}
        self._locks: dict[str, _LockEntry] = {}
        self._generation = 0
        self._lock = asyncio.Lock()

    async def get_or_load(
        self,
        key: str,
        loader: Callable[[], Awaitable[dict[str, object]]],
        ttl_seconds: float,
    ) -> dict[str, object]:
        cached = await self.get(key)
        if cached is not None:
            return cached
        async with self._single_flight(key):
            while True:
                cached = await self.get(key)
                if cached is not None:
                    return cached
                generation = await self._current_generation()
                value = await loader()
                if await self._put_if_generation(
                    key,
                    value,
                    ttl_seconds,
                    generation=generation,
                ):
                    return copy.deepcopy(value)

    @asynccontextmanager
    async def _single_flight(self, key: str) -> AsyncIterator[None]:
        async with self._lock:
            entry = self._locks.setdefault(key, _LockEntry(asyncio.Lock()))
            entry.users += 1
        try:
            async with entry.lock:
                yield
        finally:
            async with self._lock:
                entry.users -= 1
                if entry.users == 0 and self._locks.get(key) is entry:
                    self._locks.pop(key, None)

    async def get(self, key: str) -> dict[str, object] | None:
        now = time.monotonic()
        async with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.expires_at <= now:
                del self._entries[key]
                return None
            return copy.deepcopy(entry.value)

    async def _current_generation(self) -> int:
        async with self._lock:
            return self._generation

    async def _put_if_generation(
        self,
        key: str,
        value: dict[str, object],
        ttl_seconds: float,
        *,
        generation: int,
    ) -> bool:
        async with self._lock:
            if generation != self._generation:
                return False
            self._put_locked(key, value, ttl_seconds)
            return True

    async def put(self, key: str, value: dict[str, object], ttl_seconds: float) -> None:
        async with self._lock:
            self._put_locked(key, value, ttl_seconds)

    def _put_locked(
        self,
        key: str,
        value: dict[str, object],
        ttl_seconds: float,
    ) -> None:
        if ttl_seconds <= 0:
            return
        if len(self._entries) >= self._maximum_entries and key not in self._entries:
            oldest_key = min(
                self._entries,
                key=lambda candidate: self._entries[candidate].expires_at,
            )
            del self._entries[oldest_key]
        self._entries[key] = _CacheEntry(
            expires_at=time.monotonic() + ttl_seconds,
            value=copy.deepcopy(value),
        )

    async def clear(self) -> None:
        async with self._lock:
            self._generation += 1
            self._entries.clear()


__all__ = ["PublicStockDataCache"]
