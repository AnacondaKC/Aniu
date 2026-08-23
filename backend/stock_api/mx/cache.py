"""Short-lived, single-flight cache for MX read responses."""

from __future__ import annotations

import asyncio
import copy
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TypeVar, cast

_T = TypeVar("_T")


@dataclass(slots=True)
class _Entry:
    value: object
    expires_at: float
    generation: int


@dataclass(slots=True)
class _LockEntry:
    lock: asyncio.Lock
    users: int = 0


class MxReadCache:
    """Cache only successful normalized reads for three seconds per key."""

    def __init__(self, ttl_seconds: float = 3.0, max_entries: int = 64) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be positive")
        self._ttl_seconds = ttl_seconds
        self._max_entries = max_entries
        self._entries: dict[str, _Entry] = {}
        self._locks: dict[str, _LockEntry] = {}
        self._generation = 0
        self._lock = asyncio.Lock()

    async def get_or_load(self, key: str, loader: Callable[[], Awaitable[_T]]) -> _T:
        cached = self._get(key)
        if cached is not None:
            return cast(_T, cached)
        async with self._single_flight(key):
            cached = self._get(key)
            if cached is not None:
                return cast(_T, cached)
            generation = self._generation
            value = await loader()
            if generation == self._generation:
                now = time.monotonic()
                self._prune_expired(now)
                self._entries[key] = _Entry(
                    value=copy.deepcopy(value),
                    expires_at=now + self._ttl_seconds,
                    generation=generation,
                )
                while len(self._entries) > self._max_entries:
                    oldest_key = next(iter(self._entries))
                    self._entries.pop(oldest_key, None)
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

    def clear(self) -> None:
        self._generation += 1
        self._entries.clear()

    def _prune_expired(self, now: float) -> None:
        expired_keys = [
            key
            for key, entry in self._entries.items()
            if entry.generation != self._generation or entry.expires_at <= now
        ]
        for key in expired_keys:
            self._entries.pop(key, None)

    def _get(self, key: str) -> object | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.generation != self._generation or entry.expires_at <= time.monotonic():
            self._entries.pop(key, None)
            return None
        return copy.deepcopy(entry.value)


__all__ = ["MxReadCache"]
