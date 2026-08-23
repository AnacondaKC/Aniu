"""Memory-bound behavior for the public stock data cache."""

from __future__ import annotations

import asyncio

import pytest

from backend.stock_api.public.cache import PublicStockDataCache


@pytest.mark.parametrize("maximum_entries", [0, -1])
def test_public_cache_rejects_non_positive_capacity(maximum_entries: int) -> None:
    with pytest.raises(ValueError, match="maximum_entries must be positive"):
        PublicStockDataCache(maximum_entries=maximum_entries)


@pytest.mark.asyncio
async def test_public_cache_releases_single_flight_locks() -> None:
    cache = PublicStockDataCache(maximum_entries=2)
    release = asyncio.Event()
    calls = 0

    async def load() -> dict[str, object]:
        nonlocal calls
        calls += 1
        await release.wait()
        return {"ok": True}

    tasks = [
        asyncio.create_task(cache.get_or_load("same", load, 60.0)) for _ in range(5)
    ]
    await asyncio.sleep(0)
    release.set()

    assert await asyncio.gather(*tasks) == [{"ok": True}] * 5
    assert calls == 1
    assert cache._locks == {}


@pytest.mark.asyncio
async def test_public_cache_bounds_dynamic_keys() -> None:
    cache = PublicStockDataCache(maximum_entries=2)

    async def load() -> dict[str, object]:
        return {"ok": True}

    for index in range(5):
        await cache.get_or_load(f"dynamic:{index}", load, 60.0)

    assert len(cache._entries) == 2
    assert cache._locks == {}


@pytest.mark.asyncio
async def test_public_cache_clear_invalidates_an_in_flight_loader() -> None:
    cache = PublicStockDataCache()
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def load() -> dict[str, object]:
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            await release.wait()
            return {"version": "stale"}
        return {"version": "fresh"}

    task = asyncio.create_task(cache.get_or_load("same", load, 60.0))
    await started.wait()
    await cache.clear()
    release.set()

    assert await task == {"version": "fresh"}
    assert calls == 2
    assert await cache.get("same") == {"version": "fresh"}
