from __future__ import annotations

import asyncio

import pytest

from backend.stock_api.mx.cache import MxReadCache


@pytest.mark.asyncio
async def test_mx_read_cache_single_flight_and_returns_independent_copies() -> None:
    cache = MxReadCache()
    calls = 0
    started = asyncio.Event()
    release = asyncio.Event()

    async def load() -> dict[str, list[int]]:
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return {"values": [1, 2, 3]}

    tasks = [
        asyncio.create_task(cache.get_or_load("positions", load)) for _ in range(8)
    ]
    await started.wait()
    release.set()
    values = await asyncio.gather(*tasks)

    assert calls == 1
    assert all(value == {"values": [1, 2, 3]} for value in values)
    values[0]["values"].append(4)  # type: ignore[index]
    cached = await cache.get_or_load("positions", load)
    assert cached == {"values": [1, 2, 3]}
    assert calls == 1
    assert cache._locks == {}


@pytest.mark.asyncio
async def test_mx_read_cache_bounds_dynamic_keys() -> None:
    cache = MxReadCache(max_entries=4)

    async def load() -> dict[str, bool]:
        return {"ok": True}

    for index in range(10):
        await cache.get_or_load(f"dynamic:{index}", load)

    assert len(cache._entries) == 4
    assert cache._locks == {}


@pytest.mark.asyncio
async def test_mx_read_cache_clear_invalidates_inflight_write() -> None:
    cache = MxReadCache()
    release = asyncio.Event()
    calls = 0

    async def load() -> dict[str, int]:
        nonlocal calls
        calls += 1
        await release.wait()
        return {"call": calls}

    task = asyncio.create_task(cache.get_or_load("account", load))
    await asyncio.sleep(0)
    cache.clear()
    release.set()

    assert await task == {"call": 1}
    assert await cache.get_or_load("account", load) == {"call": 2}
    assert calls == 2


@pytest.mark.asyncio
async def test_mx_read_cache_expires_entries() -> None:
    cache = MxReadCache(ttl_seconds=0)
    calls = 0

    async def load() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert await cache.get_or_load("orders", load) == 1
    assert await cache.get_or_load("orders", load) == 2
    assert calls == 2
