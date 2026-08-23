from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import cast

import pytest

from backend.stock_api.public.contracts import (
    ConnectMoneyFlowRequest,
    IndexKlineRequest,
    KlineRequest,
    QuoteSnapshotRequest,
    StockReportsRequest,
)
from backend.stock_api.public.router import PublicStockRouter
from backend.stock_api.public.service import (
    StockMarketDataService,
    _ttl_seconds,
    bound_agent_result,
)


def serialized(value: object) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")))


@dataclass
class FakeRouter:
    result: dict[str, object]
    calls: int = 0

    async def execute(self, *_: object, **__: object) -> dict[str, object]:
        self.calls += 1
        return self.result


@dataclass
class BlockingRouter:
    result: dict[str, object]
    calls: int = 0
    started: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)

    async def execute(self, *_: object, **__: object) -> dict[str, object]:
        self.calls += 1
        self.started.set()
        await self.release.wait()
        return self.result


def test_ttl_uses_request_period_and_domain() -> None:
    assert _ttl_seconds(IndexKlineRequest("000001.SH", period="1m")) == 5
    assert _ttl_seconds(KlineRequest("600519.SH", period="1m")) == 10
    assert _ttl_seconds(KlineRequest("600519.SH", period="5m")) == 10
    assert _ttl_seconds(KlineRequest("600519.SH", period="day")) == 60
    assert _ttl_seconds(KlineRequest("600519.SH", period="week")) == 60
    assert _ttl_seconds(QuoteSnapshotRequest(("600519.SH",))) == 3


@pytest.mark.asyncio
async def test_service_cache_keeps_complete_result_and_agent_bound_copy() -> None:
    result = {
        "data": {
            "quotes": [
                {
                    "symbol": "600519.SH",
                    "name": "贵州茅台" + ("x" * 1_500),
                    "price": 1_600,
                }
                for _ in range(60)
            ]
        },
        "meta": {"source": "eastmoney", "warnings": []},
    }
    router = FakeRouter(result)
    service = StockMarketDataService(cast(PublicStockRouter, router))
    request = QuoteSnapshotRequest(("600519.SH",))

    complete = await service.execute(request)
    assert serialized(complete) > 64_000
    assert (
        len(cast(list[object], cast(dict[str, object], complete["data"])["quotes"]))
        == 60
    )
    bounded = bound_agent_result(complete, request)

    assert serialized(bounded) <= 64_000
    assert cast(dict[str, object], bounded["meta"])["output_truncated"] is True
    assert cast(dict[str, object], complete["meta"]).get("output_truncated") is None
    assert (
        len(cast(list[object], cast(dict[str, object], complete["data"])["quotes"]))
        == 60
    )

    bounded["meta"] = {"changed": True}
    cached = await service.execute(request)
    assert router.calls == 1
    assert cast(dict[str, object], cached["meta"]).get("changed") is None
    assert (
        len(cast(list[object], cast(dict[str, object], cached["data"])["quotes"])) == 60
    )


@pytest.mark.asyncio
async def test_service_single_flights_concurrent_same_request() -> None:
    router = BlockingRouter({"data": {"value": 1}, "meta": {}})
    service = StockMarketDataService(cast(PublicStockRouter, router))
    request = QuoteSnapshotRequest(("600519.SH",))

    first = asyncio.create_task(service.execute(request))
    await router.started.wait()
    second = asyncio.create_task(service.execute(request))
    await asyncio.sleep(0)
    router.release.set()

    results = await asyncio.gather(first, second)

    assert router.calls == 1
    assert results[0] == results[1]


@pytest.mark.asyncio
async def test_agent_bound_result_samples_series_and_keeps_identity() -> None:
    bars = [
        {
            "time": str(index),
            "open": 1,
            "high": 2,
            "low": 0,
            "close": 1,
            "note": "x" * 500,
        }
        for index in range(300)
    ]
    request = KlineRequest("600519.SH", limit=300)
    result = {
        "data": {"symbol": request.symbol, "bars": bars},
        "meta": {"source": "eastmoney", "warnings": []},
    }
    bounded = bound_agent_result(result, request)
    returned = cast(
        list[dict[str, object]], cast(dict[str, object], bounded["data"])["bars"]
    )
    assert serialized(bounded) <= 64_000
    assert returned[0]["time"] == "0"
    assert returned[-1]["time"] == "299"
    assert cast(dict[str, object], bounded["data"])["sampled"] is True
    assert cast(dict[str, object], bounded["meta"])["original_count"] == 300


@pytest.mark.asyncio
async def test_agent_bound_result_keeps_both_connect_directions() -> None:
    rows = [{"date": str(index), "amount": "x" * 500} for index in range(120)]
    request = ConnectMoneyFlowRequest(direction="all", limit=120)
    result = {
        "data": {
            "direction": "all",
            "northbound": rows.copy(),
            "southbound": rows.copy(),
        },
        "meta": {"source": "eastmoney", "warnings": []},
    }
    bounded = bound_agent_result(result, request)
    data = cast(dict[str, object], bounded["data"])
    assert serialized(bounded) <= 64_000
    assert data["northbound"]
    assert data["southbound"]
    meta = cast(dict[str, object], bounded["meta"])
    assert meta["original_counts"] == {"northbound": 120, "southbound": 120}


def test_agent_bound_result_shrinks_full_report_without_mutating_source() -> None:
    request = StockReportsRequest("600519.SH", content="full", report_id="RPT_1")
    result = {
        "data": {
            "report": {"report_id": "RPT_1", "found": True, "content": "x" * 70_000}
        },
        "meta": {"source": "eastmoney", "warnings": []},
    }
    bounded = bound_agent_result(result, request)
    content = cast(
        dict[str, object], cast(dict[str, object], bounded["data"])["report"]
    )["content"]
    assert serialized(bounded) <= 64_000
    assert len(cast(str, content)) == 2_000
    assert (
        len(
            cast(
                str,
                cast(
                    dict[str, object], cast(dict[str, object], result["data"])["report"]
                )["content"],
            )
        )
        == 70_000
    )
