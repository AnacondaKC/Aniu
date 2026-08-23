"""Deterministic routing for the unified Agent K-line tool."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import cast

import pytest

from backend.business.runs.abort import RunAbortSignal
from backend.business.shared import RunAbortError
from backend.infra.integrations.kline_agent_tool import (
    QueryKlineTool,
    resolve_kline_instrument,
)
from backend.infra.integrations.tool_policy import SideEffectLevel
from backend.stock_api.public import (
    IndexKlineRequest,
    InvalidStockRequest,
    KlineRequest,
    PublicStockRequest,
    StockMarketDataService,
)


@dataclass
class RecordingPublicService:
    requests: list[PublicStockRequest] = field(default_factory=list)

    async def execute(
        self, request: PublicStockRequest, _: object | None = None
    ) -> dict[str, object]:
        self.requests.append(request)
        return {
            "data": {"bars": [{"date": "2026-08-14", "close": 100}]},
            "meta": {"source": "tencent"},
        }


def _tool() -> tuple[QueryKlineTool, RecordingPublicService]:
    public = RecordingPublicService()
    return QueryKlineTool(public_service=cast(StockMarketDataService, public)), public


@pytest.mark.parametrize(
    ("value", "instrument_type", "symbol"),
    [
        ("600519", "stock", "600519.SH"),
        ("000001", "stock", "000001.SZ"),
        ("000001.SZ", "stock", "000001.SZ"),
        ("000001.SH", "index", "000001.SH"),
        ("399006", "index", "399006.SZ"),
        ("399006.SZ", "index", "399006.SZ"),
        ("创业板指", "index", "399006.SZ"),
    ],
)
def test_resolver_distinguishes_stocks_and_indexes(
    value: str, instrument_type: str, symbol: str
) -> None:
    resolved = resolve_kline_instrument(value)

    assert resolved.instrument_type == instrument_type
    assert resolved.symbol == symbol


@pytest.mark.parametrize("value", ["600519.SZ", "399006.SH", "半导体板块", ""])
def test_resolver_rejects_ambiguous_or_unsupported_instruments(value: str) -> None:
    with pytest.raises(InvalidStockRequest, match="K 线标的"):
        resolve_kline_instrument(value)


def test_query_kline_schema_exposes_one_closed_intent_based_tool() -> None:
    tool, _ = _tool()

    definition = tool.to_tool_definition()
    parameters = definition["parameters"]

    assert definition["name"] == "query_kline"
    assert parameters["required"] == ["instrument"]
    assert parameters["additionalProperties"] is False
    assert "个股或指数" in definition["description"]
    assert "000001.SH" in parameters["properties"]["instrument"]["description"]
    assert parameters["properties"]["start_date"]["pattern"] == (r"^\d{4}-\d{2}-\d{2}$")
    assert tool.side_effect_level is SideEffectLevel.READ
    assert tool.enabled_stages == ("Run",)
    assert tool.execution_mode == "parallel"


@pytest.mark.asyncio
async def test_query_kline_routes_a_share_to_public_data() -> None:
    tool, public = _tool()

    result = await tool.run("000001", period="day", adjust="qfq", limit=30)

    assert len(public.requests) == 1
    request = public.requests[0]
    assert isinstance(request, KlineRequest)
    assert request.symbol == "000001.SZ"
    assert request.limit == 30
    assert result["meta"] == {
        "source": "tencent",
        "instrument_type": "stock",
        "resolved_instrument": "000001.SZ",
        "data_source": "public",
    }


@pytest.mark.asyncio
async def test_query_kline_routes_index_to_public_data() -> None:
    tool, public = _tool()

    result = await tool.run(
        "000001.SH",
        period="day",
        start_date="2026-07-01",
        end_date="2026-08-14",
        limit=30,
    )

    assert len(public.requests) == 1
    request = public.requests[0]
    assert isinstance(request, IndexKlineRequest)
    assert request.symbol == "000001.SH"
    assert request.period == "day"
    assert request.start_date == "2026-07-01"
    assert request.end_date == "2026-08-14"
    assert request.limit == 30
    assert request.adjust == "none"
    assert result["meta"] == {
        "source": "tencent",
        "instrument_type": "index",
        "resolved_instrument": "000001.SH",
        "data_source": "public",
    }


@pytest.mark.asyncio
async def test_query_kline_supports_public_index_minute_periods() -> None:
    tool, public = _tool()

    await tool.run("创业板指", period="1m", adjust="hfq", limit=300)

    request = public.requests[0]
    assert isinstance(request, IndexKlineRequest)
    assert request.symbol == "399006.SZ"
    assert request.period == "1m"
    assert request.limit == 300
    assert request.adjust == "none"


@pytest.mark.asyncio
async def test_query_kline_aborts_inflight_index_request() -> None:
    class BlockingPublicService:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.cancelled = False
            self.request: PublicStockRequest | None = None

        async def execute(
            self, request: PublicStockRequest, abort_signal: RunAbortSignal | None
        ) -> dict[str, object]:
            self.request = request
            self.started.set()
            assert abort_signal is not None
            await abort_signal.wait()
            try:
                abort_signal.throw_if_aborted()
            except RunAbortError:
                self.cancelled = True
                raise
            raise AssertionError("unreachable")

    public = BlockingPublicService()
    tool = QueryKlineTool(public_service=cast(StockMarketDataService, public))
    abort_signal = RunAbortSignal(run_id=42)
    task = asyncio.create_task(
        tool.run_with_abort("000001.SH", abort_signal=abort_signal)
    )

    await public.started.wait()
    abort_signal.abort()

    with pytest.raises(RunAbortError):
        await task
    assert public.cancelled is True
    assert isinstance(public.request, IndexKlineRequest)


def test_query_kline_logs_public_index_source() -> None:
    tool, _ = _tool()

    assert tool.stock_api_log_parameters(
        {"instrument": "000001.SH", "period": "day", "limit": 30}
    ) == {
        "instrument": "000001.SH",
        "period": "day",
        "limit": 30,
        "instrument_type": "index",
        "resolved_instrument": "000001.SH",
        "data_source": "public",
    }
