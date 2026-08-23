"""Tests for the mx moni client mapping layer."""

from __future__ import annotations

import asyncio
from datetime import date

import httpx
import pytest

from backend.business.shared import ServiceIntegrationError
from backend.business.shared.stock_api_source import (
    STOCK_API_SOURCE_OVERVIEW_REFRESH,
    stock_api_source,
)
from backend.stock_api.models import StockApiCall
from backend.stock_api.mx import MxMoniClient
from backend.stock_api.mx.moni import _normalize_order_status


def make_transport() -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/balance"):
            return httpx.Response(
                200,
                json={
                    "code": 200,
                    "data": {
                        "totalAssets": 100000.0,
                        "availBalance": 25000.0,
                        "frozenMoney": 1000.0,
                        "totalPosValue": 74000.0,
                        "initMoney": 92000.0,
                        "nav": 1.08696,
                        "totalPosPct": 74.0,
                        "openDate": 20260412,
                        "oprDays": 15,
                        "dailyProfit": 500.0,
                        "currencyUnit": 1,
                    },
                },
            )
        if request.url.path.endswith("/positions"):
            return httpx.Response(
                200,
                json={
                    "code": 200,
                    "data": {
                        "currencyUnit": 1,
                        "posList": [
                            {
                                "secCode": "600519",
                                "secName": "贵州茅台",
                                "count": 100,
                                "costPriceDec": 2,
                                "costPrice": 160000,
                                "priceDec": 2,
                                "price": 170000,
                                "value": 170000.0,
                                "profitPct": 6.25,
                                "dayProfit": 88.8,
                                "availableQty": 80,
                            }
                        ],
                    },
                },
            )
        if request.url.path.endswith("/orders"):
            return httpx.Response(
                200,
                json={
                    "code": 200,
                    "data": {
                        "orders": [
                            {
                                "id": "order-1",
                                "secCode": "000001",
                                "secName": "平安银行",
                                "drt": 1,
                                "count": 200,
                                "priceDec": 2,
                                "price": 1234,
                                "dbStatus": 200,
                                "tradeCount": 200,
                                "tradePrice": 1230,
                                "time": 1777061400,
                            }
                        ]
                    },
                },
            )
        return httpx.Response(404, json={"code": 404, "message": request.url.path})

    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_api_key_resolver_is_authoritative(monkeypatch) -> None:
    monkeypatch.setenv("MX_APIKEY", "environment-key")

    async def resolve_empty() -> str | None:
        return None

    client = MxMoniClient(api_key_resolver=resolve_empty)

    assert await client._resolve_api_key() is None  # noqa: SLF001


@pytest.mark.asyncio
async def test_mx_moni_client_maps_balance_positions_and_orders() -> None:
    client = MxMoniClient(
        api_key="test-key",
        base_url="https://example.test",
        transport=make_transport(),
    )

    overview = await client.get_account_snapshot()
    positions = await client.get_positions()
    orders = await client.get_orders()

    assert overview.total_asset == 100000.0
    assert overview.total_profit == 8000.0
    assert overview.market_value == 74000.0
    assert overview.initial_capital == 92000.0
    assert overview.net_value == 1.08696
    assert overview.position_ratio == 0.74
    assert overview.open_date == date(2026, 4, 12)
    assert overview.operation_days == 15
    assert positions[0].symbol == "600519"
    assert positions[0].avg_cost == 1600.0
    assert positions[0].current_price == 1700.0
    assert positions[0].profit_ratio == 0.0625
    assert positions[0].day_profit == 88.8
    assert positions[0].available_quantity == 80
    assert orders[0].order_id == "order-1"
    assert orders[0].direction == "BUY"
    assert orders[0].status == "FILLED"
    assert orders[0].order_price == 12.34
    assert orders[0].filled_quantity == 200
    assert orders[0].filled_price == 12.3
    assert orders[0].submitted_at is not None


@pytest.mark.asyncio
async def test_mx_moni_invalidates_read_cache_when_api_key_changes() -> None:
    api_key = "key-a"
    calls = 0

    async def resolve_api_key() -> str | None:
        return api_key

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.headers["apikey"] == api_key
        return httpx.Response(
            200,
            json={
                "code": 200,
                "data": {
                    "totalAssets": 100 if api_key == "key-a" else 200,
                    "currencyUnit": 1,
                },
            },
        )

    client = MxMoniClient(
        api_key_resolver=resolve_api_key,
        base_url="https://example.test",
        transport=httpx.MockTransport(handler),
        request_interval=0,
    )

    assert (await client.get_account_snapshot()).total_asset == 100
    assert (await client.get_account_snapshot()).total_asset == 100
    assert calls == 1

    api_key = "key-b"
    assert (await client.get_account_snapshot()).total_asset == 200
    assert calls == 2

    api_key = ""
    with pytest.raises(ServiceIntegrationError, match="MX_APIKEY is not configured"):
        await client.get_account_snapshot()
    assert calls == 2
    await client.aclose()

    calls: list[StockApiCall] = []

    async def record(call: StockApiCall) -> None:
        calls.append(call)

    client = MxMoniClient(
        api_key="test-mx-key",
        base_url="https://example.test",
        transport=make_transport(),
        call_logger=record,
    )

    with stock_api_source(STOCK_API_SOURCE_OVERVIEW_REFRESH):
        await client.get_account_snapshot()

    assert calls[0].source == "overview_refresh"
    assert calls[0].interface_name == "模拟交易"
    assert calls[0].interface_identifier == "模拟交易 · 账户资金"
    await client.aclose()


@pytest.mark.parametrize(
    "raw_value,expected",
    [
        ("1", "PENDING"),
        ("2", "PENDING"),
        ("3", "PARTIAL"),
        ("4", "FILLED"),
        ("5", "PARTIAL_PENDING_CANCEL"),
        ("6", "PENDING_CANCEL"),
        ("7", "PARTIAL_CANCELLED"),
        ("8", "CANCELLED"),
        ("9", "REJECTED"),
        ("10", "CANCEL_FAILED"),
        ("100", "PENDING"),
        ("200", "FILLED"),
        ("204", "CANCELLED"),
        ("206", "CANCELLED"),
        ("203", "CANCELLED"),
        ("999", "UNKNOWN"),
        ("unknown", "UNKNOWN"),
        (2, "PENDING"),
    ],
)
def test_normalize_order_status(raw_value: object, expected: str) -> None:
    assert _normalize_order_status(raw_value) == expected


@pytest.mark.asyncio
async def test_mx_moni_scales_money_using_currency_unit() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/balance")
        return httpx.Response(
            200,
            json={
                "code": "0",
                "data": {
                    "totalAssets": 125680500,
                    "availBalance": 23450000,
                    "frozenMoney": 50000,
                    "totalPosValue": 102230500,
                    "initMoney": 100000000,
                    "currencyUnit": 1000,
                },
            },
        )

    client = MxMoniClient(
        api_key="test-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(handler),
    )

    overview = await client.get_account_snapshot()

    assert overview.total_asset == 125680.5
    assert overview.available_cash == 23450.0
    assert overview.frozen_cash == 50.0
    assert overview.market_value == 102230.5
    assert overview.initial_capital == 100000.0
    assert overview.total_profit == 25680.5
    await client.aclose()


@pytest.mark.asyncio
async def test_mx_moni_position_day_profit_missing_or_placeholder_is_none() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/positions"):
            return httpx.Response(
                200,
                json={
                    "code": 200,
                    "data": {
                        "currencyUnit": 1,
                        "posList": [
                            {
                                "secCode": "600519",
                                "secName": "贵州茅台",
                                "count": 100,
                                "price": 170000,
                                "value": 170000.0,
                                "dayProfit": "--",
                            },
                            {
                                "secCode": "000001",
                                "secName": "平安银行",
                                "count": 200,
                                "price": 1100,
                                "value": 2200.0,
                                "dayProfit": 12.3,
                            },
                        ],
                    },
                },
            )
        if request.url.path.endswith("/balance"):
            return httpx.Response(200, json={"code": 200, "data": {"totalAssets": 1}})
        if request.url.path.endswith("/orders"):
            return httpx.Response(200, json={"code": 200, "data": {"orders": []}})
        return httpx.Response(404, json={"code": 404})

    client = MxMoniClient(
        api_key="test-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(handler),
    )

    positions = await client.get_positions()

    assert positions[0].day_profit is None
    assert positions[1].day_profit == 12.3
    await client.aclose()


@pytest.mark.asyncio
async def test_mx_moni_rejects_balance_without_currency_unit() -> None:
    client = MxMoniClient(
        api_key="test-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={"code": 200, "data": {"totalAssets": 100}},
            )
        ),
    )

    with pytest.raises(ServiceIntegrationError, match="currencyUnit"):
        await client.get_account_snapshot()
    await client.aclose()


@pytest.mark.asyncio
async def test_mx_moni_client_ignores_price_when_order_has_no_fill() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/orders")
        return httpx.Response(
            200,
            json={
                "code": 200,
                "data": {
                    "orders": [
                        {
                            "id": "order-pending",
                            "secCode": "000001",
                            "secName": "平安银行",
                            "drt": 1,
                            "count": 100,
                            "priceDec": 3,
                            "price": 3874,
                            "dbStatus": 100,
                            "tradeCount": 0,
                            "tradePrice": 0,
                            "time": 1777061400,
                        }
                    ]
                },
            },
        )

    client = MxMoniClient(
        api_key="test-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(handler),
    )

    orders = await client.get_orders()

    assert orders[0].status == "PENDING"
    assert orders[0].order_price == 3.874
    assert orders[0].filled_quantity == 0
    assert orders[0].filled_price is None


@pytest.mark.asyncio
async def test_mx_moni_client_maps_real_filled_order_prices() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/orders")
        return httpx.Response(
            200,
            json={
                "code": 200,
                "data": {
                    "orders": [
                        {
                            "id": "order-filled",
                            "secCode": "000001",
                            "secName": "平安银行",
                            "drt": 2,
                            "count": 100,
                            "priceDec": 3,
                            "price": 3874,
                            "status": 4,
                            "dbStatus": 200,
                            "tradeCount": 100,
                            "tradePrice": 3880,
                            "time": 1777061400,
                        }
                    ]
                },
            },
        )

    client = MxMoniClient(
        api_key="test-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(handler),
    )

    orders = await client.get_orders()

    assert orders[0].direction == "SELL"
    assert orders[0].status == "FILLED"
    assert orders[0].order_price == 3.874
    assert orders[0].filled_quantity == 100
    assert orders[0].filled_price == 3.88


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, text="<html>invalid</html>"),
        httpx.Response(200, json=[]),
    ],
)
async def test_mx_moni_client_wraps_malformed_success_response(
    response: httpx.Response,
) -> None:
    client = MxMoniClient(
        api_key="test-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(lambda request: response),
    )

    with pytest.raises(ServiceIntegrationError, match="mx-moni"):
        await client.get_account_snapshot()
    await client.aclose()


@pytest.mark.asyncio
async def test_mx_moni_records_cancelled_error_before_reraising() -> None:
    calls: list[StockApiCall] = []

    async def record(call: StockApiCall) -> None:
        calls.append(call)

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        raise asyncio.CancelledError

    client = MxMoniClient(
        api_key="test-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(handler),
        call_logger=record,
        request_interval=0,
    )

    with pytest.raises(asyncio.CancelledError):
        await client.get_account_snapshot()

    assert calls[0].status == "failed"
    assert calls[0].error_message == "request cancelled"
    await client.aclose()


@pytest.mark.asyncio
async def test_mx_moni_client_does_not_close_shared_http_client() -> None:
    shared_client = httpx.AsyncClient(transport=make_transport())
    first = MxMoniClient(
        api_key="test-key",
        base_url="https://example.test",
        http_client=shared_client,
    )
    second = MxMoniClient(
        api_key="test-key",
        base_url="https://example.test",
        http_client=shared_client,
    )

    await first.get_account_snapshot()
    await second.get_positions()
    await first.aclose()
    await second.aclose()

    assert first._client is shared_client
    assert second._client is shared_client
    assert shared_client.is_closed is False
    await shared_client.aclose()


@pytest.mark.asyncio
async def test_mx_moni_retries_transient_rate_limit_and_returns_balance() -> None:
    calls = 0
    call_logs: list[StockApiCall] = []

    async def record(call: StockApiCall) -> None:
        call_logs.append(call)

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                200,
                json={"code": 503, "message": "请求频率过高"},
            )
        return httpx.Response(
            200,
            json={
                "code": 200,
                "data": {
                    "currencyUnit": 1,
                    "totalAssets": 100,
                    "availBalance": 80,
                    "frozenMoney": 0,
                    "totalPosValue": 20,
                    "initMoney": 100,
                },
            },
        )

    client = MxMoniClient(
        api_key="test-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(handler),
        call_logger=record,
        request_interval=0,
        rate_limit_backoff=0,
        rate_limit_retries=1,
    )

    snapshot = await client.get_account_snapshot()

    assert snapshot.total_asset == 100
    assert calls == 2
    assert [call.status for call in call_logs] == ["failed", "success"]
    await client.aclose()


@pytest.mark.asyncio
async def test_mx_moni_preserves_final_rate_limit_detail_and_status() -> None:
    call_logs: list[StockApiCall] = []

    async def record(call: StockApiCall) -> None:
        call_logs.append(call)

    client = MxMoniClient(
        api_key="test-mx-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                429,
                headers={"Retry-After": "6"},
                json={"message": "请求频率过高，token=test-mx-key"},
            )
        ),
        call_logger=record,
        request_interval=0,
        rate_limit_backoff=0,
        rate_limit_retries=1,
    )

    with pytest.raises(ServiceIntegrationError) as raised:
        await client.get_account_snapshot()

    assert raised.value.status_code == 429
    assert raised.value.error_code.value == "rate_limit"
    assert raised.value.retry_after == "6"
    assert "请求频率过高" in str(raised.value)
    assert "test-mx-key" not in str(raised.value)
    assert len(call_logs) == 2
    assert all("test-mx-key" not in (call.error_message or "") for call in call_logs)
    await client.aclose()
