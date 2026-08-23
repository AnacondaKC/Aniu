"""Tests for MX HTTP transport observability metadata."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from backend.business.shared import ServiceIntegrationError
from backend.business.shared.stock_api_source import (
    STOCK_API_SOURCE_RUN,
    stock_api_source,
    stock_api_tool_context,
)
from backend.stock_api.models import StockApiCall
from backend.stock_api.mx.http import MxHttpTransport


@pytest.mark.asyncio
async def test_mx_http_logs_sanitized_call_metadata() -> None:
    calls: list[StockApiCall] = []

    async def record(call: StockApiCall) -> None:
        calls.append(call)

    client = MxHttpTransport(
        api_key="test-mx-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"code": 200, "data": {}})
        ),
        call_logger=record,
    )

    with (
        stock_api_source(STOCK_API_SOURCE_RUN),
        stock_api_tool_context(
            run_id=1,
            stage_name="Research",
            tool_call_id="tool-1",
            tool_name="query_market_data",
        ) as stock_calls,
    ):
        result = await client.post_data(
            "/api/claw/query",
            {"query": "半导体"},
        )
    assert result == {}
    assert calls[0].source == "run"
    assert stock_calls[0]["provider"] == "mx"
    assert stock_calls[0]["interface_name"] == "金融数据查询"
    assert stock_calls[0]["interface_identifier"] == "金融数据查询"
    assert stock_calls[0]["operation_id"] == "query_market_data"
    assert stock_calls[0]["tool_call_id"] == "tool-1"
    await client.aclose()


@pytest.mark.asyncio
async def test_mx_http_accepts_status_zero_success_envelope() -> None:
    client = MxHttpTransport(
        api_key="test-mx-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={"status": 0, "message": "成功", "data": {"answer": "ok"}},
            )
        ),
    )

    assert await client.post_data("/api/claw/query", {"query": "半导体"}) == {
        "answer": "ok"
    }
    await client.aclose()


@pytest.mark.asyncio
async def test_mx_http_accepts_select_nested_success_code_100() -> None:
    client = MxHttpTransport(
        api_key="test-mx-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={"status": 0, "data": {"code": 100, "result": []}},
            )
        ),
    )

    assert await client.post_data("/api/claw/stock-screen", {"query": "低估值"}) == {
        "code": 100,
        "result": [],
    }
    await client.aclose()


@pytest.mark.asyncio
async def test_mx_http_accepts_direct_paper_trade_success_response() -> None:
    client = MxHttpTransport(
        api_key="test-mx-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={"orderId": "order-1", "status": "submitted"},
            )
        ),
    )

    assert await client.post_envelope(
        "/api/claw/mockTrading/trade",
        {"type": "buy"},
    ) == {"orderId": "order-1", "status": "submitted"}
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("endpoint", "body", "response", "expected_identifier"),
    [
        (
            "/api/claw/mockTrading/trade",
            {"type": "buy"},
            {"orderId": "order-1", "status": "submitted"},
            "模拟交易 · 下单",
        ),
        (
            "/api/claw/mockTrading/cancel",
            {"orderId": "order-1"},
            {"status": "cancelled"},
            "模拟交易 · 撤销委托",
        ),
    ],
)
async def test_mx_http_logs_simulated_trading_metadata(
    endpoint: str,
    body: dict[str, str],
    response: dict[str, str],
    expected_identifier: str,
) -> None:
    calls: list[StockApiCall] = []

    async def record(call: StockApiCall) -> None:
        calls.append(call)

    client = MxHttpTransport(
        api_key="test-mx-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=response)
        ),
        call_logger=record,
    )

    await client.post_envelope(endpoint, body)

    assert calls[0].interface_name == "模拟交易"
    assert calls[0].interface_identifier == expected_identifier
    await client.aclose()
    calls: list[StockApiCall] = []

    async def record(call: StockApiCall) -> None:
        calls.append(call)

    client = MxHttpTransport(
        api_key="test-mx-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={
                    "success": True,
                    "code": 0,
                    "data": {"code": 503, "message": "操作过于频繁"},
                },
            )
        ),
        call_logger=record,
        request_interval=0,
        rate_limit_retries=0,
    )

    with pytest.raises(ServiceIntegrationError, match="操作过于频繁"):
        await client.post_data("/api/claw/query", {"query": "半导体"})

    assert calls[0].status == "failed"
    assert calls[0].error_message == "MX request failed: 操作过于频繁"
    await client.aclose()


@pytest.mark.asyncio
async def test_mx_http_retries_transient_rate_limit_and_returns_success() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                200,
                json={"code": 503, "message": "请求频率过高"},
            )
        return httpx.Response(200, json={"code": 200, "data": {"answer": "ok"}})

    client = MxHttpTransport(
        api_key="test-mx-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(handler),
        request_interval=0,
        rate_limit_backoff=0,
        rate_limit_retries=1,
    )

    assert await client.post_data("/api/claw/query", {"query": "半导体"}) == {
        "answer": "ok"
    }
    assert calls == 2
    await client.aclose()


@pytest.mark.asyncio
async def test_mx_http_does_not_retry_daily_quota_exhaustion() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        nonlocal calls
        calls += 1
        return httpx.Response(
            429,
            json={
                "code": 429,
                "message": "您的调用次数已达到上限，账户已经进入休眠状态",
            },
        )

    client = MxHttpTransport(
        api_key="test-mx-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(handler),
        request_interval=0,
        rate_limit_backoff=0,
        rate_limit_retries=2,
    )

    with pytest.raises(ServiceIntegrationError, match="休眠状态"):
        await client.post_data("/api/claw/query", {"query": "半导体"})
    assert calls == 1
    await client.aclose()


@pytest.mark.asyncio
async def test_mx_http_records_cancelled_error_before_reraising() -> None:
    calls: list[StockApiCall] = []

    async def record(call: StockApiCall) -> None:
        calls.append(call)

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        raise asyncio.CancelledError

    client = MxHttpTransport(
        api_key="test-mx-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(handler),
        call_logger=record,
        request_interval=0,
    )

    with pytest.raises(asyncio.CancelledError):
        await client.post_data("/api/claw/query", {"query": "半导体"})

    assert calls[0].status == "failed"
    assert calls[0].error_message == "request cancelled"
    await client.aclose()


@pytest.mark.asyncio
async def test_mx_http_records_task_cancellation_during_rate_limit_backoff() -> None:
    call_logs: list[StockApiCall] = []
    response_seen = asyncio.Event()

    async def record(call: StockApiCall) -> None:
        call_logs.append(call)

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        response_seen.set()
        return httpx.Response(200, json={"code": 503, "message": "请求频率过高"})

    client = MxHttpTransport(
        api_key="test-mx-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(handler),
        call_logger=record,
        request_interval=0,
        rate_limit_backoff=10,
        rate_limit_retries=1,
    )
    task = asyncio.create_task(client.post_data("/api/claw/query", {"query": "半导体"}))

    await response_seen.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(call_logs) == 1
    assert call_logs[0].error_message == "request cancelled"
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "endpoint",
    ["/api/claw/mockTrading/trade", "/api/claw/mockTrading/cancel"],
)
async def test_mx_http_does_not_retry_write_operations(endpoint: str) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        nonlocal calls
        calls += 1
        return httpx.Response(429, json={"message": "请求频率过高"})

    client = MxHttpTransport(
        api_key="test-mx-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(handler),
        request_interval=0,
        rate_limit_backoff=0,
        rate_limit_retries=2,
    )

    with pytest.raises(ServiceIntegrationError) as raised:
        await client.post_envelope(endpoint, {})

    assert raised.value.status_code == 429
    assert calls == 1
    await client.aclose()


@pytest.mark.asyncio
async def test_mx_http_preserves_final_rate_limit_detail_and_logs_attempts() -> None:
    calls: list[StockApiCall] = []

    async def record(call: StockApiCall) -> None:
        calls.append(call)

    client = MxHttpTransport(
        api_key="test-mx-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                429,
                headers={"Retry-After": "4"},
                json={"message": "请求频率过高，apikey=test-mx-key"},
            )
        ),
        call_logger=record,
        request_interval=0,
        rate_limit_backoff=0,
        rate_limit_retries=1,
    )

    with pytest.raises(ServiceIntegrationError) as raised:
        await client.post_data("/api/claw/query", {"query": "半导体"})

    assert raised.value.status_code == 429
    assert raised.value.error_code.value == "rate_limit"
    assert raised.value.retry_after == "4"
    assert "请求频率过高" in str(raised.value)
    assert "test-mx-key" not in str(raised.value)
    assert len(calls) == 2
    assert [call.status_code for call in calls] == [429, 429]
    assert all("test-mx-key" not in (call.error_message or "") for call in calls)
    await client.aclose()


@pytest.mark.asyncio
async def test_mx_http_logs_each_attempt_before_returning_success() -> None:
    upstream_calls = 0
    call_logs: list[StockApiCall] = []

    async def record(call: StockApiCall) -> None:
        call_logs.append(call)

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        nonlocal upstream_calls
        upstream_calls += 1
        if upstream_calls == 1:
            return httpx.Response(200, json={"code": 503, "message": "请求频率过高"})
        return httpx.Response(200, json={"code": 200, "data": {"answer": "ok"}})

    client = MxHttpTransport(
        api_key="test-mx-key",
        base_url="https://example.test",
        transport=httpx.MockTransport(handler),
        call_logger=record,
        request_interval=0,
        rate_limit_backoff=0,
        rate_limit_retries=1,
    )

    assert await client.post_data("/api/claw/query", {"query": "半导体"}) == {
        "answer": "ok"
    }
    assert [call.status for call in call_logs] == ["failed", "success"]
    assert call_logs[0].error_message == "MX request failed: 请求频率过高"
    await client.aclose()
