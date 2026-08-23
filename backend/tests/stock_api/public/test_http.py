from __future__ import annotations

from dataclasses import dataclass

import httpx
import pytest

from backend.stock_api.models import StockApiCall
from backend.stock_api.public.http import PublicHttpRequest, PublicHttpTransport


@dataclass
class CallLog:
    values: list[StockApiCall]

    async def __call__(self, call: StockApiCall) -> None:
        self.values.append(call)


def request(*, fallback_urls: tuple[str, ...] = ()) -> PublicHttpRequest:
    return PublicHttpRequest(
        provider="tencent",
        operation="quote.snapshot",
        endpoint="test_endpoint",
        url="https://primary.example.test/data",
        fallback_urls=fallback_urls,
        parameters={"symbol": "600519.SH"},
        headers={},
    )


def test_only_eastmoney_push_lane_has_a_start_interval() -> None:
    transport = PublicHttpTransport(
        transport=httpx.MockTransport(lambda _: httpx.Response(200))
    )

    assert transport._gates["eastmoney_push"].maximum_concurrency == 2
    assert transport._gates["eastmoney_push"].minimum_start_interval == 0.5
    assert transport._gates["eastmoney"].maximum_concurrency == 10
    assert transport._gates["eastmoney"].minimum_start_interval == 0
    assert transport._gates["eastmoney_f10"].maximum_concurrency == 10
    assert transport._gates["eastmoney_f10"].minimum_start_interval == 0


@pytest.mark.asyncio
async def test_http_retries_primary_then_first_fallback_only() -> None:
    urls: list[str] = []
    logs: list[StockApiCall] = []

    def handler(raw_request: httpx.Request) -> httpx.Response:
        urls.append(str(raw_request.url))
        if len(urls) == 1:
            return httpx.Response(503)
        return httpx.Response(200, text='{"ok":true}')

    transport = PublicHttpTransport(
        transport=httpx.MockTransport(handler), call_logger=CallLog(logs)
    )
    transport._gates["tencent"].minimum_start_interval = 0
    result = await transport.request_json(
        request(
            fallback_urls=(
                "https://fallback-one.example.test/data",
                "https://fallback-two.example.test/data",
            )
        ),
        timeout_seconds=1,
    )
    assert result == {"ok": True}
    assert urls == [
        "https://primary.example.test/data",
        "https://fallback-one.example.test/data",
    ]
    assert [call.error_category for call in logs] == ["upstream_http", None]
    assert logs[0].response_characters == 0
    assert logs[1].response_characters == len('{"ok":true}')
    await transport.aclose()


@pytest.mark.asyncio
async def test_http_parser_failure_is_invalid_response_and_not_retried() -> None:
    logs: list[StockApiCall] = []
    transport = PublicHttpTransport(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, text="ok")),
        call_logger=CallLog(logs),
    )
    transport._gates["tencent"].minimum_start_interval = 0

    with pytest.raises(Exception):
        await transport.request_text(
            request(), parser=lambda _: cast_never(), timeout_seconds=1
        )
    assert len(logs) == 1
    assert logs[0].error_category == "invalid_response"
    await transport.aclose()


def cast_never() -> str:
    raise KeyError("malformed")


@pytest.mark.asyncio
async def test_http_decoding_failure_is_invalid_response() -> None:
    logs: list[StockApiCall] = []

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=b"\xff",
            headers={"content-type": "text/plain"},
        )

    transport = PublicHttpTransport(
        transport=httpx.MockTransport(handler),
        call_logger=CallLog(logs),
    )
    transport._gates["tencent"].minimum_start_interval = 0
    with pytest.raises(Exception):
        await transport.request_json(request(), timeout_seconds=1)
    assert len(logs) == 1
    assert logs[0].error_category == "invalid_response"
    await transport.aclose()


@pytest.mark.asyncio
async def test_http_timeout_category_is_stable() -> None:
    logs: list[StockApiCall] = []

    def handler(_: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("read timeout")

    transport = PublicHttpTransport(
        transport=httpx.MockTransport(handler), call_logger=CallLog(logs)
    )
    transport._gates["tencent"].minimum_start_interval = 0
    with pytest.raises(Exception):
        await transport.request_json(request(), timeout_seconds=1)
    assert len(logs) == 2
    assert [call.error_category for call in logs] == ["timeout", "timeout"]
    await transport.aclose()
