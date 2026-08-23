"""Shared async HTTP transport for MX StockApi interfaces."""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import httpx

from backend.business.shared import ServiceConfigurationError, ServiceIntegrationError
from backend.business.shared.stock_api_source import current_stock_api_source
from backend.stock_api.models import (
    StockApiCall,
    StockApiCallLogger,
    active_exception_message,
    emit_stock_api_call_log,
    json_safe,
)
from backend.stock_api.mx.retry import (
    MxRequestGate,
    is_mx_daily_quota_message,
    is_mx_rate_limit_message,
    mx_http_error_detail,
    mx_redact_text,
    mx_retry_delay,
)

MxApiKeyResolver = Callable[[], Awaitable[str | None]]


@dataclass(slots=True)
class MxHttpTransport:
    api_key: str | None = None
    api_key_resolver: MxApiKeyResolver | None = None
    base_url: str | None = None
    timeout: float = 15.0
    http_client: httpx.AsyncClient | None = None
    transport: httpx.AsyncBaseTransport | None = None
    call_logger: StockApiCallLogger | None = None
    request_interval: float = 0.25
    rate_limit_retries: int = 2
    rate_limit_backoff: float = 0.5
    request_gate: MxRequestGate | None = None
    _client: httpx.AsyncClient | None = field(init=False, default=None)
    _owns_client: bool = field(init=False, default=False)
    _request_gate: MxRequestGate = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.api_key = self.api_key or os.environ.get("MX_APIKEY")
        self.base_url = self.base_url or os.environ.get(
            "MX_API_URL", "https://mkapi2.dfcfs.com/finskillshub"
        )
        self._client = self.http_client
        self._request_gate = self.request_gate or MxRequestGate(
            min_interval=max(0.0, self.request_interval)
        )

    async def post_envelope(
        self,
        endpoint: str,
        body: dict[str, object],
    ) -> dict[str, Any]:
        operation_id = _MX_OPERATION_BY_ENDPOINT.get(endpoint, endpoint)
        started = time.perf_counter()
        status = "failed"
        status_code: int | None = None
        response_size_bytes: int | None = None
        response_characters: int | None = None
        error_message: str | None = None
        api_key: str | None = None
        retry_count = 0
        attempt_started = False
        try:
            api_key = await self._resolve_api_key()
            if not api_key:
                raise ServiceConfigurationError("MX_APIKEY is not configured")
            if self.base_url is None:
                raise ServiceConfigurationError("MX_API_URL is not configured")
            request_url = f"{self.base_url.rstrip('/')}{endpoint}"
            headers = {"apikey": api_key, "Content-Type": "application/json"}
            while True:
                started = time.perf_counter()
                status = "failed"
                status_code = None
                response_size_bytes = None
                response_characters = None
                error_message = None
                attempt_finished_at: float | None = None
                retry_after: str | None = None
                attempt_started = True
                try:
                    should_retry = False
                    retry_delay_seconds = 0.0
                    async with self._request_gate:
                        response = await self._ensure_client().post(
                            request_url,
                            json=body,
                            headers=headers,
                        )
                        status_code = response.status_code
                        response_size_bytes = len(response.content)
                        response_characters = len(response.text)
                        if response.status_code == 429:
                            detail = mx_http_error_detail(
                                response.text,
                                api_key=api_key,
                            )
                            if (
                                operation_id in _MX_READ_OPERATIONS
                                and retry_count < self.rate_limit_retries
                                and not is_mx_daily_quota_message(response.text)
                            ):
                                retry_count += 1
                                retry_after = response.headers.get("Retry-After")
                                error_message = (
                                    f"MX request failed with HTTP 429: {detail}"
                                    if detail
                                    else "MX request failed with HTTP 429"
                                )
                                attempt_finished_at = time.perf_counter()
                                retry_delay_seconds = mx_retry_delay(
                                    retry_count,
                                    base_delay=self.rate_limit_backoff,
                                    retry_after=retry_after,
                                )
                                self._request_gate.defer(retry_delay_seconds)
                                should_retry = True
                            else:
                                suffix = f": {detail}" if detail else ""
                                error = ServiceIntegrationError(
                                    f"MX request failed with HTTP 429{suffix}",
                                    status_code=429,
                                )
                                setattr(
                                    error,
                                    "retry_after",
                                    response.headers.get("Retry-After"),
                                )
                                raise error
                        else:
                            response.raise_for_status()
                            result = response.json()
                            if not isinstance(result, dict):
                                raise ServiceIntegrationError(
                                    "MX response root must be an object"
                                )
                            failure_message = _mx_business_error(
                                result,
                                operation_id=operation_id,
                            )
                            if failure_message is not None:
                                safe_failure_message = mx_redact_text(
                                    failure_message,
                                    api_key=api_key,
                                )
                                if (
                                    operation_id in _MX_READ_OPERATIONS
                                    and is_mx_rate_limit_message(failure_message)
                                    and retry_count < self.rate_limit_retries
                                ):
                                    retry_count += 1
                                    retry_after = response.headers.get("Retry-After")
                                    error_message = (
                                        f"MX request failed: {safe_failure_message}"
                                    )
                                    attempt_finished_at = time.perf_counter()
                                    retry_delay_seconds = mx_retry_delay(
                                        retry_count,
                                        base_delay=self.rate_limit_backoff,
                                        retry_after=retry_after,
                                    )
                                    self._request_gate.defer(retry_delay_seconds)
                                    should_retry = True
                                else:
                                    raise ServiceIntegrationError(
                                        f"MX request failed: {safe_failure_message}"
                                    )
                            else:
                                status = "success"
                                return result
                    if should_retry:
                        await asyncio.sleep(retry_delay_seconds)
                        continue
                except (httpx.HTTPError, ValueError) as exc:
                    error_message = str(exc)
                    raise ServiceIntegrationError(f"MX request failed: {exc}") from exc
                except Exception as exc:
                    error_message = str(exc)
                    raise
                finally:
                    active_error_message = active_exception_message()
                    if (
                        error_message is None
                        or active_error_message == "request cancelled"
                    ):
                        error_message = active_error_message
                    await self._log_call(
                        operation_id=operation_id,
                        endpoint=endpoint,
                        body=body,
                        started=started,
                        status=status,
                        status_code=status_code,
                        response_size_bytes=response_size_bytes,
                        response_characters=response_characters,
                        error_message=error_message,
                        finished_at=attempt_finished_at,
                    )
        finally:
            if not attempt_started:
                if error_message is None:
                    error_message = active_exception_message()
                await self._log_call(
                    operation_id=operation_id,
                    endpoint=endpoint,
                    body=body,
                    started=started,
                    status=status,
                    status_code=status_code,
                    response_size_bytes=response_size_bytes,
                    response_characters=response_characters,
                    error_message=error_message,
                    finished_at=None,
                )

    async def post_data(self, endpoint: str, body: dict[str, object]) -> Any:
        return (await self.post_envelope(endpoint, body)).get("data")

    async def _resolve_api_key(self) -> str | None:
        if self.api_key_resolver is not None:
            resolved = await self.api_key_resolver()
            return resolved.strip() if resolved and resolved.strip() else None
        return self.api_key.strip() if self.api_key and self.api_key.strip() else None

    async def _log_call(
        self,
        *,
        operation_id: str,
        endpoint: str,
        body: dict[str, object],
        started: float,
        status: str,
        status_code: int | None,
        response_size_bytes: int | None,
        response_characters: int | None,
        error_message: str | None,
        finished_at: float | None,
    ) -> None:
        await emit_stock_api_call_log(
            self.call_logger,
            StockApiCall(
                provider="mx",
                operation_id=operation_id,
                interface_name=_MX_INTERFACE_NAME_BY_OPERATION.get(
                    operation_id, endpoint
                ),
                interface_identifier=_MX_INTERFACE_IDENTIFIER_BY_OPERATION.get(
                    operation_id, operation_id
                ),
                endpoint=endpoint,
                method="POST",
                parameters=json_safe(body),
                status=status,
                status_code=status_code,
                duration_ms=_duration_ms(started, finished_at),
                response_size_bytes=response_size_bytes,
                response_characters=response_characters,
                error_message=error_message,
                source=current_stock_api_source(),
            ),
        )

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                transport=self.transport,
            )
            self._owns_client = True
        return self._client

    async def aclose(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None
            self._owns_client = False


__all__ = ["MxApiKeyResolver", "MxHttpTransport"]


_MX_OPERATION_BY_ENDPOINT = {
    "/api/claw/query": "query_market_data",
    "/api/claw/news-search": "search_news",
    "/api/claw/stock-screen": "select_stocks",
    "/api/claw/mockTrading/trade": "trade",
    "/api/claw/mockTrading/cancel": "cancel",
}


_MX_INTERFACE_NAME_BY_OPERATION = {
    "query_market_data": "金融数据查询",
    "search_news": "资讯搜索",
    "select_stocks": "智能选股",
    "trade": "模拟交易",
    "cancel": "模拟交易",
}

_MX_READ_OPERATIONS = frozenset({"query_market_data", "search_news", "select_stocks"})
_MX_DIRECT_RESPONSE_OPERATIONS = frozenset({"trade", "cancel"})
_MX_DIRECT_SUCCESS_STATUSES = frozenset(
    {"accepted", "cancelled", "ok", "submitted", "success"}
)
_MX_SUCCESS_CODES = frozenset({"0", "200"})
_MX_NESTED_SUCCESS_CODES_BY_OPERATION = {
    "query_market_data": frozenset({"0", "200"}),
    "search_news": frozenset({"0", "200"}),
    "select_stocks": frozenset({"0", "100", "200"}),
}


_MX_INTERFACE_IDENTIFIER_BY_OPERATION = {
    "query_market_data": "金融数据查询",
    "search_news": "资讯搜索",
    "select_stocks": "智能选股",
    "trade": "模拟交易 · 下单",
    "cancel": "模拟交易 · 撤销委托",
}


def _mx_business_error(
    response: dict[str, Any],
    *,
    operation_id: str,
) -> str | None:
    """Recognize MX's operation-specific response envelopes."""

    if response.get("error") not in (None, ""):
        return str(response["error"])

    code = response.get("code")
    if code is not None:
        if str(code) not in _MX_SUCCESS_CODES:
            return str(
                response.get("message") or response.get("msg") or "unknown MX error"
            )
    else:
        status = response.get("status")
        if status is not None:
            if operation_id in _MX_DIRECT_RESPONSE_OPERATIONS:
                if str(status).casefold() in _MX_DIRECT_SUCCESS_STATUSES:
                    return None
                return str(
                    response.get("message")
                    or response.get("msg")
                    or f"MX operation status: {status}"
                )
            if str(status) not in _MX_SUCCESS_CODES:
                return str(
                    response.get("message") or response.get("msg") or "unknown MX error"
                )
        elif "success" in response:
            if response.get("success") is not True:
                return str(
                    response.get("message") or response.get("msg") or "unknown MX error"
                )
        elif operation_id not in _MX_DIRECT_RESPONSE_OPERATIONS:
            return str(
                response.get("message") or response.get("msg") or "unknown MX error"
            )
        elif response.get("message") not in (None, ""):
            return str(response["message"])

    if operation_id not in _MX_READ_OPERATIONS:
        return None

    data = response.get("data")
    if not isinstance(data, dict):
        return None
    nested_code = data.get("code")
    if nested_code is None:
        return None
    success_codes = _MX_NESTED_SUCCESS_CODES_BY_OPERATION[operation_id]
    if str(nested_code) in success_codes:
        return None
    return str(data.get("message") or data.get("msg") or "unknown MX data error")


def _duration_ms(started: float, finished_at: float | None = None) -> int:
    ended = finished_at if finished_at is not None else time.perf_counter()
    return max(0, round((ended - started) * 1000))
