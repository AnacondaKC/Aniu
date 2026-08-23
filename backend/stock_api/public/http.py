"""Rate-limited, observable transport used only by fixed public adapters."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import TypeVar

import httpx

from backend.business.shared.stock_api_source import (
    current_stock_api_source,
    normalize_public_stock_operation_id,
)
from backend.stock_api.models import (
    StockApiCall,
    StockApiCallLogger,
    StockApiErrorCategory,
    emit_stock_api_call_log,
)
from backend.stock_api.public.cancellation import (
    CancellationToken as AbortSignal,
)
from backend.stock_api.public.cancellation import (
    await_with_cancellation as await_with_abort,
)
from backend.stock_api.public.cancellation import (
    throw_if_cancelled as throw_if_aborted,
)
from backend.stock_api.public.contracts import ProviderName
from backend.stock_api.public.errors import UpstreamUnavailable

_T = TypeVar("_T")
_RETRYABLE_STATUSES = frozenset({429, 502, 503, 504})
_MAX_RESPONSE_BYTES = 8 * 1024 * 1024


def _public_log_error_message(error: UpstreamUnavailable) -> str:
    message = str(error)
    if "超时" in message:
        return "公开数据源请求超时。"
    if "无法解析" in message or "必填字段" in message:
        return "公开数据源响应无效。"
    return "公开数据源请求失败。"


@dataclass(frozen=True, slots=True)
class PublicHttpRequest:
    provider: ProviderName
    operation: str
    endpoint: str
    url: str
    parameters: Mapping[str, object]
    headers: Mapping[str, str]
    encoding: str = "utf-8"
    lane: str | None = None
    fallback_urls: tuple[str, ...] = ()
    method: str = "GET"
    body: bytes | str | None = None


@dataclass(slots=True)
class ProviderRequestGate:
    maximum_concurrency: int
    minimum_start_interval: float
    _semaphore: asyncio.Semaphore = field(init=False)
    _schedule_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _next_start: float = field(default=0.0, init=False)

    def __post_init__(self) -> None:
        self._semaphore = asyncio.Semaphore(self.maximum_concurrency)

    async def run(
        self,
        operation: Callable[[], Awaitable[_T]],
        cancellation_token: AbortSignal | None,
    ) -> _T:
        await await_with_abort(self._semaphore.acquire(), cancellation_token)
        try:
            async with self._schedule_lock:
                now = time.monotonic()
                scheduled = max(now, self._next_start)
                self._next_start = scheduled + self.minimum_start_interval
            delay = scheduled - time.monotonic()
            if delay > 0:
                await await_with_abort(asyncio.sleep(delay), cancellation_token)
            return await operation()
        finally:
            self._semaphore.release()


class PublicHttpTransport:
    """HTTP boundary with per-provider gates and one same-source retry."""

    def __init__(
        self,
        *,
        http_client: httpx.AsyncClient | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        call_logger: StockApiCallLogger | None = None,
        max_response_bytes: int = _MAX_RESPONSE_BYTES,
    ) -> None:
        self._client = http_client
        self._transport = transport
        self._owns_client = http_client is None
        self._call_logger = call_logger
        self._max_response_bytes = max_response_bytes
        self._gates = {
            "eastmoney": ProviderRequestGate(10, 0),
            "eastmoney_f10": ProviderRequestGate(10, 0),
            "eastmoney_push": ProviderRequestGate(2, 0.5),
            "tencent": ProviderRequestGate(4, 0.1),
            "sina": ProviderRequestGate(2, 0.3),
        }

    async def request_text(
        self,
        request: PublicHttpRequest,
        *,
        parser: Callable[[str], _T] | None = None,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None = None,
    ) -> str | _T:
        return await self.request(
            request,
            parser=parser or (lambda value: value),
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def request_json(
        self,
        request: PublicHttpRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None = None,
    ) -> object:
        def parse(value: str) -> object:
            parsed = json.loads(value.lstrip("\ufeff"))
            if not isinstance(parsed, (dict, list)):
                raise ValueError("JSON root must be an object or array")
            return parsed

        return await self.request(
            request,
            parser=parse,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def request(
        self,
        request: PublicHttpRequest,
        *,
        parser: Callable[[str], _T],
        timeout_seconds: float,
        cancellation_token: AbortSignal | None = None,
    ) -> _T:
        """Execute only an adapter-built request and parse its bounded body."""

        throw_if_aborted(cancellation_token)
        urls = (request.url, *request.fallback_urls)
        attempts = urls if len(urls) > 1 else (request.url, request.url)
        last_error: UpstreamUnavailable | None = None
        for index, url in enumerate(attempts[:2]):
            try:
                return await self._request_once(
                    request,
                    url=url,
                    parser=parser,
                    timeout_seconds=timeout_seconds,
                    cancellation_token=cancellation_token,
                )
            except UpstreamUnavailable as exc:
                if not exc.retryable:
                    raise
                last_error = exc
                if index == 0 and len(attempts) > 1:
                    continue
        if last_error is not None:
            raise last_error
        raise AssertionError("public request did not have a URL")

    async def _request_once(
        self,
        request: PublicHttpRequest,
        *,
        url: str,
        parser: Callable[[str], _T],
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> _T:
        started = time.perf_counter()
        status = "failed"
        status_code: int | None = None
        response_size_bytes: int | None = None
        response_characters: int | None = None
        error_message: str | None = None
        error_category: StockApiErrorCategory | None = None
        lane = request.lane or request.provider
        gate = self._gates[lane]
        try:

            async def send() -> httpx.Response:
                try:
                    async with asyncio.timeout(timeout_seconds):
                        return await await_with_abort(
                            self._ensure_client().request(
                                request.method,
                                url,
                                headers=dict(request.headers),
                                content=request.body,
                            ),
                            cancellation_token,
                        )
                except (TimeoutError, httpx.TimeoutException) as exc:
                    raise UpstreamUnavailable(
                        "公开数据源请求超时。",
                        error_category="timeout",
                    ) from exc

            response = await gate.run(send, cancellation_token)
            status_code = response.status_code
            body = response.content
            response_size_bytes = len(body)
            if response_size_bytes > self._max_response_bytes:
                raise UpstreamUnavailable(
                    "上游响应超过大小限制。",
                    retryable=False,
                    error_category="invalid_response",
                )
            try:
                text = body.decode(request.encoding)
            except UnicodeDecodeError as exc:
                raise UpstreamUnavailable(
                    "上游响应编码无效。",
                    retryable=False,
                    error_category="invalid_response",
                ) from exc
            response_characters = len(text)
            if not 200 <= response.status_code < 300:
                retryable = response.status_code in _RETRYABLE_STATUSES
                raise UpstreamUnavailable(
                    f"公开数据源请求失败：HTTP {response.status_code}",
                    retryable=retryable,
                    error_category=(
                        "rate_limited"
                        if response.status_code == 429
                        else "upstream_http"
                    ),
                )
            try:
                result = parser(text)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise UpstreamUnavailable(
                    "上游响应无法解析。",
                    retryable=False,
                    error_category="invalid_response",
                ) from exc
            status = "success"
            return result
        except asyncio.CancelledError:
            error_category = "cancelled"
            error_message = "request cancelled"
            raise
        except UpstreamUnavailable as exc:
            error_category = exc.error_category or "unknown"
            error_message = _public_log_error_message(exc)
            raise
        except (httpx.HTTPError, OSError) as exc:
            error_category = "network"
            error_message = "公开数据源网络请求失败。"
            raise UpstreamUnavailable(
                "公开数据源网络请求失败。", error_category="network"
            ) from exc
        finally:
            await emit_stock_api_call_log(
                self._call_logger,
                StockApiCall(
                    provider=request.provider,
                    operation_id=normalize_public_stock_operation_id(request.operation),
                    endpoint=request.endpoint,
                    method=request.method,
                    parameters={},
                    status=status,
                    status_code=status_code,
                    duration_ms=round((time.perf_counter() - started) * 1000),
                    response_size_bytes=response_size_bytes,
                    response_characters=response_characters,
                    error_category=error_category,
                    error_message=error_message,
                    source=current_stock_api_source(),
                    interface_name=None,
                    interface_identifier=None,
                ),
            )

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                follow_redirects=False,
                transport=self._transport,
            )
        return self._client

    async def aclose(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None


__all__ = ["PublicHttpRequest", "PublicHttpTransport", "ProviderRequestGate"]
