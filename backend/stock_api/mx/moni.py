"""MX moni client for account/positions/orders queries."""

from __future__ import annotations

import asyncio
import math
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

import httpx

from backend.business.account import (
    AccountSnapshot,
    PortfolioOrderSnapshot,
    PositionSnapshot,
)
from backend.business.shared import ServiceConfigurationError, ServiceIntegrationError
from backend.business.shared.stock_api_source import current_stock_api_source
from backend.stock_api.models import (
    StockApiCall,
    StockApiCallLogger,
    active_exception_message,
    emit_stock_api_call_log,
    json_safe,
)
from backend.stock_api.mx.cache import MxReadCache
from backend.stock_api.mx.retry import (
    MxRequestGate,
    is_mx_daily_quota_message,
    is_mx_rate_limit_message,
    mx_http_error_detail,
    mx_redact_text,
    mx_retry_delay,
)


def _duration_ms(started: float, finished_at: float | None = None) -> int:
    ended = finished_at if finished_at is not None else time.perf_counter()
    return max(0, round((ended - started) * 1000))


def _as_float(value: Any, default: float = 0.0) -> float:
    if value in {None, "", "--"}:
        return default
    if isinstance(value, str):
        normalized = value.replace(",", "").replace("%", "").strip()
        if not normalized:
            return default
        return float(normalized)
    return float(value)


def _as_int(value: Any, default: int = 0) -> int:
    if value in {None, "", "--"}:
        return default
    if isinstance(value, str):
        normalized = value.replace(",", "").strip()
        if not normalized:
            return default
        return int(float(normalized))
    return int(value)


def _pick(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in data and data[key] not in {None, ""}:
            return data[key]
    return default


def _normalize_list_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("items", "list", "rows", "data", "records", "posList", "orders"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _parse_datetime(value: Any) -> datetime | None:
    if value in {None, "", "--"}:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    text = str(value).strip().replace("Z", "+00:00")
    if text.isdigit():
        timestamp = float(text)
        if len(text) == 13:
            timestamp /= 1000
        return datetime.fromtimestamp(timestamp, tz=UTC)
    for candidate in (text, text.replace("/", "-")):
        try:
            parsed = datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed
        except ValueError:
            continue
    return None


def _parse_date(value: Any) -> date | None:
    if value in {None, "", "--"}:
        return None
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        text = f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _as_scaled_float(
    value: Any,
    decimals: Any,
    *,
    default: float = 0.0,
) -> float:
    scaled = _as_float(value, default=default)
    digits = _as_int(decimals, default=0)
    if digits <= 0:
        return float(scaled)
    return float(scaled / (10**digits))


def _currency_unit(
    data: dict[str, Any],
    *,
    default: float | None = None,
) -> float:
    raw_unit = _pick(data, "currencyUnit", default=default)
    if raw_unit in {None, "", "--"}:
        raise ServiceIntegrationError("MX response is missing currencyUnit")
    try:
        unit = _as_float(raw_unit)
    except (TypeError, ValueError) as exc:
        raise ServiceIntegrationError(
            "MX currencyUnit must be a positive number"
        ) from exc
    if not math.isfinite(unit) or unit <= 0:
        raise ServiceIntegrationError("MX currencyUnit must be a positive number")
    return unit


def _as_currency_float(value: Any, currency_unit: float) -> float:
    return _as_float(value) / currency_unit


def _normalize_direction(value: Any) -> str:
    normalized = str(value).strip().upper()
    mapping = {
        "1": "BUY",
        "2": "SELL",
        "BUY": "BUY",
        "SELL": "SELL",
    }
    return mapping.get(normalized, normalized or "UNKNOWN")


def _normalize_order_status(value: Any) -> str:
    normalized = str(value).strip().upper()
    mapping = {
        "1": "PENDING",
        "2": "PENDING",
        "3": "PARTIAL",
        "4": "FILLED",
        "5": "PARTIAL_PENDING_CANCEL",
        "6": "PENDING_CANCEL",
        "7": "PARTIAL_CANCELLED",
        "8": "CANCELLED",
        "9": "REJECTED",
        "10": "CANCEL_FAILED",
        "100": "PENDING",
        "200": "FILLED",
        "203": "CANCELLED",
        "204": "CANCELLED",
        "206": "CANCELLED",
        "PENDING": "PENDING",
        "FILLED": "FILLED",
        "PARTIAL": "PARTIAL",
        "PARTIAL_PENDING_CANCEL": "PARTIAL_PENDING_CANCEL",
        "PENDING_CANCEL": "PENDING_CANCEL",
        "PARTIAL_CANCELLED": "PARTIAL_CANCELLED",
        "CANCELLED": "CANCELLED",
        "CANCELED": "CANCELLED",
        "REJECTED": "REJECTED",
        "CANCEL_FAILED": "CANCEL_FAILED",
        "UNKNOWN": "UNKNOWN",
    }
    return mapping.get(normalized, "UNKNOWN")


def _normalize_order_price(row: dict[str, Any]) -> float | None:
    raw_price = _pick(row, "price", "orderPrice", "entrustPrice")
    if raw_price in {None, "", "--"}:
        return None
    return _as_scaled_float(raw_price, _pick(row, "priceDec", default=0))


def _normalize_filled_price(row: dict[str, Any]) -> float | None:
    if _as_int(_pick(row, "filledQuantity", "dealQty", "tradeCount", default=0)) <= 0:
        return None
    raw_price = _pick(row, "filledPrice", "dealPrice", "tradePrice")
    if raw_price in {None, "", "--"}:
        return None
    return _as_scaled_float(raw_price, _pick(row, "priceDec", default=0))


@dataclass(slots=True)
class MxMoniClient:
    """Live portfolio query client backed by MX moni HTTP endpoints."""

    api_key: str | None = None
    api_key_resolver: Callable[[], Awaitable[str | None]] | None = None
    base_url: str | None = None
    timeout: float = 15.0
    transport: httpx.AsyncBaseTransport | None = None
    http_client: httpx.AsyncClient | None = None
    call_logger: StockApiCallLogger | None = None
    request_interval: float = 0.25
    rate_limit_retries: int = 2
    rate_limit_backoff: float = 0.5
    request_gate: MxRequestGate | None = None
    read_cache: MxReadCache = field(default_factory=MxReadCache)
    _client: httpx.AsyncClient | None = field(init=False, default=None)
    _owns_client: bool = field(init=False, default=False)
    _request_gate: MxRequestGate = field(init=False, repr=False)
    _cache_api_key: str | None = field(init=False, default=None, repr=False)
    _cache_api_key_initialized: bool = field(init=False, default=False, repr=False)

    def __post_init__(self) -> None:
        self.api_key = self.api_key or os.environ.get("MX_APIKEY")
        self.base_url = self.base_url or os.environ.get(
            "MX_API_URL",
            "https://mkapi2.dfcfs.com/finskillshub",
        )
        self._client = self.http_client
        self._owns_client = False
        self._request_gate = self.request_gate or MxRequestGate(
            min_interval=max(0.0, self.request_interval)
        )

    def _ensure_client(self) -> httpx.AsyncClient:
        if self.base_url is None:
            raise ServiceConfigurationError("MX_API_URL is not configured")
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

    def clear_read_cache(self) -> None:
        self.read_cache.clear()

    async def get_account_snapshot(self) -> AccountSnapshot:
        await self._invalidate_cache_for_changed_api_key()
        return await self.read_cache.get_or_load(
            "account_snapshot", self._load_account_snapshot
        )

    async def _load_account_snapshot(self) -> AccountSnapshot:

        payload = await self._post("/api/claw/mockTrading/balance", {"moneyUnit": 1})
        data = payload if isinstance(payload, dict) else {}
        currency_unit = _currency_unit(data)
        total_asset = _as_currency_float(
            _pick(data, "totalAssets", "totalAsset"), currency_unit
        )
        initial_capital = _as_currency_float(_pick(data, "initMoney"), currency_unit)
        total_profit_value = _pick(data, "totalProfit", "accumulatedProfit")
        total_profit = (
            _as_currency_float(total_profit_value, currency_unit)
            if total_profit_value not in {None, "", "--"}
            else (total_asset - initial_capital if initial_capital > 0 else 0)
        )
        position_ratio_value = _pick(data, "totalPosPct")
        position_ratio = (
            _as_float(position_ratio_value) / 100
            if position_ratio_value not in {None, "", "--"}
            else (
                _as_currency_float(
                    _pick(data, "totalPosValue", "marketValue", "holdMarketValue"),
                    currency_unit,
                )
                / total_asset
                if total_asset > 0
                else None
            )
        )
        net_value_value = _pick(data, "nav")
        return AccountSnapshot(
            total_asset=total_asset,
            available_cash=_as_currency_float(
                _pick(data, "availBalance", "availableCash"), currency_unit
            ),
            frozen_cash=_as_currency_float(
                _pick(data, "frozenBalance", "frozenCash", "frozenMoney"),
                currency_unit,
            ),
            market_value=_as_currency_float(
                _pick(data, "marketValue", "holdMarketValue", "totalPosValue"),
                currency_unit,
            ),
            total_profit=total_profit,
            daily_profit=_as_currency_float(
                _pick(data, "dailyProfit", "todayProfit", default=0),
                currency_unit,
            ),
            operation_days=_as_int(_pick(data, "oprDays"), default=0),
            open_date=_parse_date(_pick(data, "openDate")),
            initial_capital=(initial_capital if initial_capital > 0 else None),
            net_value=(
                _as_float(net_value_value)
                if net_value_value not in {None, "", "--"}
                else (total_asset / initial_capital if initial_capital > 0 else None)
            ),
            position_ratio=position_ratio,
        )

    async def get_positions(self) -> list[PositionSnapshot]:
        await self._invalidate_cache_for_changed_api_key()
        return await self.read_cache.get_or_load("positions", self._load_positions)

    async def _load_positions(self) -> list[PositionSnapshot]:

        payload = await self._post("/api/claw/mockTrading/positions", {"moneyUnit": 1})
        rows = _normalize_list_payload(payload)
        if not rows:
            return []
        payload_data = payload if isinstance(payload, dict) else {}
        currency_unit = (
            _currency_unit(payload_data) if payload_data else _currency_unit(rows[0])
        )
        positions: list[PositionSnapshot] = []
        for row in rows:
            day_profit_value = _pick(row, "dayProfit")
            positions.append(
                PositionSnapshot(
                    symbol=str(_pick(row, "stockCode", "code", "secCode")),
                    stock_name=str(
                        _pick(
                            row,
                            "stockName",
                            "name",
                            "secName",
                            default="未知标的",
                        )
                    ),
                    quantity=_as_int(
                        _pick(
                            row,
                            "quantity",
                            "holdAmount",
                            "stockQty",
                            "count",
                            default=0,
                        )
                    ),
                    avg_cost=_as_scaled_float(
                        _pick(row, "avgCost", "costPrice", "cost", default=0),
                        _pick(row, "costPriceDec", default=0),
                        default=0,
                    ),
                    current_price=_as_scaled_float(
                        _pick(
                            row,
                            "currentPrice",
                            "newPrice",
                            "lastPrice",
                            "price",
                            default=0,
                        ),
                        _pick(row, "priceDec", default=0),
                        default=0,
                    ),
                    market_value=_as_currency_float(
                        _pick(
                            row,
                            "marketValue",
                            "stockMarketValue",
                            "value",
                            default=0,
                        ),
                        _currency_unit(row, default=currency_unit),
                    ),
                    profit_ratio=(
                        _as_float(_pick(row, "profitPct", default=0)) / 100
                        if row.get("profitPct") not in {None, "", "--"}
                        and row.get("profitRatio") in {None, "", "--"}
                        and row.get("profitRate") in {None, "", "--"}
                        else _as_float(
                            _pick(row, "profitRatio", "profitRate", default=0)
                        )
                    ),
                    day_profit=(
                        _as_currency_float(
                            day_profit_value,
                            _currency_unit(row, default=currency_unit),
                        )
                        if day_profit_value not in {None, "", "--"}
                        else None
                    ),
                    available_quantity=(
                        _as_int(
                            _pick(
                                row,
                                "availableQuantity",
                                "availableQty",
                                "sellableQuantity",
                                "enableAmount",
                                "enableQty",
                                "availableStockQty",
                            )
                        )
                        if _pick(
                            row,
                            "availableQuantity",
                            "availableQty",
                            "sellableQuantity",
                            "enableAmount",
                            "enableQty",
                            "availableStockQty",
                        )
                        not in {None, "", "--"}
                        else None
                    ),
                )
            )
        return positions

    async def get_orders(self) -> list[PortfolioOrderSnapshot]:
        await self._invalidate_cache_for_changed_api_key()
        return await self.read_cache.get_or_load("orders", self._load_orders)

    async def _load_orders(self) -> list[PortfolioOrderSnapshot]:

        payload = await self._post(
            "/api/claw/mockTrading/orders",
            {"fltOrderDrt": 0, "fltOrderStatus": 0},
        )
        rows = _normalize_list_payload(payload)
        return [
            PortfolioOrderSnapshot(
                order_id=str(_pick(row, "orderId", "entrustId", "id")),
                symbol=str(_pick(row, "stockCode", "code", "secCode")),
                stock_name=str(
                    _pick(
                        row,
                        "stockName",
                        "name",
                        "secName",
                        default="未知标的",
                    )
                ),
                direction=str(
                    _normalize_direction(
                        _pick(
                            row,
                            "direction",
                            "orderDrt",
                            "tradeType",
                            "drt",
                            default="UNKNOWN",
                        )
                    )
                ),
                quantity=_as_int(
                    _pick(row, "quantity", "orderQty", "entrustQty", "count", default=0)
                ),
                order_price=_normalize_order_price(row),
                status=str(
                    _normalize_order_status(
                        _pick(
                            row,
                            "status",
                            "orderStatus",
                            "statusName",
                            "dbStatus",
                            default="UNKNOWN",
                        )
                    )
                ),
                filled_quantity=_as_int(
                    _pick(row, "filledQuantity", "dealQty", "tradeCount", default=0)
                ),
                filled_price=_normalize_filled_price(row),
                submitted_at=_parse_datetime(
                    _pick(row, "submittedAt", "orderTime", "entrustTime", "time")
                ),
                updated_at=_parse_datetime(
                    _pick(row, "updatedAt", "updateTime", "lastUpdateTime", "time")
                ),
            )
            for row in rows
        ]

    async def _invalidate_cache_for_changed_api_key(self) -> None:
        if not self._cache_api_key_initialized:
            return
        self._set_cache_api_key(await self._resolve_api_key())

    def _set_cache_api_key(self, api_key: str | None) -> None:
        if self._cache_api_key_initialized and api_key != self._cache_api_key:
            self.read_cache.clear()
        self._cache_api_key = api_key
        self._cache_api_key_initialized = True

    async def _resolve_api_key(self) -> str | None:
        if self.api_key_resolver is not None:
            resolved = await self.api_key_resolver()
            return resolved.strip() if resolved and resolved.strip() else None
        return self.api_key.strip() if self.api_key and self.api_key.strip() else None

    async def _post(self, endpoint: str, body: dict[str, object]) -> Any:
        operation_id = _MX_MONI_OPERATION_BY_ENDPOINT.get(endpoint, endpoint)
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
            self._set_cache_api_key(api_key)
            if not api_key:
                raise ServiceConfigurationError("MX_APIKEY is not configured")
            if self.base_url is None:
                raise ServiceConfigurationError("MX_API_URL is not configured")

            headers = {"apikey": api_key, "Content-Type": "application/json"}
            request_url = f"{self.base_url.rstrip('/')}{endpoint}"
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
                            request_url, json=body, headers=headers
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
                                retry_count < self.rate_limit_retries
                                and not is_mx_daily_quota_message(response.text)
                            ):
                                retry_count += 1
                                retry_after = response.headers.get("Retry-After")
                                error_message = (
                                    f"mx-moni request failed with HTTP 429: {detail}"
                                    if detail
                                    else "mx-moni request failed with HTTP 429"
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
                                    f"mx-moni request failed with HTTP 429{suffix}",
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
                                    "mx-moni response root must be an object"
                                )
                            success = bool(result.get("success")) or str(
                                result.get("code")
                            ) in {"0", "200"}
                            if not success:
                                message = str(
                                    result.get("message")
                                    or result.get("msg")
                                    or "unknown mx-moni error"
                                )
                                safe_message = mx_redact_text(
                                    message,
                                    api_key=api_key,
                                )
                                if (
                                    is_mx_rate_limit_message(message)
                                    and retry_count < self.rate_limit_retries
                                ):
                                    retry_count += 1
                                    retry_after = response.headers.get("Retry-After")
                                    error_message = (
                                        f"mx-moni request failed: {safe_message}"
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
                                        f"mx-moni request failed: {safe_message}"
                                    )
                            else:
                                status = "success"
                                return result.get("data")
                    if should_retry:
                        await asyncio.sleep(retry_delay_seconds)
                        continue
                except (httpx.HTTPError, ValueError) as exc:
                    error_message = str(exc)
                    raise ServiceIntegrationError(
                        f"mx-moni request failed: {exc}"
                    ) from exc
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
                interface_name=_MX_MONI_INTERFACE_NAME_BY_OPERATION.get(
                    operation_id, endpoint
                ),
                interface_identifier=_MX_MONI_INTERFACE_IDENTIFIER_BY_OPERATION.get(
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


_MX_MONI_OPERATION_BY_ENDPOINT = {
    "/api/claw/mockTrading/balance": "query_portfolio.balance",
    "/api/claw/mockTrading/positions": "query_portfolio.positions",
    "/api/claw/mockTrading/orders": "query_portfolio.orders",
}

_MX_MONI_INTERFACE_NAME_BY_OPERATION = {
    "query_portfolio.balance": "模拟交易",
    "query_portfolio.positions": "模拟交易",
    "query_portfolio.orders": "模拟交易",
}

_MX_MONI_INTERFACE_IDENTIFIER_BY_OPERATION = {
    "query_portfolio.balance": "模拟交易 · 账户资金",
    "query_portfolio.positions": "模拟交易 · 持仓明细",
    "query_portfolio.orders": "模拟交易 · 委托查询",
}
