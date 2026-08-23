"""Facade for the seven normalized public stock-data domains."""

from __future__ import annotations

import copy
import json

import httpx

from backend.stock_api.models import StockApiCallLogger
from backend.stock_api.public.cache import PublicStockDataCache
from backend.stock_api.public.cancellation import CancellationToken as AbortSignal
from backend.stock_api.public.contracts import (
    ConnectMoneyFlowRequest,
    IndexKlineRequest,
    KlineRequest,
    ProviderName,
    PublicStockRequest,
)
from backend.stock_api.public.errors import UpstreamUnavailable
from backend.stock_api.public.http import PublicHttpTransport
from backend.stock_api.public.normalizers.common import sampled
from backend.stock_api.public.providers.eastmoney import EastMoneyAdapter
from backend.stock_api.public.providers.sina import SinaAdapter
from backend.stock_api.public.providers.tencent import TencentAdapter
from backend.stock_api.public.router import PublicProviderAdapters, PublicStockRouter


class StockMarketDataService:
    """Public business facade; callers cannot supply upstream endpoint details."""

    def __init__(
        self,
        router: PublicStockRouter,
        *,
        cache: PublicStockDataCache | None = None,
        transport: PublicHttpTransport | None = None,
    ) -> None:
        self._router = router
        self._cache = cache or PublicStockDataCache()
        self._transport = transport

    @classmethod
    def create(
        cls,
        *,
        http_client: httpx.AsyncClient | None = None,
        call_logger: StockApiCallLogger | None = None,
    ) -> StockMarketDataService:
        transport = PublicHttpTransport(
            http_client=http_client,
            call_logger=call_logger,
        )
        router = PublicStockRouter(
            PublicProviderAdapters(
                eastmoney=EastMoneyAdapter(transport),
                tencent=TencentAdapter(transport),
                sina=SinaAdapter(transport),
            )
        )
        return cls(router, transport=transport)

    async def execute(
        self,
        request: PublicStockRequest,
        cancellation_token: AbortSignal | None = None,
    ) -> dict[str, object]:
        """Route automatically and cache the complete normalized result."""

        key = _cache_key(request.cache_payload())
        return await self._cache.get_or_load(
            key,
            lambda: self._router.execute(
                request,
                cancellation_token=cancellation_token,
            ),
            _ttl_seconds(request),
        )

    async def execute_diagnostic(
        self,
        request: PublicStockRequest,
        provider: ProviderName,
        cancellation_token: AbortSignal | None = None,
    ) -> dict[str, object]:
        """Run one fixed source for diagnostics; never expose this to Agent tools."""

        return await self._router.execute(
            request,
            cancellation_token=cancellation_token,
            diagnostic_provider=provider,
        )

    async def aclose(self) -> None:
        if self._transport is not None:
            await self._transport.aclose()


def _cache_key(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _ttl_seconds(request: PublicStockRequest) -> float:
    operation = request.operation
    if operation in {
        "quote.snapshot",
        "chart.intraday",
        "index.quote",
        "market.breadth",
    }:
        return 3.0
    if isinstance(request, IndexKlineRequest):
        return 5.0 if request.period.endswith("m") else 60.0
    if isinstance(request, KlineRequest):
        return 10.0 if request.period.endswith("m") else 60.0
    if operation.startswith("ranking.") or operation.startswith("money_flow."):
        return 10.0
    if operation.startswith("fundamentals."):
        return 6 * 60 * 60
    if operation.startswith("research."):
        return 30 * 60
    if operation.startswith("news."):
        return 60.0
    return 0.0


_MAX_RESULT_CHARACTERS = 64_000


def bound_agent_result(
    result: dict[str, object], request: PublicStockRequest
) -> dict[str, object]:
    """Copy and bound a complete result only at the model-facing boundary."""

    return _bound_result(result, request)


def _bound_result(
    result: dict[str, object], request: PublicStockRequest
) -> dict[str, object]:
    candidate = copy.deepcopy(result)
    if _serialized_length(candidate) <= _MAX_RESULT_CHARACTERS:
        return candidate
    data = candidate.get("data")
    meta = candidate.get("meta")
    if not isinstance(data, dict) or not isinstance(meta, dict):
        raise UpstreamUnavailable("公开数据结果超过输出大小限制。", retryable=False)
    meta["degraded"] = True
    meta["output_truncated"] = True
    warnings = meta.get("warnings")
    if not isinstance(warnings, list):
        warnings = list(warnings) if isinstance(warnings, tuple) else []
        meta["warnings"] = warnings
    warnings.append("结果已按输出大小限制收缩。")

    if isinstance(request, ConnectMoneyFlowRequest):
        _shrink_connect(candidate, data, meta)
    else:
        _shrink_text_fields(candidate, data, meta)
        if _serialized_length(candidate) <= _MAX_RESULT_CHARACTERS:
            return candidate
        time_series = isinstance(
            request, (KlineRequest, IndexKlineRequest)
        ) or request.operation in {
            "chart.intraday",
            "money_flow.stock_intraday",
            "money_flow.stock_history",
        }
        for key in ("quotes", "bars", "points", "items", "reports", "contents"):
            if isinstance(data.get(key), list):
                _shrink_list(
                    candidate,
                    data,
                    meta,
                    key,
                    time_series and key in {"bars", "points", "items"},
                )
                if _serialized_length(candidate) <= _MAX_RESULT_CHARACTERS:
                    return candidate
        _shrink_text_fields(candidate, data, meta)
    if _serialized_length(candidate) > _MAX_RESULT_CHARACTERS:
        raise UpstreamUnavailable("公开数据结果超过输出大小限制。", retryable=False)
    return candidate


def _shrink_list(
    candidate: dict[str, object],
    data: dict[str, object],
    meta: dict[str, object],
    key: str,
    sample_series: bool,
) -> None:
    values = data.get(key)
    if not isinstance(values, list) or len(values) <= 1:
        return
    original = len(values)
    while len(values) > 1 and _serialized_length(candidate) > _MAX_RESULT_CHARACTERS:
        target = max(1, len(values) // 2)
        values = sampled(values, target)[0] if sample_series else values[:target]
        data[key] = values
    meta["original_count"] = original
    meta["returned_count"] = len(values)
    if sample_series:
        data["sampled"] = True


def _shrink_connect(
    candidate: dict[str, object], data: dict[str, object], meta: dict[str, object]
) -> None:
    original_counts: dict[str, int] = {}
    returned_counts: dict[str, int] = {}
    while _serialized_length(candidate) > _MAX_RESULT_CHARACTERS:
        changed = False
        for direction in ("northbound", "southbound"):
            values = data.get(direction)
            if not isinstance(values, list):
                continue
            original_counts.setdefault(direction, len(values))
            if len(values) > 1:
                data[direction] = sampled(values, max(1, len(values) // 2))[0]
                changed = True
        if not changed:
            break
    for direction in ("northbound", "southbound"):
        values = data.get(direction)
        if isinstance(values, list):
            original_counts.setdefault(direction, len(values))
            returned_counts[direction] = len(values)
    meta["original_counts"] = original_counts
    meta["returned_counts"] = returned_counts


def _shrink_text_fields(
    candidate: dict[str, object],
    data: dict[str, object],
    meta: dict[str, object] | None = None,
) -> None:
    report = data.get("report")
    if isinstance(report, dict) and isinstance(report.get("content"), str):
        for maximum in (2_000, 1_000, 500):
            report["content"] = report["content"][:maximum]
            if _serialized_length(candidate) <= _MAX_RESULT_CHARACTERS:
                return
        return

    contents = data.get("contents")
    if isinstance(contents, list):
        original = len(contents)
        for maximum in (2_000, 1_000, 500):
            for item in contents:
                if isinstance(item, dict) and isinstance(item.get("content"), str):
                    item["content"] = item["content"][:maximum]
            if _serialized_length(candidate) <= _MAX_RESULT_CHARACTERS:
                if meta is not None and original != len(contents):
                    meta["original_count"] = original
                    meta["returned_count"] = len(contents)
                return
        reports = data.get("reports")
        report_values = reports if isinstance(reports, list) else None
        original_reports = len(report_values) if report_values is not None else 0
        while (
            len(contents) > 1 and _serialized_length(candidate) > _MAX_RESULT_CHARACTERS
        ):
            contents.pop()
            if report_values is not None and len(report_values) > 1:
                report_values.pop()
        if report_values is not None:
            while (
                len(report_values) > 1
                and _serialized_length(candidate) > _MAX_RESULT_CHARACTERS
            ):
                report_values.pop()
        if meta is not None and (
            original != len(contents) or original_reports != len(report_values or [])
        ):
            meta["original_count"] = max(original, original_reports)
            meta["returned_count"] = max(len(contents), len(report_values or []))
        if _serialized_length(candidate) <= _MAX_RESULT_CHARACTERS:
            return

    reports = data.get("reports")
    if isinstance(reports, list):
        original = len(reports)
        for maximum in (500, 200):
            for item in reports:
                if isinstance(item, dict) and isinstance(item.get("summary"), str):
                    item["summary"] = item["summary"][:maximum]
            if _serialized_length(candidate) <= _MAX_RESULT_CHARACTERS:
                if meta is not None and original != len(reports):
                    meta["original_count"] = original
                    meta["returned_count"] = len(reports)
                return
        while (
            len(reports) > 1 and _serialized_length(candidate) > _MAX_RESULT_CHARACTERS
        ):
            reports.pop()
        if meta is not None and original != len(reports):
            meta["original_count"] = original
            meta["returned_count"] = len(reports)
        if _serialized_length(candidate) <= _MAX_RESULT_CHARACTERS:
            return

    for key in ("text", "content"):
        value = data.get(key)
        if isinstance(value, str):
            for maximum in (2_000, 1_000, 500):
                data[key] = value[:maximum]
                if _serialized_length(candidate) <= _MAX_RESULT_CHARACTERS:
                    return


def _serialized_length(value: object) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")))


__all__ = ["StockMarketDataService", "bound_agent_result"]
