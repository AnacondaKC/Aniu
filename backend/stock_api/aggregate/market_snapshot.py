"""Seven-index market snapshot assembled from normalized public data sources."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import cast

from backend.stock_api import json_safe
from backend.stock_api.public import (
    IndexKlineRequest,
    IndexQuoteRequest,
    MarketReportsRequest,
    PublicStockRequest,
    StockMarketDataService,
)
from backend.stock_api.public.cancellation import CancellationToken as AbortSignal
from backend.stock_api.public.cancellation import (
    throw_if_cancelled as throw_if_aborted,
)

MARKET_REPORT_LIST_LIMIT = 15
MARKET_INDEX_KLINE_LIMIT = 5
MARKET_READ_CONCURRENCY = 8

MAJOR_INDICES: tuple[tuple[str, str], ...] = (
    ("000001.SH", "上证指数"),
    ("399001.SZ", "深证成指"),
    ("399006.SZ", "创业板指"),
    ("000688.SH", "科创50"),
    ("000300.SH", "沪深300"),
    ("000905.SH", "中证500"),
    ("000852.SH", "中证1000"),
)


async def _with_concurrency[T](
    semaphore: asyncio.Semaphore, operation: Awaitable[T]
) -> T:
    acquired = False
    try:
        async with semaphore:
            acquired = True
            return await operation
    finally:
        if not acquired and inspect.iscoroutine(operation):
            operation.close()


async def _collect_sections(
    sections: dict[str, Awaitable[object]],
    *,
    abort_signal: AbortSignal | None,
) -> tuple[dict[str, object], dict[str, str]]:
    throw_if_aborted(abort_signal)
    names = tuple(sections)
    outcomes = await asyncio.gather(*sections.values(), return_exceptions=True)
    values: dict[str, object] = {}
    errors: dict[str, str] = {}
    for name, outcome in zip(names, outcomes, strict=True):
        if isinstance(outcome, asyncio.CancelledError):
            raise outcome
        if isinstance(outcome, BaseException):
            throw_if_aborted(abort_signal)
            errors[name] = (str(outcome).strip() or type(outcome).__name__)[:500]
        else:
            values[name] = outcome
    return values, errors


def _data(result: object) -> dict[str, object]:
    if not isinstance(result, dict):
        return {}
    data = result.get("data")
    return dict(data) if isinstance(data, dict) else {}


def _meta(result: object) -> dict[str, object]:
    if not isinstance(result, dict):
        return {}
    meta = result.get("meta")
    return dict(meta) if isinstance(meta, dict) else {}


def _quote_by_symbol(result: object) -> dict[str, dict[str, object]]:
    quotes = _data(result).get("quotes")
    if not isinstance(quotes, list):
        return {}
    return {
        str(quote["symbol"]): dict(quote)
        for quote in quotes
        if isinstance(quote, dict) and isinstance(quote.get("symbol"), str)
    }


def _source_details(result: object) -> dict[str, object]:
    meta = _meta(result)
    return {
        "source": meta.get("source"),
        "attempted_sources": meta.get("attempted_sources", []),
        "fallback_used": bool(meta.get("fallback_used")),
        "warnings": meta.get("warnings", []),
    }


def _bound_market_reports(data: dict[str, object]) -> dict[str, object]:
    result = dict(data)
    reports = result.get("reports")
    if isinstance(reports, list):
        result["reports"] = reports[:MARKET_REPORT_LIST_LIMIT]
        result["reports_truncated"] = len(reports) > MARKET_REPORT_LIST_LIMIT

    contents = result.get("contents")
    if isinstance(contents, list):
        result["contents"] = list(contents[:3])
        result["contents_truncated"] = len(contents) > 3
    return result


@dataclass(slots=True)
class MarketSnapshotAggregator:
    """Combine major-index quotes, daily bars, and current strategy reports."""

    public_data: StockMarketDataService
    max_concurrency: int = MARKET_READ_CONCURRENCY

    async def snapshot(
        self, *, abort_signal: AbortSignal | None = None
    ) -> dict[str, object]:
        semaphore = asyncio.Semaphore(self.max_concurrency)

        async def execute(request: PublicStockRequest) -> dict[str, object]:
            throw_if_aborted(abort_signal)
            return await self.public_data.execute(request, abort_signal)

        async def load_reports() -> dict[str, object]:
            response = await execute(
                MarketReportsRequest(
                    category="strategy",
                    days=1,
                    top=3,
                    content_max_characters=None,
                )
            )
            return _bound_market_reports(_data(response))

        sections, errors = await _collect_sections(
            {
                "index_quotes": _with_concurrency(
                    semaphore,
                    execute(
                        IndexQuoteRequest(tuple(symbol for symbol, _ in MAJOR_INDICES))
                    ),
                ),
                **{
                    symbol: _with_concurrency(
                        semaphore,
                        execute(
                            IndexKlineRequest(
                                symbol=symbol,
                                period="day",
                                limit=MARKET_INDEX_KLINE_LIMIT,
                            )
                        ),
                    )
                    for symbol, _ in MAJOR_INDICES
                },
                "strategy_reports": _with_concurrency(semaphore, load_reports()),
            },
            abort_signal=abort_signal,
        )

        quote_response = sections.get("index_quotes")
        quotes = _quote_by_symbol(quote_response)
        quote_source = _source_details(quote_response)
        indices: list[dict[str, object]] = []
        failed: list[str] = []
        for symbol, name in MAJOR_INDICES:
            quote = quotes.get(symbol)
            kline_response = sections.get(symbol)
            bars = _data(kline_response).get("bars")
            if quote is None or not isinstance(bars, list) or not bars:
                failed.append(symbol)
                continue
            indices.append(
                {
                    "symbol": symbol,
                    "name": name,
                    "market_data": {
                        "quote": quote,
                        "bars_5d": bars,
                        "sources": {
                            "quote": quote_source,
                            "bars_5d": _source_details(kline_response),
                        },
                    },
                }
            )

        strategy_reports = sections.get("strategy_reports", {})
        if not isinstance(strategy_reports, dict):
            strategy_reports = {}
        result: dict[str, object] = {
            "indices": indices,
            "strategy_reports": strategy_reports,
            "failed": failed,
            "errors": errors,
        }
        return cast(dict[str, object], json_safe(result))


__all__ = [
    "MAJOR_INDICES",
    "MARKET_INDEX_KLINE_LIMIT",
    "MarketSnapshotAggregator",
]
