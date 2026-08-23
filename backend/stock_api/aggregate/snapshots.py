"""Bounded aggregate research views assembled from current data sources."""

from __future__ import annotations

import asyncio
import copy
import inspect
import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import cast

from backend.business.account import PositionSnapshot
from backend.stock_api import MxMoniClient, MxResearchClient, json_safe
from backend.stock_api.public import (
    FinancialsRequest,
    ForecastRequest,
    IndustryComparisonRequest,
    InvalidStockRequest,
    KlineRequest,
    NewsFeedRequest,
    OperatingIndicatorsRequest,
    PublicStockRequest,
    QuoteSnapshotRequest,
    RatingsRequest,
    SectorMoneyFlowRequest,
    ShareholdersRequest,
    StockMarketDataService,
    StockMoneyFlowHistoryRequest,
    StockNewsRequest,
    StockReportsRequest,
    ValuationRequest,
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
from backend.stock_api.public.contracts import normalize_symbol

READ_CONCURRENCY = 4
PORTFOLIO_PAGE_SIZE = 8
INDUSTRY_MONEY_FLOW_LIMIT = 10
TOP_NEWS_LIMIT = 20
STOCK_ANALYSIS_MAX_RESULT_CHARACTERS = 32_000
_STOCK_ANALYSIS_FINAL_PREVIEW_CHARACTERS = 10_000

_ETF_SYMBOL_PATTERN = re.compile(r"^(?:15\d|5\d{2})\d{3}$")


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
    """Collect independent sections without allowing one failure to hide others."""

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
            errors[name] = _error_message(outcome)
        else:
            values[name] = outcome
    return values, errors


def _error_message(error: BaseException) -> str:
    return (str(error).strip() or type(error).__name__)[:500]


def _public_data(result: object) -> dict[str, object]:
    if not isinstance(result, dict):
        return {}
    data = result.get("data")
    return dict(data) if isinstance(data, dict) else {}


def _quote_for_symbol(data: dict[str, object], symbol: str) -> dict[str, object]:
    quotes = data.get("quotes")
    if not isinstance(quotes, list):
        return {}
    for item in quotes:
        if isinstance(item, dict) and item.get("symbol") == symbol:
            return dict(item)
    return {}


def _holding(position: PositionSnapshot) -> dict[str, object]:
    return {
        "symbol": position.symbol,
        "stock_name": position.stock_name,
        "quantity": position.quantity,
        "available_quantity": position.available_quantity,
        "avg_cost": position.avg_cost,
        "current_price": position.current_price,
        "market_value": position.market_value,
        "profit_ratio": position.profit_ratio,
        "day_profit": position.day_profit,
        "captured_at": position.captured_at.isoformat(),
    }


def _is_etf(symbol: str) -> bool:
    return bool(_ETF_SYMBOL_PATTERN.fullmatch(symbol))


def _list_value(data: object, key: str) -> list[object]:
    if not isinstance(data, dict):
        return []
    value = data.get(key)
    return list(value) if isinstance(value, list) else []


def _bounded_list(data: object, key: str, limit: int) -> tuple[list[object], bool]:
    items = _list_value(data, key)
    return items[:limit], len(items) > limit


def _serialized_length(value: object) -> int:
    return len(
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
    )


def _preview_value(value: object) -> object:
    """Keep useful records when one aggregate section exceeds its output budget."""

    safe = json_safe(value)
    if isinstance(safe, list):
        return {
            "items": [_preview_value(item) for item in safe[:3]],
            "total_items": len(safe),
            "truncated": len(safe) > 3,
        }
    if not isinstance(safe, dict):
        return str(safe)[:320]

    preview: dict[str, object] = {}
    for key, item in safe.items():
        if key in {"items", "bars", "quotes", "reports"} and isinstance(item, list):
            preview[key] = [_preview_value(row) for row in item[:3]]
            preview[f"{key}_total"] = len(item)
            preview[f"{key}_truncated"] = len(item) > 3
        elif isinstance(item, str):
            preview[key] = item[:320] + ("…" if len(item) > 320 else "")
        elif isinstance(item, (bool, int, float)) or item is None:
            preview[key] = item
        elif isinstance(item, dict):
            preview[key] = _preview_value(item)
    preview["section_truncated"] = True
    return preview


def _bound_stock_analysis(result: dict[str, object]) -> dict[str, object]:
    """Retain the historical 32k model-facing safety boundary for single-stock views."""

    safe = json_safe(result)
    if not isinstance(safe, dict):
        return {"result": safe}
    candidate = cast(dict[str, object], copy.deepcopy(safe))
    original_characters = _serialized_length(candidate)
    if original_characters <= STOCK_ANALYSIS_MAX_RESULT_CHARACTERS:
        return candidate

    compacted_sections: list[str] = []
    for section in (
        "stock_news",
        "stock_reports",
        "quarterly_financials",
        "shareholder_counts",
        "industry_comparison",
        "supplementary_indicators",
        "institutional_forecasts",
        "rating_statistics",
        "fund_flow_10d",
        "bars_10d",
    ):
        if section not in candidate:
            continue
        candidate[section] = _preview_value(candidate[section])
        compacted_sections.append(section)
        if _serialized_length(candidate) <= STOCK_ANALYSIS_MAX_RESULT_CHARACTERS:
            break

    candidate["safety_boundary"] = {
        "output_truncated": True,
        "maximum_characters": STOCK_ANALYSIS_MAX_RESULT_CHARACTERS,
        "original_characters": original_characters,
        "compacted_sections": compacted_sections,
    }
    if _serialized_length(candidate) <= STOCK_ANALYSIS_MAX_RESULT_CHARACTERS:
        return candidate

    fallback = {
        "symbol": candidate.get("symbol"),
        "quote": _preview_value(candidate.get("quote")),
        "bars_10d": _preview_value(candidate.get("bars_10d")),
        "financial_summary": _preview_value(candidate.get("financial_summary")),
        "errors": candidate.get("errors", {}),
        "safety_boundary": {
            "output_truncated": True,
            "maximum_characters": STOCK_ANALYSIS_MAX_RESULT_CHARACTERS,
            "original_characters": original_characters,
            "compacted_sections": compacted_sections,
        },
    }
    if _serialized_length(fallback) <= STOCK_ANALYSIS_MAX_RESULT_CHARACTERS:
        return fallback

    serialized_candidate = json.dumps(
        candidate,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )
    result_preview = serialized_candidate[:_STOCK_ANALYSIS_FINAL_PREVIEW_CHARACTERS]
    if len(serialized_candidate) > _STOCK_ANALYSIS_FINAL_PREVIEW_CHARACTERS:
        result_preview += "…"
    # The 10k preview leaves room for JSON escaping when this result is serialized.
    return {
        "symbol": str(candidate.get("symbol") or "")[:64],
        "result_preview": result_preview,
        "safety_boundary": {
            "output_truncated": True,
            "maximum_characters": STOCK_ANALYSIS_MAX_RESULT_CHARACTERS,
            "original_characters": original_characters,
            "compacted_sections": compacted_sections,
        },
    }


@dataclass(slots=True)
class _AggregateReader:
    public_data: StockMarketDataService

    async def _execute_public(
        self,
        request: PublicStockRequest,
        abort_signal: AbortSignal | None,
    ) -> dict[str, object]:
        throw_if_aborted(abort_signal)
        return _public_data(await self.public_data.execute(request, abort_signal))


@dataclass(slots=True)
class _MxAggregateReader(_AggregateReader):
    research: MxResearchClient

    async def _execute_mx(
        self,
        operation: Callable[[], Awaitable[object]],
        abort_signal: AbortSignal | None,
    ) -> object:
        throw_if_aborted(abort_signal)
        return await await_with_abort(operation(), abort_signal)


@dataclass(slots=True)
class PortfolioStockSnapshotAggregator(_MxAggregateReader):
    """Page through portfolio holdings and enrich supported positions concurrently."""

    portfolio: MxMoniClient
    page_size: int = PORTFOLIO_PAGE_SIZE
    max_concurrency: int = READ_CONCURRENCY

    async def snapshot(
        self,
        page: int = 1,
        *,
        abort_signal: AbortSignal | None = None,
    ) -> dict[str, object]:
        if isinstance(page, bool) or not isinstance(page, int) or page < 1:
            raise InvalidStockRequest("page 必须是大于 0 的整数")
        throw_if_aborted(abort_signal)
        raw_positions = await self._execute_mx(
            lambda: self.portfolio.get_positions(), abort_signal
        )
        if not isinstance(raw_positions, list) or not all(
            isinstance(position, PositionSnapshot) for position in raw_positions
        ):
            raise TypeError("模拟组合持仓响应格式无效")
        all_positions = cast(list[PositionSnapshot], raw_positions)
        supported_positions: list[tuple[PositionSnapshot, str]] = []
        for position in all_positions:
            try:
                normalize_symbol(position.symbol)
            except InvalidStockRequest:
                if _is_etf(position.symbol):
                    supported_positions.append((position, "etf"))
            else:
                supported_positions.append((position, "a_share"))
        supported_positions.sort(
            key=lambda item: (-item[0].market_value, item[0].symbol)
        )
        start = (page - 1) * self.page_size
        selected = supported_positions[start : start + self.page_size]
        end = start + len(selected)

        a_share_symbols = [
            normalize_symbol(position.symbol)
            for position, instrument_type in selected
            if instrument_type == "a_share"
        ]

        quotes_by_symbol: dict[str, dict[str, object]] = {}
        quote_error: str | None = None
        if a_share_symbols:
            try:
                quote_data = await self._execute_public(
                    QuoteSnapshotRequest(tuple(a_share_symbols), detail="full"),
                    abort_signal,
                )
                quotes_by_symbol = {
                    str(item.get("symbol")): dict(item)
                    for item in _list_value(quote_data, "quotes")
                    if isinstance(item, dict) and isinstance(item.get("symbol"), str)
                }
            except asyncio.CancelledError:
                raise
            except BaseException as error:
                throw_if_aborted(abort_signal)
                quote_error = _error_message(error)

        semaphore = asyncio.Semaphore(self.max_concurrency)
        enrichments = await asyncio.gather(
            *(
                self._enrich_position(
                    position,
                    instrument_type,
                    quotes_by_symbol,
                    quote_error,
                    semaphore,
                    abort_signal,
                )
                for position, instrument_type in selected
            )
        )
        total_a_share_positions = sum(
            instrument_type == "a_share"
            for _position, instrument_type in supported_positions
        )
        result: dict[str, object] = {
            "page": page,
            "page_size": self.page_size,
            "total_positions": len(supported_positions),
            "total_a_share_positions": total_a_share_positions,
            "total_etf_positions": len(supported_positions) - total_a_share_positions,
            "excluded_unsupported_positions": len(all_positions)
            - len(supported_positions),
            "returned_positions": len(enrichments),
            "next_page": page + 1 if end < len(supported_positions) else None,
            "positions": enrichments,
        }
        return cast(dict[str, object], json_safe(result))

    async def _enrich_position(
        self,
        position: PositionSnapshot,
        instrument_type: str,
        quotes_by_symbol: dict[str, dict[str, object]],
        quote_error: str | None,
        semaphore: asyncio.Semaphore,
        abort_signal: AbortSignal | None,
    ) -> dict[str, object]:
        if instrument_type == "etf":
            return await self._enrich_etf(position, semaphore, abort_signal)
        symbol = normalize_symbol(position.symbol)

        sections, errors = await _collect_sections(
            {
                "bars_5d": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        KlineRequest(
                            symbol=symbol,
                            period="day",
                            adjust="qfq",
                            limit=5,
                        ),
                        abort_signal,
                    ),
                ),
                "fund_flow_5d": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        StockMoneyFlowHistoryRequest(symbol=symbol, page=1, limit=5),
                        abort_signal,
                    ),
                ),
                "financial_summary": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        FinancialsRequest(symbol=symbol, mode="latest"),
                        abort_signal,
                    ),
                ),
                "stock_news": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        StockNewsRequest(symbol=symbol, page=1, limit=10),
                        abort_signal,
                    ),
                ),
            },
            abort_signal=abort_signal,
        )
        quote = quotes_by_symbol.get(symbol)
        if quote is None and quote_error is not None:
            errors["quote"] = quote_error
        elif quote is None:
            errors["quote"] = "实时行情未返回该持仓。"
        return {
            "instrument_type": "a_share",
            "holding": _holding(position),
            "quote": quote,
            "bars_5d": sections.get("bars_5d", {}),
            "fund_flow_5d": sections.get("fund_flow_5d", {}),
            "financial_summary": sections.get("financial_summary", {}),
            "stock_news": sections.get("stock_news", {}),
            "errors": errors,
        }

    async def _enrich_etf(
        self,
        position: PositionSnapshot,
        semaphore: asyncio.Semaphore,
        abort_signal: AbortSignal | None,
    ) -> dict[str, object]:
        target = f"{position.stock_name}（{position.symbol}）ETF"
        sections, errors = await _collect_sections(
            {
                "quote": _with_concurrency(
                    semaphore,
                    self._execute_mx(
                        lambda: self.research.query_market_data(
                            f"查询{target}当前实时行情。"
                        ),
                        abort_signal,
                    ),
                ),
                "stock_news": _with_concurrency(
                    semaphore,
                    self._execute_mx(
                        lambda: self.research.search_news(
                            f"{position.stock_name} ETF 最新资讯"
                        ),
                        abort_signal,
                    ),
                ),
            },
            abort_signal=abort_signal,
        )
        return {
            "instrument_type": "etf",
            "holding": _holding(position),
            "quote": {"data_source": "mx", "result": sections.get("quote")},
            "stock_news": {
                "data_source": "mx",
                "result": sections.get("stock_news"),
            },
            "bars_5d": [],
            "fund_flow_5d": [],
            "financial_summary": None,
            "unsupported_sections": [
                "bars_5d",
                "fund_flow_5d",
                "financial_summary",
            ],
            "errors": errors,
        }


@dataclass(slots=True)
class StockAnalysisAggregator(_AggregateReader):
    """Single-stock research view using every equivalent current public contract."""

    max_concurrency: int = READ_CONCURRENCY

    async def snapshot(
        self,
        symbol: str,
        *,
        abort_signal: AbortSignal | None = None,
    ) -> dict[str, object]:
        normalized_symbol = normalize_symbol(symbol)
        semaphore = asyncio.Semaphore(self.max_concurrency)

        async def quote() -> dict[str, object]:
            data = await self._execute_public(
                QuoteSnapshotRequest((normalized_symbol,), detail="full"),
                abort_signal,
            )
            return _quote_for_symbol(data, normalized_symbol)

        sections, errors = await _collect_sections(
            {
                "quote": _with_concurrency(semaphore, quote()),
                "bars_10d": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        KlineRequest(
                            symbol=normalized_symbol,
                            period="day",
                            adjust="qfq",
                            limit=10,
                        ),
                        abort_signal,
                    ),
                ),
                "fund_flow_10d": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        StockMoneyFlowHistoryRequest(
                            symbol=normalized_symbol,
                            page=1,
                            limit=10,
                        ),
                        abort_signal,
                    ),
                ),
                "financial_summary": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        FinancialsRequest(symbol=normalized_symbol, mode="latest"),
                        abort_signal,
                    ),
                ),
                "quarterly_financials": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        FinancialsRequest(
                            symbol=normalized_symbol,
                            mode="quarterly",
                            page=1,
                            limit=8,
                        ),
                        abort_signal,
                    ),
                ),
                "shareholder_counts": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        ShareholdersRequest(symbol=normalized_symbol, page=1, limit=8),
                        abort_signal,
                    ),
                ),
                "valuation_percentiles": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        ValuationRequest(symbol=normalized_symbol),
                        abort_signal,
                    ),
                ),
                "industry_comparison": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        IndustryComparisonRequest(
                            symbol=normalized_symbol,
                            page=1,
                            limit=20,
                        ),
                        abort_signal,
                    ),
                ),
                "supplementary_indicators": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        OperatingIndicatorsRequest(
                            symbol=normalized_symbol,
                            page=1,
                            limit=20,
                        ),
                        abort_signal,
                    ),
                ),
                "stock_reports": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        StockReportsRequest(
                            symbol=normalized_symbol,
                            page=1,
                            limit=3,
                            content="summary",
                            summary_max_characters=None,
                        ),
                        abort_signal,
                    ),
                ),
                "institutional_forecasts": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        ForecastRequest(
                            symbol=normalized_symbol,
                            mode="institutions",
                            page=1,
                            limit=5,
                        ),
                        abort_signal,
                    ),
                ),
                "rating_statistics": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        RatingsRequest(symbol=normalized_symbol),
                        abort_signal,
                    ),
                ),
                "stock_news": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        StockNewsRequest(symbol=normalized_symbol, page=1, limit=20),
                        abort_signal,
                    ),
                ),
            },
            abort_signal=abort_signal,
        )
        result: dict[str, object] = {
            "symbol": normalized_symbol,
            "quote": sections.get("quote", {}),
            "bars_10d": sections.get("bars_10d", {}),
            "fund_flow_10d": sections.get("fund_flow_10d", {}),
            "financial_summary": sections.get("financial_summary", {}),
            "quarterly_financials": sections.get("quarterly_financials", {}),
            "shareholder_counts": sections.get("shareholder_counts", {}),
            "valuation_percentiles": sections.get("valuation_percentiles", {}),
            "industry_comparison": sections.get("industry_comparison", {}),
            "supplementary_indicators": sections.get("supplementary_indicators", {}),
            "stock_reports": sections.get("stock_reports", {}),
            "institutional_forecasts": sections.get("institutional_forecasts", {}),
            "rating_statistics": sections.get("rating_statistics", {}),
            "stock_news": sections.get("stock_news", {}),
            "errors": errors,
        }
        return _bound_stock_analysis(result)


@dataclass(slots=True)
class IndustrySnapshotAggregator(_AggregateReader):
    """Current industry and concept heat."""

    max_concurrency: int = READ_CONCURRENCY

    async def snapshot(
        self, *, abort_signal: AbortSignal | None = None
    ) -> dict[str, object]:
        semaphore = asyncio.Semaphore(self.max_concurrency)
        sections, errors = await _collect_sections(
            {
                "industry_money_flow": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        SectorMoneyFlowRequest(
                            sector_type="industry",
                            page=1,
                            limit=INDUSTRY_MONEY_FLOW_LIMIT + 1,
                        ),
                        abort_signal,
                    ),
                ),
                "concept_money_flow": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        SectorMoneyFlowRequest(
                            sector_type="concept",
                            page=1,
                            limit=INDUSTRY_MONEY_FLOW_LIMIT + 1,
                        ),
                        abort_signal,
                    ),
                ),
                "top_news": _with_concurrency(
                    semaphore,
                    self._execute_public(
                        NewsFeedRequest(
                            feed="headlines",
                            page=1,
                            limit=TOP_NEWS_LIMIT + 1,
                        ),
                        abort_signal,
                    ),
                ),
            },
            abort_signal=abort_signal,
        )
        industry_flow = sections.get("industry_money_flow", {})
        concept_flow = sections.get("concept_money_flow", {})
        news = sections.get("top_news", {})
        industry_money_flow, industry_money_flow_truncated = _bounded_list(
            industry_flow,
            "items",
            INDUSTRY_MONEY_FLOW_LIMIT,
        )
        concept_money_flow, concept_money_flow_truncated = _bounded_list(
            concept_flow,
            "items",
            INDUSTRY_MONEY_FLOW_LIMIT,
        )
        top_news, top_news_truncated = _bounded_list(news, "items", TOP_NEWS_LIMIT)
        result: dict[str, object] = {
            "industries": {
                "money_flow": industry_money_flow,
                "money_flow_limit": INDUSTRY_MONEY_FLOW_LIMIT,
                "money_flow_truncated": industry_money_flow_truncated,
            },
            "concepts": {
                "money_flow": concept_money_flow,
                "money_flow_limit": INDUSTRY_MONEY_FLOW_LIMIT,
                "money_flow_truncated": concept_money_flow_truncated,
            },
            "top_news": top_news,
            "top_news_limit": TOP_NEWS_LIMIT,
            "top_news_truncated": top_news_truncated,
            "errors": errors,
        }
        return cast(dict[str, object], json_safe(result))
