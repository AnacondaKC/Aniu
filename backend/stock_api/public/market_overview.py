"""Public-stock implementation of the market overview query."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from backend.business.market import MarketResourceError
from backend.stock_api.public.contracts import (
    IndexKlineRequest,
    IndexQuoteRequest,
    MarketBreadthRequest,
    NewsFeedRequest,
    PublicStockRequest,
    SectorRankingRequest,
    StockRankingRequest,
)
from backend.stock_api.public.service import StockMarketDataService

INDEXES = (
    ("sse", "000001.SH", "上证指数"),
    ("szse", "399001.SZ", "深证成指"),
    ("chinext", "399006.SZ", "创业板指"),
    ("star50", "000680.SH", "科创综指"),
)
INDEX_TREND_POINT_LIMIT = 300

RANKINGS = (
    ("gainers", StockRankingRequest(sort="change_percent", order="desc", limit=10)),
    ("losers", StockRankingRequest(sort="change_percent", order="asc", limit=10)),
    ("net_inflow", StockRankingRequest(sort="net_inflow", order="desc", limit=5)),
    ("net_outflow", StockRankingRequest(sort="net_inflow", order="asc", limit=5)),
)


class PublicMarketOverviewQuery:
    """Compose dashboard resources through the shared public-data service."""

    def __init__(self, public_data: StockMarketDataService) -> None:
        self._public_data = public_data

    async def get_market_indices(self) -> dict[str, object]:
        index_request = IndexQuoteRequest(tuple(symbol for _, symbol, _ in INDEXES))
        index_result, trend_results = await asyncio.gather(
            self._safe_execute("index", None, index_request),
            asyncio.gather(
                *(
                    self._safe_execute(
                        "trend",
                        index_id,
                        IndexKlineRequest(
                            symbol, period="1m", limit=INDEX_TREND_POINT_LIMIT
                        ),
                    )
                    for index_id, symbol, _ in INDEXES
                )
            ),
        )
        errors: list[MarketResourceError] = []
        indices = self._index_quotes(index_result, errors)
        trends = self._index_trends(trend_results, indices, errors)
        return {
            "generated_at": datetime.now(tz=UTC),
            "indices": indices,
            "trends": trends,
            "errors": [error.as_dict() for error in errors],
        }

    async def get_market_details(self) -> dict[str, object]:
        index_request = IndexQuoteRequest(tuple(symbol for _, symbol, _ in INDEXES))
        (
            index_result,
            breadth_result,
            ranking_results,
            hotspot_results,
            news_results,
        ) = await asyncio.gather(
            self._safe_execute("index", None, index_request),
            self._safe_execute("breadth", None, MarketBreadthRequest()),
            asyncio.gather(
                *(
                    self._safe_execute("ranking", label, request)
                    for label, request in RANKINGS
                )
            ),
            asyncio.gather(
                self._safe_execute(
                    "hotspot",
                    "industry",
                    SectorRankingRequest(
                        sector_type="industry",
                        sort="change_percent",
                        order="desc",
                        limit=5,
                    ),
                ),
                self._safe_execute(
                    "hotspot",
                    "concept",
                    SectorRankingRequest(
                        sector_type="concept",
                        sort="change_percent",
                        order="desc",
                        limit=5,
                    ),
                ),
            ),
            asyncio.gather(
                self._safe_execute(
                    "news", "headlines", NewsFeedRequest(feed="headlines", limit=10)
                ),
                self._safe_execute(
                    "news", "flash", NewsFeedRequest(feed="flash", limit=10)
                ),
            ),
        )
        details = self._details_payload(
            index_result=index_result,
            breadth_result=breadth_result,
            ranking_results=ranking_results,
            hotspot_results=hotspot_results,
            news_results=news_results,
        )
        details["generated_at"] = datetime.now(tz=UTC)
        return details

    async def get_market_overview(self) -> dict[str, object]:
        index_request = IndexQuoteRequest(tuple(symbol for _, symbol, _ in INDEXES))
        (
            index_result,
            breadth_result,
            trend_results,
            ranking_results,
            hotspot_results,
            news_results,
        ) = await asyncio.gather(
            self._safe_execute("index", None, index_request),
            self._safe_execute("breadth", None, MarketBreadthRequest()),
            asyncio.gather(
                *(
                    self._safe_execute(
                        "trend",
                        index_id,
                        IndexKlineRequest(
                            symbol, period="1m", limit=INDEX_TREND_POINT_LIMIT
                        ),
                    )
                    for index_id, symbol, _ in INDEXES
                )
            ),
            asyncio.gather(
                *(
                    self._safe_execute("ranking", label, request)
                    for label, request in RANKINGS
                )
            ),
            asyncio.gather(
                self._safe_execute(
                    "hotspot",
                    "industry",
                    SectorRankingRequest(
                        sector_type="industry",
                        sort="change_percent",
                        order="desc",
                        limit=5,
                    ),
                ),
                self._safe_execute(
                    "hotspot",
                    "concept",
                    SectorRankingRequest(
                        sector_type="concept",
                        sort="change_percent",
                        order="desc",
                        limit=5,
                    ),
                ),
            ),
            asyncio.gather(
                self._safe_execute(
                    "news", "headlines", NewsFeedRequest(feed="headlines", limit=10)
                ),
                self._safe_execute(
                    "news", "flash", NewsFeedRequest(feed="flash", limit=10)
                ),
            ),
        )
        errors: list[MarketResourceError] = []
        details = self._details_payload(
            index_result=index_result,
            breadth_result=breadth_result,
            ranking_results=ranking_results,
            hotspot_results=hotspot_results,
            news_results=news_results,
            errors=errors,
        )
        indices = self._index_quotes(index_result, errors)
        trends = self._index_trends(trend_results, indices, errors)
        return {
            "generated_at": datetime.now(tz=UTC),
            "indices": indices,
            "trends": trends,
            **details,
            "errors": [error.as_dict() for error in errors],
        }

    async def _safe_execute(
        self, resource: str, item_id: str | None, request: PublicStockRequest
    ) -> dict[str, Any]:
        try:
            return await self._public_data.execute(request)
        except Exception as exc:  # upstream failures are independent page resources
            return {"_market_error": MarketResourceError(resource, item_id, str(exc))}

    def _details_payload(
        self,
        *,
        index_result: dict[str, Any],
        breadth_result: dict[str, Any],
        ranking_results: tuple[dict[str, Any], ...],
        hotspot_results: tuple[dict[str, Any], ...],
        news_results: tuple[dict[str, Any], ...],
        errors: list[MarketResourceError] | None = None,
    ) -> dict[str, object]:
        collected = errors if errors is not None else []
        indices = self._index_quotes(index_result, collected)
        rankings = {
            label: self._items(result, collected)[:5]
            for (label, _), result in zip(RANKINGS, ranking_results, strict=True)
        }
        hotspots = {
            label: self._items(result, collected)
            for label, result in zip(
                ("industry", "concept"), hotspot_results, strict=True
            )
        }
        amounts = [
            quote.get("amount")
            for quote in indices
            if quote.get("symbol") in {"000001.SH", "399001.SZ"}
        ]
        numeric_amounts = [
            value for value in amounts if isinstance(value, (int, float))
        ]
        return {
            "turnover": {
                "today_amount": sum(numeric_amounts) if numeric_amounts else None
            },
            "breadth": self._breadth(breadth_result, collected),
            "rankings": rankings,
            "hotspots": hotspots,
            "headlines": self._items(news_results[0], collected),
            "flash_news": self._items(news_results[1], collected),
            "errors": [error.as_dict() for error in collected],
        }

    @staticmethod
    def _error(result: dict[str, Any], errors: list[MarketResourceError]) -> bool:
        error = result.get("_market_error")
        if isinstance(error, MarketResourceError):
            errors.append(error)
            return True
        return False

    @classmethod
    def _breadth(
        cls, result: dict[str, Any], errors: list[MarketResourceError]
    ) -> dict[str, int] | None:
        if cls._error(result, errors):
            return None
        data = result.get("data")
        if not isinstance(data, dict):
            return None
        values = tuple(data.get(key) for key in ("rising", "falling", "flat"))
        if not all(type(value) is int and value >= 0 for value in values):
            return None
        rising, falling, flat = values
        assert type(rising) is int and type(falling) is int and type(flat) is int
        return {"rising": rising, "falling": falling, "flat": flat}

    @classmethod
    def _index_quotes(
        cls, result: dict[str, Any], errors: list[MarketResourceError]
    ) -> list[dict[str, Any]]:
        if cls._error(result, errors):
            return []
        data = result.get("data")
        if not isinstance(data, dict):
            return []
        quotes = data.get("quotes")
        if not isinstance(quotes, list):
            return []
        names = {symbol: (index_id, name) for index_id, symbol, name in INDEXES}
        normalized: list[dict[str, Any]] = []
        for quote in quotes:
            if not isinstance(quote, dict):
                continue
            symbol = quote.get("symbol")
            if not isinstance(symbol, str) or symbol not in names:
                continue
            index_id, name = names[symbol]
            normalized.append({"id": index_id, "name": name, **quote})
        return normalized

    @classmethod
    def _index_trends(
        cls,
        results: tuple[dict[str, Any], ...],
        indices: list[dict[str, Any]],
        errors: list[MarketResourceError],
    ) -> list[dict[str, Any]]:
        previous_close = {
            quote.get("id"): quote.get("previous_close") for quote in indices
        }
        trends: list[dict[str, Any]] = []
        for (index_id, _, name), result in zip(INDEXES, results, strict=True):
            if cls._error(result, errors):
                continue
            data = result.get("data")
            bars = data.get("bars") if isinstance(data, dict) else None
            if not isinstance(bars, list):
                continue
            points = [
                {
                    "time": bar.get("time"),
                    "price": bar.get("close"),
                    "average_price": None,
                    "cumulative_amount": bar.get("amount")
                    if isinstance(bar.get("amount"), (int, float))
                    else None,
                }
                for bar in bars
                if isinstance(bar, dict)
                and isinstance(bar.get("time"), str)
                and isinstance(bar.get("close"), (int, float))
            ]
            trends.append(
                {
                    "id": index_id,
                    "name": name,
                    "previous_close": previous_close.get(index_id),
                    "points": points,
                }
            )
        return trends

    @classmethod
    def _items(
        cls, result: dict[str, Any], errors: list[MarketResourceError]
    ) -> list[dict[str, Any]]:
        if cls._error(result, errors):
            return []
        data = result.get("data")
        items = data.get("items") if isinstance(data, dict) else None
        return (
            [item for item in items if isinstance(item, dict)]
            if isinstance(items, list)
            else []
        )


__all__ = ["PublicMarketOverviewQuery"]
