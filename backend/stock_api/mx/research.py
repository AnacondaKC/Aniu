"""MX research, news-search, and stock-screening interfaces."""

from __future__ import annotations

from dataclasses import dataclass

from backend.stock_api.mx.http import MxHttpTransport


@dataclass(slots=True)
class MxResearchClient:
    transport: MxHttpTransport

    async def query_market_data(self, query: str) -> dict[str, object]:
        text = query.strip()
        if not text:
            raise ValueError("query must not be empty")
        return await self.transport.post_envelope(
            "/api/claw/query",
            {"toolQuery": text},
        )

    async def search_news(self, query: str) -> dict[str, object]:
        text = query.strip()
        if not text:
            raise ValueError("query must not be empty")
        return await self.transport.post_envelope(
            "/api/claw/news-search",
            {"query": text},
        )

    async def select_stocks(self, keyword: str) -> dict[str, object]:
        text = keyword.strip()
        if not text:
            raise ValueError("keyword must not be empty")
        return await self.transport.post_envelope(
            "/api/claw/stock-screen",
            {"keyword": text},
        )


__all__ = ["MxResearchClient"]
