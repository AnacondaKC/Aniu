"""Process-scoped MX client composition."""

from __future__ import annotations

from dataclasses import dataclass

import httpx

from backend.stock_api.models import StockApiCallLogger
from backend.stock_api.mx import (
    MxApiKeyResolver,
    MxHttpTransport,
    MxMoniClient,
    MxPaperTradingClient,
    MxReadCache,
    MxRequestGate,
    MxResearchClient,
)


@dataclass(slots=True)
class MxClients:
    """Own the concrete clients for the sole MX upstream."""

    transport: MxHttpTransport
    research: MxResearchClient
    portfolio: MxMoniClient
    trading: MxPaperTradingClient

    @classmethod
    def create(
        cls,
        *,
        api_key_resolver: MxApiKeyResolver,
        http_client: httpx.AsyncClient | None = None,
        call_logger: StockApiCallLogger | None = None,
    ) -> MxClients:
        request_gate = MxRequestGate()
        read_cache = MxReadCache()
        transport = MxHttpTransport(
            api_key_resolver=api_key_resolver,
            http_client=http_client,
            call_logger=call_logger,
            request_gate=request_gate,
        )
        portfolio = MxMoniClient(
            api_key_resolver=api_key_resolver,
            http_client=http_client,
            call_logger=call_logger,
            request_gate=request_gate,
            read_cache=read_cache,
        )
        return cls(
            transport=transport,
            research=MxResearchClient(transport),
            portfolio=portfolio,
            trading=MxPaperTradingClient(transport, read_cache),
        )

    async def aclose(self) -> None:
        await self.transport.aclose()
        await self.portfolio.aclose()


__all__ = ["MxClients"]
