"""Ports and result contracts for market overview queries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class MarketResourceError:
    """One independently degradable market-overview resource failure."""

    resource: str
    item_id: str | None
    message: str

    def as_dict(self) -> dict[str, str | None]:
        return {
            "resource": self.resource,
            "item_id": self.item_id,
            "message": self.message,
        }


class MarketOverviewQueryPort(Protocol):
    async def get_market_indices(self) -> dict[str, object]: ...

    async def get_market_details(self) -> dict[str, object]: ...

    async def get_market_overview(self) -> dict[str, object]: ...


__all__ = ["MarketOverviewQueryPort", "MarketResourceError"]
