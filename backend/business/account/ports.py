"""Account feature ports."""

from __future__ import annotations

from datetime import date, datetime
from typing import Protocol

from backend.business.account.models import (
    AccountCacheState,
    AccountSnapshot,
    PortfolioOrderSnapshot,
    PositionSnapshot,
)


class PortfolioQueryPort(Protocol):
    async def get_account_snapshot(self) -> AccountSnapshot: ...

    async def get_positions(self) -> list[PositionSnapshot]: ...

    async def get_orders(self) -> list[PortfolioOrderSnapshot]: ...


class TradingCalendarPort(Protocol):
    def last_trading_day_on_or_before(self, day: date) -> date: ...


class AccountCacheRepositoryPort(Protocol):
    async def get_state(self) -> AccountCacheState | None: ...

    async def get_account_snapshot(self) -> AccountSnapshot | None: ...

    async def list_positions(self) -> list[PositionSnapshot]: ...

    async def list_orders(self, *, limit: int) -> list[PortfolioOrderSnapshot]: ...

    async def count_filled_orders_on_date(self, day: date) -> int: ...

    async def replace_cache(
        self,
        snapshot: AccountSnapshot,
        positions: list[PositionSnapshot],
        orders: list[PortfolioOrderSnapshot],
        *,
        attempted_at: datetime,
    ) -> AccountCacheState: ...

    async def record_refresh_failure(
        self,
        *,
        attempted_at: datetime,
        error_message: str,
    ) -> AccountCacheState: ...
