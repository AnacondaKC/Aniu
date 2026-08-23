"""Application service for account / portfolio queries."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime

from backend.business.account import (
    AccountSnapshot,
    PortfolioOrderSnapshot,
    PositionSnapshot,
)
from backend.business.account.dto import (
    AccountDashboardDTO,
    AccountRefreshResultDTO,
    to_account_overview_dto,
    to_account_refresh_result_dto,
    to_portfolio_order_dto,
    to_position_dto,
)
from backend.business.account.ports import (
    AccountCacheRepositoryPort,
    PortfolioQueryPort,
    TradingCalendarPort,
)
from backend.business.account.runtime import AccountRefreshGate
from backend.business.shared import (
    AccountRefreshThrottledError,
    CommitterPort,
    ServiceIntegrationError,
)
from backend.business.shared.stock_api_source import (
    STOCK_API_SOURCE_OVERVIEW_REFRESH,
    stock_api_source,
)

ExecutionGuard = Callable[[], Awaitable[None]]


class AccountAppService:
    """Use-case orchestrator for portfolio read-only queries."""

    def __init__(
        self,
        portfolio_client: PortfolioQueryPort,
        account_cache_repo: AccountCacheRepositoryPort,
        committer: CommitterPort | None = None,
        trading_calendar: TradingCalendarPort | None = None,
        refresh_gate: AccountRefreshGate | None = None,
    ) -> None:
        self._portfolio_client = portfolio_client
        self._account_cache_repo = account_cache_repo
        self._refresh_gate = refresh_gate or AccountRefreshGate()
        self._committer = committer
        self._trading_calendar = trading_calendar

    async def get_account_dashboard(self) -> AccountDashboardDTO:
        snapshot = await self._ensure_cache()
        positions = await self._account_cache_repo.list_positions()
        orders = await self._account_cache_repo.list_orders(limit=20)
        performance_date = self._resolve_performance_date(snapshot.captured_at.date())
        today_trade_count = (
            0
            if performance_date is None
            else await self._account_cache_repo.count_filled_orders_on_date(
                performance_date
            )
        )
        return AccountDashboardDTO(
            overview=to_account_overview_dto(
                snapshot,
                performance_date=performance_date,
                today_trade_count=today_trade_count,
            ),
            positions=[to_position_dto(position) for position in positions],
            orders=[to_portfolio_order_dto(order) for order in orders],
        )

    async def refresh_account_cache(
        self, *, execution_guard: ExecutionGuard | None = None
    ) -> AccountRefreshResultDTO:
        if not self._refresh_gate.allow():
            raise AccountRefreshThrottledError("10 秒内只能刷新一次，请稍后重试。")
        clear_read_cache = getattr(self._portfolio_client, "clear_read_cache", None)
        if callable(clear_read_cache):
            clear_read_cache()
        now = datetime.now(tz=UTC)

        try:
            snapshot, positions, orders = await self._fetch_live_cache(now)
        except (ServiceIntegrationError, TypeError, ValueError, OverflowError) as exc:
            integration_error = (
                exc
                if isinstance(exc, ServiceIntegrationError)
                else ServiceIntegrationError(f"invalid mx-moni response: {exc}")
            )
            state = await self._account_cache_repo.record_refresh_failure(
                attempted_at=now,
                error_message=str(integration_error),
            )
            await self._commit(execution_guard=execution_guard)
            if state.captured_at is None:
                raise integration_error from exc
            return to_account_refresh_result_dto(
                state=state,
                status="stale",
                message=f"刷新失败，继续使用本地数据：{integration_error}",
            )

        state = await self._account_cache_repo.replace_cache(
            snapshot,
            positions,
            orders,
            attempted_at=now,
        )
        await self._commit(execution_guard=execution_guard)
        return to_account_refresh_result_dto(
            state=state,
            status="refreshed",
            message="已从妙想接口刷新并写入本地数据。",
        )

    async def _ensure_cache(self) -> AccountSnapshot:
        snapshot = await self._account_cache_repo.get_account_snapshot()
        if snapshot is not None:
            return snapshot

        await self.refresh_account_cache()
        snapshot = await self._account_cache_repo.get_account_snapshot()
        if snapshot is None:
            raise ServiceIntegrationError("account cache is unavailable")
        return snapshot

    async def _fetch_live_cache(
        self,
        captured_at: datetime,
    ) -> tuple[AccountSnapshot, list[PositionSnapshot], list[PortfolioOrderSnapshot]]:
        with stock_api_source(STOCK_API_SOURCE_OVERVIEW_REFRESH):
            overview, positions, orders = await asyncio.gather(
                self._portfolio_client.get_account_snapshot(),
                self._portfolio_client.get_positions(),
                self._portfolio_client.get_orders(),
            )
        # The balance endpoint does not expose a daily-profit field; the
        # positions payload reports per-position dayProfit (including positions
        # closed today), so their sum is the account's daily P&L.  Only trust
        # the sum when every position reports a value; a partial payload would
        # silently undercount, so fall back to the balance figure instead.
        position_day_profits = [
            position.day_profit
            for position in positions
            if position.day_profit is not None
        ]
        daily_profit = (
            sum(position_day_profits)
            if positions and len(position_day_profits) == len(positions)
            else overview.daily_profit
        )
        return (
            AccountSnapshot(
                total_asset=overview.total_asset,
                available_cash=overview.available_cash,
                frozen_cash=overview.frozen_cash,
                market_value=overview.market_value,
                total_profit=overview.total_profit,
                daily_profit=daily_profit,
                operation_days=overview.operation_days,
                open_date=overview.open_date,
                initial_capital=overview.initial_capital,
                net_value=overview.net_value,
                position_ratio=overview.position_ratio,
                captured_at=captured_at,
            ),
            [
                PositionSnapshot(
                    symbol=position.symbol,
                    stock_name=position.stock_name,
                    quantity=position.quantity,
                    avg_cost=position.avg_cost,
                    current_price=position.current_price,
                    market_value=position.market_value,
                    profit_ratio=position.profit_ratio,
                    day_profit=position.day_profit,
                    available_quantity=position.available_quantity,
                    captured_at=captured_at,
                )
                for position in positions
            ],
            [
                PortfolioOrderSnapshot(
                    order_id=order.order_id,
                    symbol=order.symbol,
                    stock_name=order.stock_name,
                    direction=order.direction,
                    quantity=order.quantity,
                    order_price=order.order_price,
                    filled_quantity=order.filled_quantity,
                    filled_price=order.filled_price,
                    status=order.status,
                    submitted_at=order.submitted_at,
                    updated_at=order.updated_at,
                )
                for order in orders
            ],
        )

    async def _commit(self, *, execution_guard: ExecutionGuard | None = None) -> None:
        if execution_guard is not None:
            await execution_guard()
        if self._committer is not None:
            await self._committer.commit()

    def _resolve_performance_date(self, day: date) -> date | None:
        if self._trading_calendar is None:
            return day
        return self._trading_calendar.last_trading_day_on_or_before(day)
