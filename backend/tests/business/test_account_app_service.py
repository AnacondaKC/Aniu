"""Tests for account application service."""

from __future__ import annotations

from datetime import datetime

import pytest

from backend.business.account import (
    AccountCacheState,
    AccountSnapshot,
    PortfolioOrderSnapshot,
    PositionSnapshot,
)
from backend.business.account.service import AccountAppService
from backend.business.shared import ServiceIntegrationError


class FakePortfolioClient:
    async def get_account_snapshot(self) -> AccountSnapshot:
        return AccountSnapshot(
            total_asset=100000.0,
            available_cash=20000.0,
            frozen_cash=1000.0,
            market_value=79000.0,
            total_profit=5000.0,
            daily_profit=600.0,
        )

    async def get_positions(self) -> list[PositionSnapshot]:
        return [
            PositionSnapshot(
                symbol="600519",
                stock_name="贵州茅台",
                quantity=100,
                avg_cost=1600.0,
                current_price=1700.0,
                market_value=170000.0,
                profit_ratio=0.0625,
                day_profit=300.0,
                available_quantity=75,
            )
        ]

    async def get_orders(self) -> list[PortfolioOrderSnapshot]:
        return [
            PortfolioOrderSnapshot(
                order_id="order-1",
                symbol="000001",
                stock_name="平安银行",
                direction="BUY",
                quantity=200,
                order_price=12.34,
                status="FILLED",
                filled_quantity=200,
                filled_price=12.30,
            )
        ]


class BrokenPortfolioClient(FakePortfolioClient):
    async def get_account_snapshot(self) -> AccountSnapshot:
        raise ServiceIntegrationError("mx failed")


class MalformedPortfolioClient(FakePortfolioClient):
    async def get_account_snapshot(self) -> AccountSnapshot:
        raise ValueError("invalid numeric field")


class FakeAccountCacheRepo:
    def __init__(self) -> None:
        self.state: AccountCacheState | None = None
        self.positions: list[PositionSnapshot] = []
        self.orders: list[PortfolioOrderSnapshot] = []
        self.snapshot_reads = 0
        self.requested_order_limit: int | None = None

    async def get_state(self) -> AccountCacheState | None:
        return self.state

    async def get_account_snapshot(self) -> AccountSnapshot | None:
        self.snapshot_reads += 1
        return None if self.state is None else self.state.to_snapshot()

    async def list_positions(self) -> list[PositionSnapshot]:
        return list(self.positions)

    async def list_orders(self, *, limit: int) -> list[PortfolioOrderSnapshot]:
        self.requested_order_limit = limit
        return list(self.orders[:limit])

    async def count_filled_orders_on_date(self, day) -> int:
        return sum(
            1
            for order in self.orders
            if (order.updated_at or order.submitted_at) is not None
            and (order.updated_at or order.submitted_at).date() == day
            and order.filled_quantity > 0
        )

    async def replace_cache(
        self,
        snapshot: AccountSnapshot,
        positions: list[PositionSnapshot],
        orders: list[PortfolioOrderSnapshot],
        *,
        attempted_at: datetime,
    ) -> AccountCacheState:
        self.state = AccountCacheState(
            total_asset=snapshot.total_asset,
            available_cash=snapshot.available_cash,
            frozen_cash=snapshot.frozen_cash,
            market_value=snapshot.market_value,
            total_profit=snapshot.total_profit,
            daily_profit=snapshot.daily_profit,
            operation_days=snapshot.operation_days,
            open_date=snapshot.open_date,
            initial_capital=snapshot.initial_capital,
            net_value=snapshot.net_value,
            position_ratio=snapshot.position_ratio,
            captured_at=snapshot.captured_at,
            last_refresh_attempt_at=attempted_at,
            last_refresh_succeeded_at=attempted_at,
            last_refresh_error=None,
        )
        self.positions = list(positions)
        self.orders = list(orders)
        return self.state

    async def record_refresh_failure(
        self,
        *,
        attempted_at: datetime,
        error_message: str,
    ) -> AccountCacheState:
        previous = self.state or AccountCacheState()
        self.state = AccountCacheState(
            total_asset=previous.total_asset,
            available_cash=previous.available_cash,
            frozen_cash=previous.frozen_cash,
            market_value=previous.market_value,
            total_profit=previous.total_profit,
            daily_profit=previous.daily_profit,
            operation_days=previous.operation_days,
            open_date=previous.open_date,
            initial_capital=previous.initial_capital,
            net_value=previous.net_value,
            position_ratio=previous.position_ratio,
            captured_at=previous.captured_at,
            last_refresh_attempt_at=attempted_at,
            last_refresh_succeeded_at=previous.last_refresh_succeeded_at,
            last_refresh_error=error_message,
        )
        return self.state


class FakeCommitter:
    def __init__(self) -> None:
        self.commits = 0

    async def commit(self) -> None:
        self.commits += 1


@pytest.mark.asyncio
async def test_account_app_service_maps_overview_positions_and_orders() -> None:
    repo = FakeAccountCacheRepo()
    service = AccountAppService(
        portfolio_client=FakePortfolioClient(),
        account_cache_repo=repo,
        committer=FakeCommitter(),
    )

    dashboard = await service.get_account_dashboard()

    assert repo.snapshot_reads == 2
    assert dashboard.overview.total_asset == 100000.0
    assert dashboard.overview.daily_profit == 300.0
    assert dashboard.positions[0].symbol == "600519"
    assert dashboard.positions[0].available_quantity == 75
    assert dashboard.orders[0].order_id == "order-1"
    assert dashboard.orders[0].order_price == 12.34
    assert repo.requested_order_limit == 20


@pytest.mark.asyncio
async def test_account_daily_profit_sums_position_day_profit() -> None:
    repo = FakeAccountCacheRepo()
    service = AccountAppService(
        portfolio_client=FakePortfolioClient(),
        account_cache_repo=repo,
        committer=FakeCommitter(),
    )

    await service.refresh_account_cache()

    assert repo.state is not None
    # Balance reports 600.0; positions carry day_profit 300.0, which wins.
    assert repo.state.daily_profit == 300.0


@pytest.mark.asyncio
async def test_account_daily_profit_falls_back_without_position_data() -> None:
    class NoDayProfitClient(FakePortfolioClient):
        async def get_positions(self) -> list[PositionSnapshot]:
            positions = await super().get_positions()
            return [
                PositionSnapshot(
                    symbol=position.symbol,
                    stock_name=position.stock_name,
                    quantity=position.quantity,
                    avg_cost=position.avg_cost,
                    current_price=position.current_price,
                    market_value=position.market_value,
                    profit_ratio=position.profit_ratio,
                )
                for position in positions
            ]

    repo = FakeAccountCacheRepo()
    service = AccountAppService(
        portfolio_client=NoDayProfitClient(),
        account_cache_repo=repo,
        committer=FakeCommitter(),
    )

    await service.refresh_account_cache()

    assert repo.state is not None
    assert repo.state.daily_profit == 600.0


@pytest.mark.asyncio
async def test_account_daily_profit_falls_back_when_position_data_partial() -> None:
    class PartialDayProfitClient(FakePortfolioClient):
        async def get_positions(self) -> list[PositionSnapshot]:
            positions = await super().get_positions()
            positions[0] = PositionSnapshot(
                symbol=positions[0].symbol,
                stock_name=positions[0].stock_name,
                quantity=positions[0].quantity,
                avg_cost=positions[0].avg_cost,
                current_price=positions[0].current_price,
                market_value=positions[0].market_value,
                profit_ratio=positions[0].profit_ratio,
                day_profit=None,
            )
            return positions + [
                PositionSnapshot(
                    symbol="000001",
                    stock_name="平安银行",
                    quantity=100,
                    avg_cost=10.0,
                    current_price=11.0,
                    market_value=1100.0,
                    profit_ratio=0.1,
                    day_profit=50.0,
                )
            ]

    repo = FakeAccountCacheRepo()
    service = AccountAppService(
        portfolio_client=PartialDayProfitClient(),
        account_cache_repo=repo,
        committer=FakeCommitter(),
    )

    await service.refresh_account_cache()

    assert repo.state is not None
    # One position reports day_profit and the other does not; summing the
    # partial payload would undercount, so the balance figure wins.
    assert repo.state.daily_profit == 600.0


@pytest.mark.asyncio
async def test_account_daily_profit_sums_only_when_all_positions_report() -> None:
    class AllDayProfitClient(FakePortfolioClient):
        async def get_positions(self) -> list[PositionSnapshot]:
            positions = await super().get_positions()
            return positions + [
                PositionSnapshot(
                    symbol="000001",
                    stock_name="平安银行",
                    quantity=100,
                    avg_cost=10.0,
                    current_price=11.0,
                    market_value=1100.0,
                    profit_ratio=0.1,
                    day_profit=50.0,
                )
            ]

    repo = FakeAccountCacheRepo()
    service = AccountAppService(
        portfolio_client=AllDayProfitClient(),
        account_cache_repo=repo,
        committer=FakeCommitter(),
    )

    await service.refresh_account_cache()

    assert repo.state is not None
    assert repo.state.daily_profit == 350.0


@pytest.mark.asyncio
async def test_account_refresh_raises_without_existing_cache() -> None:
    service = AccountAppService(
        portfolio_client=BrokenPortfolioClient(),
        account_cache_repo=FakeAccountCacheRepo(),
        committer=FakeCommitter(),
    )

    with pytest.raises(ServiceIntegrationError):
        await service.get_account_dashboard()


@pytest.mark.asyncio
async def test_account_refresh_uses_stale_cache_for_mapping_errors() -> None:
    repo = FakeAccountCacheRepo()
    initial = AccountAppService(
        portfolio_client=FakePortfolioClient(),
        account_cache_repo=repo,
        committer=FakeCommitter(),
    )
    await initial.refresh_account_cache()
    service = AccountAppService(
        portfolio_client=MalformedPortfolioClient(),
        account_cache_repo=repo,
        committer=FakeCommitter(),
    )

    result = await service.refresh_account_cache()

    assert result.status == "stale"
    assert repo.state is not None
    assert (
        repo.state.last_refresh_error
        == "invalid mx-moni response: invalid numeric field"
    )
