"""DTOs for account / positions / orders APIs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from backend.business.account import (
    AccountCacheState,
    AccountSnapshot,
    PortfolioOrderSnapshot,
    PositionSnapshot,
)


@dataclass(frozen=True, slots=True)
class AccountOverviewDTO:
    total_asset: float
    available_cash: float
    frozen_cash: float
    market_value: float
    total_profit: float
    daily_profit: float
    operation_days: int | None
    open_date: date | None
    initial_capital: float | None
    net_value: float | None
    position_ratio: float | None
    performance_date: date | None
    captured_at: datetime
    # Backend-computed read-model fields for the dashboard.
    total_return_rate: float | None = None
    current_net_value: float | None = None
    daily_return_rate: float | None = None
    today_trade_count: int | None = None


@dataclass(frozen=True, slots=True)
class PositionSnapshotDTO:
    symbol: str
    stock_name: str
    quantity: int
    avg_cost: float
    current_price: float
    market_value: float
    profit_ratio: float
    captured_at: datetime
    day_profit: float | None = None
    available_quantity: int | None = None


@dataclass(frozen=True, slots=True)
class PortfolioOrderDTO:
    order_id: str
    symbol: str
    stock_name: str
    direction: str
    quantity: int
    order_price: float | None
    filled_quantity: int
    filled_price: float | None
    status: str
    submitted_at: datetime | None
    updated_at: datetime | None


@dataclass(frozen=True, slots=True)
class AccountDashboardDTO:
    overview: AccountOverviewDTO
    positions: list[PositionSnapshotDTO]
    orders: list[PortfolioOrderDTO]


@dataclass(frozen=True, slots=True)
class AccountRefreshResultDTO:
    status: str
    message: str
    captured_at: datetime | None
    last_refresh_attempt_at: datetime | None
    last_refresh_succeeded_at: datetime | None


def to_account_overview_dto(
    snapshot: AccountSnapshot,
    *,
    performance_date: date | None = None,
    today_trade_count: int | None = None,
) -> AccountOverviewDTO:
    initial = snapshot.initial_capital
    safe_initial = initial if initial is not None and initial > 0 else None
    current_net_value = snapshot.net_value
    if current_net_value is None and safe_initial is not None:
        current_net_value = snapshot.total_asset / safe_initial
    total_return_rate = None
    if current_net_value is not None:
        total_return_rate = current_net_value - 1.0
    elif safe_initial is not None:
        total_return_rate = snapshot.total_profit / safe_initial
    previous_asset = snapshot.total_asset - snapshot.daily_profit
    if previous_asset <= 0 and safe_initial is not None:
        previous_asset = safe_initial
    daily_return_rate = (
        snapshot.daily_profit / previous_asset if previous_asset > 0 else None
    )
    position_ratio = snapshot.position_ratio
    if position_ratio is None and snapshot.total_asset > 0:
        position_ratio = snapshot.market_value / snapshot.total_asset
    return AccountOverviewDTO(
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
        position_ratio=position_ratio,
        performance_date=performance_date,
        captured_at=snapshot.captured_at,
        total_return_rate=total_return_rate,
        current_net_value=current_net_value,
        daily_return_rate=daily_return_rate,
        today_trade_count=today_trade_count,
    )


def to_position_dto(snapshot: PositionSnapshot) -> PositionSnapshotDTO:
    return PositionSnapshotDTO(
        symbol=snapshot.symbol,
        stock_name=snapshot.stock_name,
        quantity=snapshot.quantity,
        avg_cost=snapshot.avg_cost,
        current_price=snapshot.current_price,
        market_value=snapshot.market_value,
        profit_ratio=snapshot.profit_ratio,
        day_profit=snapshot.day_profit,
        available_quantity=snapshot.available_quantity,
        captured_at=snapshot.captured_at,
    )


def to_portfolio_order_dto(snapshot: PortfolioOrderSnapshot) -> PortfolioOrderDTO:
    return PortfolioOrderDTO(
        order_id=snapshot.order_id,
        symbol=snapshot.symbol,
        stock_name=snapshot.stock_name,
        direction=snapshot.direction,
        quantity=snapshot.quantity,
        order_price=snapshot.order_price,
        status=snapshot.status,
        filled_quantity=snapshot.filled_quantity,
        filled_price=snapshot.filled_price,
        submitted_at=snapshot.submitted_at,
        updated_at=snapshot.updated_at,
    )


def to_account_refresh_result_dto(
    *, state: AccountCacheState | None, status: str, message: str
) -> AccountRefreshResultDTO:
    return AccountRefreshResultDTO(
        status=status,
        message=message,
        captured_at=None if state is None else state.captured_at,
        last_refresh_attempt_at=(
            None if state is None else state.last_refresh_attempt_at
        ),
        last_refresh_succeeded_at=(
            None if state is None else state.last_refresh_succeeded_at
        ),
    )
