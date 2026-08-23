"""Account API response schemas."""

from __future__ import annotations

from datetime import date, datetime

from backend.api.schemas.common import ApiModel


class AccountOverviewResponse(ApiModel):
    total_asset: float
    available_cash: float
    frozen_cash: float
    market_value: float
    total_profit: float
    daily_profit: float
    operation_days: int | None = None
    open_date: date | None = None
    initial_capital: float | None = None
    net_value: float | None = None
    position_ratio: float | None = None
    performance_date: date | None = None
    captured_at: datetime
    total_return_rate: float | None = None
    current_net_value: float | None = None
    daily_return_rate: float | None = None
    today_trade_count: int | None = None


class PositionSnapshotResponse(ApiModel):
    symbol: str
    stock_name: str
    quantity: int
    avg_cost: float
    current_price: float
    market_value: float
    profit_ratio: float
    day_profit: float | None = None
    available_quantity: int | None = None
    captured_at: datetime


class PortfolioOrderResponse(ApiModel):
    order_id: str
    symbol: str
    stock_name: str
    direction: str
    quantity: int
    order_price: float | None = None
    filled_quantity: int
    filled_price: float | None = None
    status: str
    submitted_at: datetime | None = None
    updated_at: datetime | None = None


class AccountDashboardResponse(ApiModel):
    overview: AccountOverviewResponse
    positions: list[PositionSnapshotResponse]
    orders: list[PortfolioOrderResponse]


class AccountRefreshResultResponse(ApiModel):
    status: str
    message: str
    captured_at: datetime | None = None
    last_refresh_attempt_at: datetime | None = None
    last_refresh_succeeded_at: datetime | None = None
