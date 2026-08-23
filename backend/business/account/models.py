"""Market & portfolio read-model snapshots."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime

from backend.business.shared.trading.value_objects import (
    ensure_non_empty_str,
    validate_symbol,
)


def utc_now() -> datetime:
    """Return the current UTC time."""

    return datetime.now(tz=UTC)


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    """Account overview snapshot from the portfolio provider."""

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
    captured_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True, slots=True)
class AccountCacheState:
    """Persisted account cache metadata plus latest overview snapshot."""

    total_asset: float | None = None
    available_cash: float | None = None
    frozen_cash: float | None = None
    market_value: float | None = None
    total_profit: float | None = None
    daily_profit: float | None = None
    operation_days: int | None = None
    open_date: date | None = None
    initial_capital: float | None = None
    net_value: float | None = None
    position_ratio: float | None = None
    captured_at: datetime | None = None
    last_refresh_attempt_at: datetime | None = None
    last_refresh_succeeded_at: datetime | None = None
    last_refresh_error: str | None = None

    def to_snapshot(self) -> AccountSnapshot | None:
        values = (
            self.total_asset,
            self.available_cash,
            self.frozen_cash,
            self.market_value,
            self.total_profit,
            self.daily_profit,
            self.captured_at,
        )
        if any(value is None for value in values):
            return None

        # Local bindings for the type checker after the None guard above.
        total_asset = self.total_asset
        available_cash = self.available_cash
        frozen_cash = self.frozen_cash
        market_value = self.market_value
        total_profit = self.total_profit
        daily_profit = self.daily_profit
        captured_at = self.captured_at
        assert total_asset is not None
        assert available_cash is not None
        assert frozen_cash is not None
        assert market_value is not None
        assert total_profit is not None
        assert daily_profit is not None
        assert captured_at is not None

        return AccountSnapshot(
            total_asset=total_asset,
            available_cash=available_cash,
            frozen_cash=frozen_cash,
            market_value=market_value,
            total_profit=total_profit,
            daily_profit=daily_profit,
            operation_days=self.operation_days,
            open_date=self.open_date,
            initial_capital=self.initial_capital,
            net_value=self.net_value,
            position_ratio=self.position_ratio,
            captured_at=captured_at,
        )


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    """Position snapshot from the portfolio provider."""

    symbol: str
    stock_name: str
    quantity: int
    avg_cost: float
    current_price: float
    market_value: float
    profit_ratio: float
    day_profit: float | None = None
    available_quantity: int | None = None
    captured_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", validate_symbol(self.symbol))
        object.__setattr__(
            self,
            "stock_name",
            ensure_non_empty_str(self.stock_name, "stock_name"),
        )
        if self.quantity < 0:
            raise ValueError("quantity must be >= 0")
        if self.available_quantity is not None and self.available_quantity < 0:
            raise ValueError("available_quantity must be >= 0")


@dataclass(frozen=True, slots=True)
class PortfolioOrderSnapshot:
    """Portfolio order/entrust snapshot from the provider."""

    order_id: str
    symbol: str
    stock_name: str
    direction: str
    quantity: int
    status: str
    filled_quantity: int
    filled_price: float | None
    order_price: float | None = None
    submitted_at: datetime | None = None
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "order_id",
            ensure_non_empty_str(self.order_id, "order_id"),
        )
        object.__setattr__(self, "symbol", validate_symbol(self.symbol))
        object.__setattr__(
            self,
            "stock_name",
            ensure_non_empty_str(self.stock_name, "stock_name"),
        )
        object.__setattr__(
            self,
            "direction",
            ensure_non_empty_str(self.direction, "direction"),
        )
        object.__setattr__(
            self,
            "status",
            ensure_non_empty_str(self.status, "status"),
        )

        if self.quantity < 0:
            raise ValueError("quantity must be >= 0")
        if self.filled_quantity < 0:
            raise ValueError("filled_quantity must be >= 0")
        if self.order_price is not None and self.order_price < 0:
            raise ValueError("order_price must be >= 0")
        if self.filled_quantity == 0 and self.filled_price is not None:
            raise ValueError("filled_price must be None when filled_quantity is 0")
