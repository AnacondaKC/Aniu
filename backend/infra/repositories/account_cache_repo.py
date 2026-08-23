"""Repository for persisted account overview/positions/orders cache."""

from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.business.account import (
    AccountCacheState,
    AccountSnapshot,
    PortfolioOrderSnapshot,
    PositionSnapshot,
)
from backend.infra.db.models import (
    AccountCacheStateModel,
    AccountOrderCacheModel,
    AccountPositionCacheModel,
    AccountProfileCacheModel,
)


class AccountCacheRepository:
    """Persistence adapter for locally cached account data."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def get_state(self) -> AccountCacheState | None:
        state_model = await self._get_state_model()
        profile_model = await self._get_profile_model()
        if state_model is None and profile_model is None:
            return None
        return self._to_state(state_model, profile_model)

    async def get_account_snapshot(self) -> AccountSnapshot | None:
        state = await self.get_state()
        return None if state is None else state.to_snapshot()

    async def list_positions(self) -> list[PositionSnapshot]:
        statement = select(AccountPositionCacheModel).order_by(
            AccountPositionCacheModel.market_value.desc(),
            AccountPositionCacheModel.symbol.asc(),
        )
        models = list((await self._session.scalars(statement)).all())
        return [self._to_position(model) for model in models]

    async def list_orders(self, *, limit: int) -> list[PortfolioOrderSnapshot]:
        statement = (
            select(AccountOrderCacheModel)
            .order_by(
                AccountOrderCacheModel.updated_at.desc(),
                AccountOrderCacheModel.order_id.desc(),
            )
            .limit(limit)
        )
        models = list((await self._session.scalars(statement)).all())
        return [self._to_order(model) for model in models]

    async def count_filled_orders_on_date(self, day: date) -> int:
        order_date = func.date(
            func.coalesce(
                AccountOrderCacheModel.updated_at,
                AccountOrderCacheModel.submitted_at,
            )
        )
        statement = (
            select(func.count())
            .select_from(AccountOrderCacheModel)
            .where(
                order_date == day.isoformat(),
                AccountOrderCacheModel.filled_quantity > 0,
            )
        )
        return int((await self._session.scalar(statement)) or 0)

    async def replace_cache(
        self,
        snapshot: AccountSnapshot,
        positions: list[PositionSnapshot],
        orders: list[PortfolioOrderSnapshot],
        *,
        attempted_at: datetime,
    ) -> AccountCacheState:
        state_model = await self._get_or_create_state_model()
        state_model.total_asset = snapshot.total_asset
        state_model.available_cash = snapshot.available_cash
        state_model.frozen_cash = snapshot.frozen_cash
        state_model.market_value = snapshot.market_value
        state_model.total_profit = snapshot.total_profit
        state_model.daily_profit = snapshot.daily_profit
        state_model.captured_at = snapshot.captured_at.isoformat()
        state_model.last_refresh_attempt_at = attempted_at.isoformat()
        state_model.last_refresh_succeeded_at = attempted_at.isoformat()
        state_model.last_refresh_error = None

        profile_model = await self._get_or_create_profile_model()
        profile_model.operation_days = snapshot.operation_days
        profile_model.open_date = _serialize_date(snapshot.open_date)
        profile_model.initial_capital = snapshot.initial_capital
        profile_model.net_value = snapshot.net_value
        profile_model.position_ratio = snapshot.position_ratio

        await self._session.execute(delete(AccountPositionCacheModel))
        await self._session.execute(delete(AccountOrderCacheModel))

        for position in positions:
            self._session.add(
                AccountPositionCacheModel(
                    symbol=position.symbol,
                    stock_name=position.stock_name,
                    quantity=position.quantity,
                    avg_cost=position.avg_cost,
                    current_price=position.current_price,
                    market_value=position.market_value,
                    profit_ratio=position.profit_ratio,
                    day_profit=position.day_profit,
                    available_quantity=position.available_quantity,
                    captured_at=position.captured_at.isoformat(),
                )
            )

        for order in orders:
            self._session.add(
                AccountOrderCacheModel(
                    order_id=order.order_id,
                    symbol=order.symbol,
                    stock_name=order.stock_name,
                    direction=order.direction,
                    quantity=order.quantity,
                    order_price=order.order_price,
                    filled_quantity=order.filled_quantity,
                    filled_price=order.filled_price,
                    status=order.status,
                    submitted_at=(
                        None
                        if order.submitted_at is None
                        else order.submitted_at.isoformat()
                    ),
                    updated_at=(
                        None
                        if order.updated_at is None
                        else order.updated_at.isoformat()
                    ),
                )
            )

        await self._session.flush()
        return self._to_state(state_model, profile_model)

    async def record_refresh_failure(
        self,
        *,
        attempted_at: datetime,
        error_message: str,
    ) -> AccountCacheState:
        model = await self._get_or_create_state_model()
        model.last_refresh_attempt_at = attempted_at.isoformat()
        model.last_refresh_error = error_message
        await self._session.flush()
        return await self.get_state() or AccountCacheState(
            last_refresh_attempt_at=attempted_at,
            last_refresh_error=error_message,
        )

    async def _get_state_model(self) -> AccountCacheStateModel | None:
        statement = (
            select(AccountCacheStateModel)
            .order_by(AccountCacheStateModel.id.asc())
            .limit(1)
        )
        return (await self._session.scalars(statement)).first()

    async def _get_or_create_state_model(self) -> AccountCacheStateModel:
        model = await self._get_state_model()
        if model is None:
            model = AccountCacheStateModel()
            self._session.add(model)
            await self._session.flush()
        return model

    async def _get_profile_model(self) -> AccountProfileCacheModel | None:
        statement = (
            select(AccountProfileCacheModel)
            .order_by(AccountProfileCacheModel.id.asc())
            .limit(1)
        )
        return (await self._session.scalars(statement)).first()

    async def _get_or_create_profile_model(self) -> AccountProfileCacheModel:
        model = await self._get_profile_model()
        if model is None:
            model = AccountProfileCacheModel()
            self._session.add(model)
            await self._session.flush()
        return model

    def _to_state(
        self,
        state_model: AccountCacheStateModel | None,
        profile_model: AccountProfileCacheModel | None,
    ) -> AccountCacheState:
        return AccountCacheState(
            total_asset=None if state_model is None else state_model.total_asset,
            available_cash=None if state_model is None else state_model.available_cash,
            frozen_cash=None if state_model is None else state_model.frozen_cash,
            market_value=None if state_model is None else state_model.market_value,
            total_profit=None if state_model is None else state_model.total_profit,
            daily_profit=None if state_model is None else state_model.daily_profit,
            operation_days=(
                None if profile_model is None else profile_model.operation_days
            ),
            open_date=(
                None if profile_model is None else _parse_date(profile_model.open_date)
            ),
            initial_capital=(
                None if profile_model is None else profile_model.initial_capital
            ),
            net_value=None if profile_model is None else profile_model.net_value,
            position_ratio=(
                None if profile_model is None else profile_model.position_ratio
            ),
            captured_at=(
                None
                if state_model is None
                else _parse_datetime(state_model.captured_at)
            ),
            last_refresh_attempt_at=(
                None
                if state_model is None
                else _parse_datetime(state_model.last_refresh_attempt_at)
            ),
            last_refresh_succeeded_at=(
                None
                if state_model is None
                else _parse_datetime(state_model.last_refresh_succeeded_at)
            ),
            last_refresh_error=(
                None if state_model is None else state_model.last_refresh_error
            ),
        )

    def _to_position(self, model: AccountPositionCacheModel) -> PositionSnapshot:
        return PositionSnapshot(
            symbol=model.symbol,
            stock_name=model.stock_name,
            quantity=model.quantity,
            avg_cost=model.avg_cost,
            current_price=model.current_price,
            market_value=model.market_value,
            profit_ratio=model.profit_ratio,
            day_profit=model.day_profit,
            available_quantity=model.available_quantity,
            captured_at=datetime.fromisoformat(model.captured_at),
        )

    def _to_order(self, model: AccountOrderCacheModel) -> PortfolioOrderSnapshot:
        return PortfolioOrderSnapshot(
            order_id=model.order_id,
            symbol=model.symbol,
            stock_name=model.stock_name,
            direction=model.direction,
            quantity=model.quantity,
            order_price=model.order_price,
            filled_quantity=model.filled_quantity,
            filled_price=model.filled_price,
            status=model.status,
            submitted_at=_parse_datetime(model.submitted_at),
            updated_at=_parse_datetime(model.updated_at),
        )


def _parse_datetime(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value)


def _parse_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


def _serialize_date(value: date | None) -> str | None:
    return None if value is None else value.isoformat()
