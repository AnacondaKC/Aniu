"""Tests for bounded account-cache reads."""

from __future__ import annotations

from datetime import date

import pytest

from backend.infra.db.models import AccountOrderCacheModel
from backend.infra.repositories import AccountCacheRepository


@pytest.mark.asyncio
async def test_account_order_reads_are_bounded_and_counted_in_sql(session) -> None:
    for index in range(30):
        day = "2026-07-31" if index < 12 else "2026-07-30"
        session.add(
            AccountOrderCacheModel(
                order_id=f"order-{index:02d}",
                symbol="600000",
                stock_name="浦发银行",
                direction="BUY",
                quantity=100,
                order_price=10.0,
                status="FILLED",
                filled_quantity=100,
                filled_price=10.0,
                submitted_at=f"{day}T08:00:00+00:00",
                updated_at=f"{day}T08:{index:02d}:00+00:00",
            )
        )
    await session.commit()
    repo = AccountCacheRepository(session)

    recent = await repo.list_orders(limit=20)
    count = await repo.count_filled_orders_on_date(date(2026, 7, 31))

    assert len(recent) == 20
    assert count == 12


@pytest.mark.asyncio
async def test_account_daily_trade_count_excludes_unfilled_orders(session) -> None:
    session.add(
        AccountOrderCacheModel(
            order_id="filled-1",
            symbol="600000",
            stock_name="浦发银行",
            direction="BUY",
            quantity=100,
            order_price=10.0,
            status="FILLED",
            filled_quantity=100,
            filled_price=10.0,
            submitted_at="2026-07-31T08:00:00+00:00",
            updated_at="2026-07-31T08:00:00+00:00",
        )
    )
    session.add(
        AccountOrderCacheModel(
            order_id="rejected-1",
            symbol="600000",
            stock_name="浦发银行",
            direction="BUY",
            quantity=100,
            order_price=10.0,
            status="REJECTED",
            filled_quantity=0,
            filled_price=None,
            submitted_at="2026-07-31T09:00:00+00:00",
            updated_at="2026-07-31T09:00:00+00:00",
        )
    )
    session.add(
        AccountOrderCacheModel(
            order_id="cancelled-1",
            symbol="600000",
            stock_name="浦发银行",
            direction="SELL",
            quantity=100,
            order_price=10.0,
            status="CANCELLED",
            filled_quantity=0,
            filled_price=None,
            submitted_at="2026-07-31T10:00:00+00:00",
            updated_at="2026-07-31T10:00:00+00:00",
        )
    )
    await session.commit()
    repo = AccountCacheRepository(session)

    count = await repo.count_filled_orders_on_date(date(2026, 7, 31))

    assert count == 1
