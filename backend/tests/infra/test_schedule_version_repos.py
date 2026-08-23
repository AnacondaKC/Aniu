"""Tests for schedule repositories."""

from __future__ import annotations

import pytest

from backend.business.runs import build_run_id
from backend.business.schedules import StrategySchedule
from backend.infra.repositories import ScheduleRepository


@pytest.mark.asyncio
async def test_schedule_repository_persists_updates(session) -> None:
    repo = ScheduleRepository(session)
    schedule = await repo.add(
        StrategySchedule(
            enabled=True,
            interval_minutes=30,
        )
    )
    expected_revision = schedule.revision
    schedule.apply_update(enabled=False, interval_minutes=30)
    updated = await repo.update_if_revision(
        schedule,
        expected_revision=expected_revision,
    )
    assert updated is not None
    schedule = updated
    await session.commit()

    listed = await repo.list_schedules()
    fetched = await repo.get_by_id(schedule.schedule_id)

    assert schedule.schedule_id > 0
    assert schedule.schedule_id == build_run_id(
        schedule.updated_at.date(),
        sequence=1,
        task_type=2,
    )
    assert listed[0].enabled is False
    assert fetched is not None
    assert fetched.enabled is False


@pytest.mark.asyncio
async def test_schedule_repository_rejects_stale_revision(session_factory) -> None:
    async with session_factory() as first_session:
        repo = ScheduleRepository(first_session)
        stored = await repo.add(StrategySchedule(enabled=True, interval_minutes=30))
        await first_session.commit()

    stale = StrategySchedule(
        schedule_id=stored.schedule_id,
        enabled=False,
        interval_minutes=45,
        revision=stored.revision + 1,
    )
    async with session_factory() as winning_session:
        winner = ScheduleRepository(winning_session)
        updated = await winner.update_if_revision(
            stale,
            expected_revision=stored.revision,
        )
        assert updated is not None
        await winning_session.commit()

    stale.interval_minutes = 60
    async with session_factory() as losing_session:
        rejected = await ScheduleRepository(losing_session).update_if_revision(
            stale,
            expected_revision=stored.revision,
        )

    assert rejected is None


@pytest.mark.asyncio
async def test_schedule_repository_degrades_corrupt_custom_times(session) -> None:
    repo = ScheduleRepository(session)
    stored = await repo.add(
        StrategySchedule(
            enabled=True,
            interval_minutes=60,
            custom_schedule_times=("09:30", "14:00"),
        )
    )
    await session.commit()

    # Simulate a payload that predates the schema or was written by a buggy
    # build; reading it must degrade to the interval-derived schedule instead
    # of failing the whole schedules listing.
    from sqlalchemy import update

    from backend.infra.db.models import StrategyScheduleModel

    await session.execute(
        update(StrategyScheduleModel)
        .where(StrategyScheduleModel.id == stored.schedule_id)
        .values(custom_schedule_times_json="not-json")
    )
    await session.commit()

    fetched = await repo.get_by_id(stored.schedule_id)
    assert fetched is not None
    assert fetched.custom_schedule_times is None
    assert fetched.schedule_times == ("09:30", "10:30", "13:00", "14:00")
