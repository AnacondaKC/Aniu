"""Tests for durable run job lease repository."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.exc import IntegrityError

from backend.business.runs import StrategyRun, StrategySnapshot
from backend.business.runs.job import RunJobStatus
from backend.business.shared.enums import TriggerSource
from backend.infra.repositories import RunJobRepository, RunRepository


def _run(run_id: int) -> StrategyRun:
    return StrategyRun(
        run_id=run_id,
        trigger_source=TriggerSource.MANUAL,
        schedule_id=None,
        snapshot=StrategySnapshot(
            prompt_version="v1",
            risk_rules_version="risk-v1",
        ),
    )


@pytest.mark.asyncio
async def test_only_one_active_job_allowed(session) -> None:
    runs = RunRepository(session)
    jobs = RunJobRepository(session)
    await runs.add(_run(202607241))
    await jobs.create_pending(202607241)
    await session.commit()

    await runs.add(_run(202607242))
    with pytest.raises(IntegrityError):
        await jobs.create_pending(202607242)
        await session.commit()


@pytest.mark.asyncio
async def test_claim_and_heartbeat_and_complete(session) -> None:
    runs = RunRepository(session)
    jobs = RunJobRepository(session)
    await runs.add(_run(202607251))
    await jobs.create_pending(202607251)
    await session.commit()

    claimed = await jobs.claim_next(worker_id="w1", lease_seconds=20)
    await session.commit()
    assert claimed is not None
    assert claimed.status is RunJobStatus.LEASED
    assert claimed.worker_id == "w1"
    assert claimed.attempt == 1

    # Another worker cannot claim while lease is live.
    second = await jobs.claim_next(worker_id="w2", lease_seconds=20)
    assert second is None

    assert claimed.claim_token is not None
    heart = await jobs.heartbeat(
        202607251,
        worker_id="w1",
        claim_token=claimed.claim_token,
        lease_seconds=20,
    )
    await session.commit()
    assert heart is not None
    assert heart.worker_id == "w1"

    done = await jobs.mark_terminal(
        202607251,
        status=RunJobStatus.COMPLETED,
        worker_id="w1",
        claim_token=claimed.claim_token,
    )
    await session.commit()
    assert done is not None
    assert done.status is RunJobStatus.COMPLETED
    assert done.active_guard is None


@pytest.mark.asyncio
async def test_two_workers_claim_job_only_once(
    session,
    session_factory,
) -> None:
    runs = RunRepository(session)
    jobs = RunJobRepository(session)
    await runs.add(_run(202607252))
    await jobs.create_pending(202607252)
    await session.commit()

    async def claim(worker_id: str):
        async with session_factory() as worker_session:
            claimed = await RunJobRepository(worker_session).claim_next(
                worker_id=worker_id,
                lease_seconds=20,
            )
            await worker_session.commit()
            return claimed

    results = await asyncio.gather(claim("w1"), claim("w2"))
    winners = [job for job in results if job is not None]

    assert len(winners) == 1
    assert winners[0].worker_id in {"w1", "w2"}
    assert winners[0].attempt == 1


@pytest.mark.asyncio
async def test_expired_lease_can_be_reclaimed(session) -> None:
    runs = RunRepository(session)
    jobs = RunJobRepository(session)
    await runs.add(_run(202607261))
    await jobs.create_pending(202607261)
    await session.commit()

    claimed = await jobs.claim_next(worker_id="w1", lease_seconds=1)
    await session.commit()
    assert claimed is not None

    # Force lease expiry.
    from sqlalchemy import update

    from backend.infra.db.models import RunJobModel

    past = (datetime.now(tz=UTC) - timedelta(seconds=5)).isoformat()
    await session.execute(
        update(RunJobModel)
        .where(RunJobModel.run_id == 202607261)
        .values(lease_expires_at=past)
    )
    await session.commit()

    reclaimed = await jobs.claim_next(worker_id="w2", lease_seconds=20)
    await session.commit()
    assert reclaimed is not None
    assert reclaimed.worker_id == "w2"
    assert reclaimed.attempt == 2


@pytest.mark.asyncio
async def test_expired_claim_cannot_heartbeat_or_save_run(session) -> None:
    runs = RunRepository(session)
    jobs = RunJobRepository(session)
    run_id = 202607263
    now = datetime.now(tz=UTC) + timedelta(seconds=1)
    await runs.add(_run(run_id))
    await jobs.create_pending(run_id)
    await session.commit()

    claimed = await jobs.claim_next(worker_id="w1", lease_seconds=1, now=now)
    await session.commit()
    assert claimed is not None and claimed.claim_token is not None
    run = await runs.get_by_id(run_id)
    assert run is not None

    expired_now = now + timedelta(seconds=2)
    assert (
        await jobs.heartbeat(
            run_id,
            worker_id="w1",
            claim_token=claimed.claim_token,
            lease_seconds=20,
            now=expired_now,
        )
        is None
    )
    assert not await runs.save_fenced(
        run,
        worker_id="w1",
        claim_token=claimed.claim_token,
        now=expired_now,
    )
    await session.commit()

    reclaimed = await jobs.claim_next(worker_id="w2", lease_seconds=20, now=expired_now)
    assert reclaimed is not None
    assert reclaimed.worker_id == "w2"


@pytest.mark.asyncio
async def test_request_cancel_pending_is_terminal(session) -> None:
    runs = RunRepository(session)
    jobs = RunJobRepository(session)
    await runs.add(_run(202607271))
    await jobs.create_pending(202607271)
    await session.commit()

    cancelled = await jobs.request_cancel(202607271, reason="user_requested")
    await session.commit()
    assert cancelled is not None
    assert cancelled.status is RunJobStatus.CANCELLED
    assert cancelled.active_guard is None


@pytest.mark.asyncio
async def test_stale_claim_cannot_heartbeat_or_finish_reclaimed_job(session) -> None:
    from sqlalchemy import update

    from backend.infra.db.models import RunJobModel

    run_id = 202607281
    runs = RunRepository(session)
    jobs = RunJobRepository(session)
    await runs.add(_run(run_id))
    await jobs.create_pending(run_id)
    await session.commit()

    first = await jobs.claim_next(worker_id="w1", lease_seconds=1)
    await session.commit()
    assert first is not None and first.claim_token is not None
    await session.execute(
        update(RunJobModel)
        .where(RunJobModel.run_id == run_id)
        .values(
            lease_expires_at=(datetime.now(tz=UTC) - timedelta(seconds=1)).isoformat()
        )
    )
    await session.commit()
    second = await jobs.claim_next(worker_id="w2", lease_seconds=20)
    await session.commit()
    assert second is not None and second.claim_token is not None
    assert second.claim_token != first.claim_token

    assert (
        await jobs.heartbeat(
            run_id,
            worker_id="w1",
            claim_token=first.claim_token,
            lease_seconds=20,
        )
        is None
    )
    assert (
        await jobs.mark_terminal(
            run_id,
            status=RunJobStatus.COMPLETED,
            worker_id="w1",
            claim_token=first.claim_token,
        )
        is None
    )
    current = await jobs.get_by_run_id(run_id)
    assert current is not None
    assert current.worker_id == "w2"
    assert current.claim_token == second.claim_token


@pytest.mark.asyncio
async def test_reclaim_preserves_cancel_request(session) -> None:
    from sqlalchemy import update

    from backend.infra.db.models import RunJobModel

    run_id = 202607282
    jobs = RunJobRepository(session)
    await RunRepository(session).add(_run(run_id))
    await jobs.create_pending(run_id)
    await session.commit()
    first = await jobs.claim_next(worker_id="w1", lease_seconds=20)
    await session.commit()
    assert first is not None
    requested = await jobs.request_cancel(run_id, reason="user_requested")
    await session.commit()
    assert requested is not None
    assert requested.status is RunJobStatus.CANCEL_REQUESTED
    await session.execute(
        update(RunJobModel)
        .where(RunJobModel.run_id == run_id)
        .values(
            lease_expires_at=(datetime.now(tz=UTC) - timedelta(seconds=1)).isoformat()
        )
    )
    await session.commit()

    reclaimed = await jobs.claim_next(worker_id="w2", lease_seconds=20)
    await session.commit()
    assert reclaimed is not None
    assert reclaimed.status is RunJobStatus.CANCEL_REQUESTED
    assert reclaimed.cancel_reason == "user_requested"
