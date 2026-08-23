"""Shared run persistence and SSE callbacks."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from backend.business.runs import StrategyRun
from backend.business.runs.dto import to_run_detail_dto
from backend.business.runs.ports import RunRepositoryPort
from backend.business.runs.traces import RunTraceRecorder
from backend.business.shared import CommitterPort

RunSnapshotPublisher = Callable[[int, object], Awaitable[None]]
PersistRun = Callable[[StrategyRun], Awaitable[StrategyRun]]
logger = logging.getLogger(__name__)


class RunTraceSupport:
    def __init__(
        self,
        *,
        run_repo: RunRepositoryPort,
        committer: CommitterPort | None,
        snapshot_publisher: RunSnapshotPublisher | None,
    ) -> None:
        self._run_repo = run_repo
        self._committer = committer
        self._snapshot_publisher = snapshot_publisher

    def make_recorder(
        self,
        run: StrategyRun,
        *,
        persist_run: PersistRun | None = None,
    ) -> RunTraceRecorder:
        return RunTraceRecorder(
            run=run,
            persist_run=persist_run or self._run_repo.save,
            publish_snapshot=self.publish_payload,
            commit=None if self._committer is None else self._commit,
            has_snapshot_subscribers=self._has_subscribers,
        )

    async def publish(self, run: StrategyRun) -> None:
        await self.publish_payload(run.run_id, to_run_detail_dto(run))

    async def publish_payload(self, run_id: int, snapshot: object) -> None:
        if self._snapshot_publisher is None:
            return
        try:
            await self._snapshot_publisher(run_id, snapshot)
        except Exception:
            logger.exception(
                "failed to publish run snapshot",
                extra={"run_id": run_id},
            )

    async def _commit(self) -> None:
        if self._committer is not None:
            await self._committer.commit()

    def _has_subscribers(self, run_id: int) -> bool:
        hub = getattr(self._snapshot_publisher, "__self__", None)
        if hub is None:
            return True
        has = getattr(hub, "has_subscribers", None)
        return True if not callable(has) else bool(has(run_id))
