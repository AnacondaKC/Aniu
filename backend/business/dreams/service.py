"""Application service for idempotent nightly memory dreams."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from backend.business.dreams.models import DreamStatus, MemoryDream
from backend.business.dreams.ports import DreamAgentPort, DreamRepositoryPort
from backend.business.shared.ports import CommitterPort

logger = logging.getLogger(__name__)
ExecutionGuard = Callable[[], Awaitable[None]]
ExecutionFence = Callable[[MemoryDream, DreamStatus], Awaitable[bool]]


class ExecutionFencedError(RuntimeError):
    """Raised when a leased dream can no longer persist its state."""


_MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")


class DreamService:
    def __init__(
        self,
        repository: DreamRepositoryPort,
        agent: DreamAgentPort | None = None,
        *,
        committer: CommitterPort | None = None,
    ) -> None:
        self._repository = repository
        self._agent = agent
        self._committer = committer

    async def create_or_get(
        self,
        target_date: date,
        *,
        execution_guard: ExecutionGuard | None = None,
    ) -> MemoryDream:
        existing = await self._repository.get_by_date(target_date)
        if existing is not None:
            return existing
        dream = MemoryDream(
            task_id=await self._repository.next_task_id(target_date),
            target_date=target_date,
        )
        try:
            stored = await self._repository.add(dream)
            await self._commit(execution_guard=execution_guard)
            return stored
        except Exception:
            await self._rollback()
            existing = await self._repository.get_by_date(target_date)
            if existing is not None:
                return existing
            raise

    async def prepare_manual_run(self, *, now: datetime | None = None) -> MemoryDream:
        current = (now or datetime.now(tz=UTC)).astimezone(_MARKET_TIMEZONE)
        target_date = current.date() - timedelta(days=1)
        dream = await self.create_or_get(target_date)
        if dream.status in {DreamStatus.COMPLETED, DreamStatus.FAILED}:
            dream.retry()
            return await self._save_and_commit(dream)
        return dream

    async def list_recent(
        self, *, limit: int, offset: int
    ) -> tuple[list[MemoryDream], int]:
        total = await self._repository.count()
        items = await self._repository.list_recent(limit=limit, offset=offset)
        return items, total

    async def get_by_id(self, task_id: int) -> MemoryDream | None:
        return await self._repository.get_by_id(task_id)

    async def delete(self, task_id: int) -> bool:
        deleted = await self._repository.delete(task_id)
        if deleted:
            await self._commit()
        return deleted

    async def execute(
        self,
        task_id: int,
        *,
        execution_fence: ExecutionFence | None = None,
    ) -> MemoryDream | None:
        if self._agent is None:
            raise RuntimeError("dream agent is not configured")
        dream = await self._repository.get_by_id(task_id)
        if dream is None or dream.status is not DreamStatus.PENDING:
            return dream
        dream.start()
        await self._save_and_commit(
            dream,
            execution_fence=execution_fence,
            expected_status=DreamStatus.PENDING,
        )
        try:
            result = await self._agent.run(dream)
        except asyncio.CancelledError:
            try:
                interrupted = await self._repository.get_by_id(task_id)
                if (
                    interrupted is not None
                    and interrupted.status is DreamStatus.RUNNING
                ):
                    interrupted.fail("梦境任务被进程取消，未自动重试，请确认后手动重试")
                    await self._save_and_commit(
                        interrupted,
                        execution_fence=execution_fence,
                        expected_status=DreamStatus.RUNNING,
                    )
            except ExecutionFencedError:
                logger.info(
                    "memory dream lease fence rejected cancellation state",
                    extra={"task_id": task_id},
                )
            except Exception:  # noqa: BLE001 - preserve cancellation semantics
                logger.exception(
                    "failed to persist cancelled memory dream",
                    extra={"task_id": task_id},
                )
            raise
        except ExecutionFencedError:
            raise
        except Exception as exc:  # noqa: BLE001 - persist task failure state
            logger.exception(
                "memory dream execution failed",
                extra={"task_id": task_id},
            )
            failed = await self._repository.get_by_id(task_id)
            if failed is not None and failed.status is DreamStatus.RUNNING:
                failed.fail(str(exc))
                await self._save_and_commit(
                    failed,
                    execution_fence=execution_fence,
                    expected_status=DreamStatus.RUNNING,
                )
                return failed
            raise
        completed = await self._repository.get_by_id(task_id)
        if completed is None:
            return None
        if completed.status is DreamStatus.RUNNING:
            completed.complete(result)
            await self._save_and_commit(
                completed,
                execution_fence=execution_fence,
                expected_status=DreamStatus.RUNNING,
            )
        return completed

    async def _save_and_commit(
        self,
        dream: MemoryDream,
        *,
        execution_fence: ExecutionFence | None = None,
        expected_status: DreamStatus | None = None,
    ) -> MemoryDream:
        if execution_fence is None:
            saved = await self._repository.save(dream)
        else:
            if expected_status is None:
                raise ValueError("fenced dream saves require an expected status")
            if not await execution_fence(dream, expected_status):
                raise ExecutionFencedError(
                    f"dream execution fence rejected task_id={dream.task_id}"
                )
            saved = dream
        await self._commit()
        return saved

    async def _commit(self, *, execution_guard: ExecutionGuard | None = None) -> None:
        if execution_guard is not None:
            await execution_guard()
        if self._committer is not None:
            await self._committer.commit()

    async def _rollback(self) -> None:
        rollback = getattr(self._committer, "rollback", None)
        if callable(rollback):
            await rollback()


__all__ = ["DreamService", "ExecutionFencedError"]
