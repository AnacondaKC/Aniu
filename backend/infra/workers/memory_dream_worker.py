"""Database-leased worker for independent memory dream tasks."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Callable
from contextlib import suppress

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.business.dreams import DreamService, DreamStatus, MemoryDream
from backend.infra.lease_guard import run_while_lease_valid
from backend.infra.repositories.memory_dream_repo import MemoryDreamRepository
from backend.infra.repositories.task_lease_repo import TaskLeaseRepository

logger = logging.getLogger(__name__)
DREAM_LEASE_SECONDS = 60.0
DREAM_LEASE_RENEW_SECONDS = 20.0
DREAM_RECOVERY_INTERVAL_SECONDS = 10.0
DREAM_QUEUE_MAXSIZE = 256


class MemoryDreamWorker:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        service_factory: Callable[[AsyncSession], DreamService],
    ) -> None:
        self._session_factory = session_factory
        self._service_factory = service_factory
        self._queue: asyncio.Queue[int] = asyncio.Queue(maxsize=DREAM_QUEUE_MAXSIZE)
        self._queued_ids: set[int] = set()
        self._task: asyncio.Task[None] | None = None
        self._owner_id = f"memory-dream-{uuid.uuid4().hex}"
        self._stopping = False

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done() and not self._stopping

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._task = asyncio.create_task(
            self._run_loop(),
            name="aniu-memory-dream-worker",
        )

    async def stop(self) -> None:
        self._stopping = True
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def submit(self, task_id: int) -> None:
        if self._stopping:
            raise RuntimeError("memory dream worker is stopping")
        if self._task is None or self._task.done():
            self.start()
        self._enqueue(task_id)

    def _enqueue(self, task_id: int) -> bool:
        if task_id in self._queued_ids:
            return False
        try:
            self._queue.put_nowait(task_id)
        except asyncio.QueueFull:
            logger.warning(
                "memory dream wake queue is full; task remains pending in database",
                extra={"task_id": task_id},
            )
            return False
        self._queued_ids.add(task_id)
        return True

    async def recover_pending(self) -> None:
        await self._fail_expired_running()
        async with self._session_factory() as session:
            pending = await MemoryDreamRepository(session).list_pending(
                limit=DREAM_QUEUE_MAXSIZE
            )
        for dream in pending:
            self._enqueue(dream.task_id)

    async def _fail_expired_running(self) -> bool:
        async with self._session_factory() as session:
            running_ids = [
                dream.task_id
                for dream in await MemoryDreamRepository(session).list_running()
            ]

        changed = False
        for task_id in running_ids:
            if await self._recover_running_task(task_id):
                changed = True
        return changed

    async def _recover_running_task(self, task_id: int) -> bool:
        lease_key = self._lease_key(task_id)
        recovery_owner = f"{self._owner_id}:recovery:{uuid.uuid4().hex}"
        async with self._session_factory() as session:
            repository = MemoryDreamRepository(session)
            lease_repository = TaskLeaseRepository(session)
            acquired = await lease_repository.try_acquire(
                lease_key=lease_key,
                owner_id=recovery_owner,
                lease_seconds=DREAM_LEASE_SECONDS,
            )
            if not acquired:
                return False
            try:
                dream = await repository.get_by_id(task_id)
                if dream is None or dream.status is not DreamStatus.RUNNING:
                    await lease_repository.release(
                        lease_key=lease_key, owner_id=recovery_owner
                    )
                    await session.commit()
                    return False
                dream.fail("应用进程中断，未自动重试，请确认后手动重试")
                await repository.save(dream)
                await lease_repository.release(
                    lease_key=lease_key, owner_id=recovery_owner
                )
                await session.commit()
                return True
            except Exception:
                await session.rollback()
                raise

    async def _recover_tasks(self) -> None:
        try:
            await self.recover_pending()
        except Exception:
            logger.exception("memory dream task recovery failed")

    async def _run_loop(self) -> None:
        while not self._stopping:
            try:
                task_id = await asyncio.wait_for(
                    self._queue.get(), timeout=DREAM_RECOVERY_INTERVAL_SECONDS
                )
            except TimeoutError:
                await self._recover_tasks()
                continue
            try:
                await self._execute(task_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "memory dream worker execution failed",
                    extra={"task_id": task_id},
                )
            finally:
                self._queued_ids.discard(task_id)
                self._queue.task_done()

    async def _execute(self, task_id: int) -> None:
        lease_key = self._lease_key(task_id)
        async with self._session_factory() as session:
            owner_id = f"{self._owner_id}:{uuid.uuid4().hex}"
            acquired = await TaskLeaseRepository(session).try_acquire(
                lease_key=lease_key,
                owner_id=owner_id,
                lease_seconds=DREAM_LEASE_SECONDS,
            )
            if not acquired:
                logger.debug("another worker owns memory dream task_id=%s", task_id)
                return
            await session.commit()

        stop_renewal = asyncio.Event()
        lease_lost = asyncio.Event()
        renewal_task = asyncio.create_task(
            self._renew_lease(lease_key, owner_id, stop_renewal, lease_lost),
            name=f"aniu-memory-dream-lease-{task_id}",
        )

        async def execute_dream() -> None:
            async with self._session_factory() as session:
                repository = MemoryDreamRepository(session)

                async def fenced_save(
                    dream: MemoryDream, expected_status: DreamStatus
                ) -> bool:
                    return await repository.save_fenced(
                        dream,
                        lease_key=lease_key,
                        owner_id=owner_id,
                        expected_status=expected_status,
                    )

                await self._service_factory(session).execute(
                    task_id, execution_fence=fenced_save
                )

        try:
            await run_while_lease_valid(
                execute_dream,
                lease_lost=lease_lost,
                task_name=f"aniu-memory-dream-{task_id}",
            )
        finally:
            stop_renewal.set()
            renewal_task.cancel()
            with suppress(asyncio.CancelledError):
                await renewal_task
            async with self._session_factory() as session:
                await TaskLeaseRepository(session).release(
                    lease_key=lease_key, owner_id=owner_id
                )
                await session.commit()

    async def _renew_lease(
        self,
        lease_key: str,
        owner_id: str,
        stop: asyncio.Event,
        lease_lost: asyncio.Event,
    ) -> None:
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=DREAM_LEASE_RENEW_SECONDS)
                return
            except TimeoutError:
                pass
            try:
                async with self._session_factory() as session:
                    renewed = await TaskLeaseRepository(session).renew(
                        lease_key=lease_key,
                        owner_id=owner_id,
                        lease_seconds=DREAM_LEASE_SECONDS,
                    )
                    await session.commit()
                if not renewed:
                    logger.warning(
                        "memory dream lease was lost",
                        extra={"lease_key": lease_key},
                    )
                    lease_lost.set()
                    return
            except Exception:
                logger.exception(
                    "failed to renew memory dream lease",
                    extra={"lease_key": lease_key},
                )
                lease_lost.set()
                return

    def _lease_key(self, task_id: int) -> str:
        return f"memory-dream:{task_id}"


__all__ = ["MemoryDreamWorker"]
