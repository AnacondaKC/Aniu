"""Cancel asynchronous work when its database lease is lost."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress


class LeaseLostError(RuntimeError):
    """Raised after leased work is cancelled because ownership was lost."""


async def run_while_lease_valid(
    operation: Callable[[], Awaitable[None]],
    *,
    lease_lost: asyncio.Event,
    task_name: str,
) -> None:
    if lease_lost.is_set():
        raise LeaseLostError(f"lease was lost before {task_name} started")

    async def run_operation() -> None:
        await operation()

    async def wait_for_lease_loss() -> None:
        await lease_lost.wait()

    operation_task: asyncio.Task[None] = asyncio.create_task(
        run_operation(), name=task_name
    )
    lease_lost_task = asyncio.create_task(
        wait_for_lease_loss(), name=f"{task_name}-lease-lost"
    )
    try:
        done, _ = await asyncio.wait(
            (operation_task, lease_lost_task),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if lease_lost_task in done:
            if not operation_task.done():
                operation_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await operation_task
            raise LeaseLostError(f"lease was lost while {task_name} was running")
        await operation_task
    finally:
        if not operation_task.done():
            operation_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await operation_task
        if not lease_lost_task.done():
            lease_lost_task.cancel()
            with suppress(asyncio.CancelledError):
                await lease_lost_task


__all__ = ["LeaseLostError", "run_while_lease_valid"]
