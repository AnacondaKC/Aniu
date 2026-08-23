"""Cancellation protocol owned by the public stock-data boundary."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from contextlib import suppress
from typing import Protocol


class CancellationToken(Protocol):
    """Structural cancellation contract accepted from the Agent runtime."""

    @property
    def aborted(self) -> bool: ...

    def throw_if_aborted(self) -> None: ...

    async def wait(self) -> None: ...


def throw_if_cancelled(token: CancellationToken | None) -> None:
    if token is not None:
        token.throw_if_aborted()


async def await_with_cancellation[T](
    operation: Awaitable[T],
    token: CancellationToken | None,
) -> T:
    """Stop an in-flight HTTP wait promptly when the caller aborts."""

    if token is None:
        return await operation
    token.throw_if_aborted()
    operation_task = asyncio.ensure_future(operation)
    cancellation_task = asyncio.create_task(token.wait())
    try:
        done, _ = await asyncio.wait(
            {operation_task, cancellation_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if operation_task in done:
            cancellation_task.cancel()
            with suppress(asyncio.CancelledError):
                await cancellation_task
            return operation_task.result()

        operation_task.cancel()
        with suppress(asyncio.CancelledError):
            await operation_task
        token.throw_if_aborted()
        raise asyncio.CancelledError("public stock request aborted")
    finally:
        if not cancellation_task.done():
            cancellation_task.cancel()
        if not operation_task.done():
            operation_task.cancel()
        await asyncio.gather(
            cancellation_task,
            operation_task,
            return_exceptions=True,
        )


__all__ = [
    "CancellationToken",
    "await_with_cancellation",
    "throw_if_cancelled",
]
