"""Provider-neutral cancellation primitives."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from contextlib import suppress
from typing import Protocol


class AbortSignal(Protocol):
    @property
    def aborted(self) -> bool: ...

    def throw_if_aborted(self) -> None: ...

    async def wait(self) -> None: ...


def throw_if_aborted(signal: AbortSignal | None) -> None:
    if signal is not None:
        signal.throw_if_aborted()


async def await_with_abort[T](
    operation: Awaitable[T],
    signal: AbortSignal | None,
) -> T:
    """Cancel an in-flight SDK/HTTP awaitable as soon as the signal fires."""

    if signal is None:
        return await operation
    signal.throw_if_aborted()
    operation_task = asyncio.ensure_future(operation)
    abort_task = asyncio.create_task(signal.wait())
    try:
        done, _ = await asyncio.wait(
            {operation_task, abort_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if operation_task in done:
            abort_task.cancel()
            with suppress(asyncio.CancelledError):
                await abort_task
            return operation_task.result()

        operation_task.cancel()
        with suppress(asyncio.CancelledError):
            await operation_task
        signal.throw_if_aborted()
        raise asyncio.CancelledError("LLM operation aborted")
    finally:
        if not abort_task.done():
            abort_task.cancel()
        if not operation_task.done():
            operation_task.cancel()
        await asyncio.gather(
            abort_task,
            operation_task,
            return_exceptions=True,
        )


__all__ = ["AbortSignal", "await_with_abort", "throw_if_aborted"]
