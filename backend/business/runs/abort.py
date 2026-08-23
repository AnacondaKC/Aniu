"""Cooperative abort signal for agent runtime work."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from backend.business.shared import RunAbortError


@dataclass(slots=True)
class RunAbortSignal:
    """Shared signal checked by long-running agent steps."""

    run_id: int
    reason: str | None = None
    _aborted: bool = False
    _event: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def aborted(self) -> bool:
        return self._aborted

    def abort(self, reason: str | None = None) -> None:
        self.reason = reason or "aborted"
        self._aborted = True
        self._event.set()

    async def wait(self) -> None:
        await self._event.wait()

    def throw_if_aborted(self) -> None:
        if self._aborted:
            raise RunAbortError(self.run_id)
