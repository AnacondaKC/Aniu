"""Shared transaction boundary port."""

from __future__ import annotations

from typing import Protocol


class CommitterPort(Protocol):
    async def commit(self) -> None: ...
