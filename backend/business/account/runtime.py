"""Process-local refresh frequency protection for account data."""

from __future__ import annotations

from collections.abc import Callable
from time import monotonic

ACCOUNT_REFRESH_COOLDOWN_SECONDS = 10.0


class AccountRefreshGate:
    """Allow at most one account refresh per cooldown window per process."""

    def __init__(
        self,
        now: Callable[[], float] = monotonic,
        cooldown_seconds: float = ACCOUNT_REFRESH_COOLDOWN_SECONDS,
    ) -> None:
        self._now = now
        self._cooldown_seconds = cooldown_seconds
        self._last_allowed_at: float | None = None

    def allow(self) -> bool:
        now = self._now()
        if (
            self._last_allowed_at is not None
            and now - self._last_allowed_at < self._cooldown_seconds
        ):
            return False
        self._last_allowed_at = now
        return True
