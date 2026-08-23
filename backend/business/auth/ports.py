"""Ports for local-identity authentication persistence."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from backend.business.auth.models import AuthSession, LocalIdentity


class LocalIdentityRepositoryPort(Protocol):
    async def get(self) -> LocalIdentity | None: ...

    async def initialize(
        self,
        *,
        username: str,
        password_hash: str,
    ) -> LocalIdentity: ...


class AuthSessionRepositoryPort(Protocol):
    async def create(self, session: AuthSession) -> AuthSession: ...

    async def get_by_token_hash(self, token_hash: str) -> AuthSession | None: ...

    async def touch(self, session_id: int, *, expires_at: datetime) -> None: ...

    async def revoke(self, session_id: int) -> None: ...
