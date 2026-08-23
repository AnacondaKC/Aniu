"""Local-identity login rate-limit tests."""

from __future__ import annotations

import pytest

from backend.business.auth import AuthSession, LocalIdentity
from backend.business.auth.service import (
    AuthAppService,
    AuthLoginThrottle,
    UnauthorizedError,
)
from backend.infra.security.password_hasher import hash_password, verify_password


class FakeIdentity:
    def __init__(self, identity: LocalIdentity) -> None:
        self.identity = identity

    async def get(self) -> LocalIdentity | None:
        return self.identity

    async def initialize(self, **kwargs):  # pragma: no cover
        raise AssertionError("not used")


class FakeSessions:
    async def create(self, session: AuthSession) -> AuthSession:
        session.session_id = 1
        return session

    async def get_by_token_hash(self, token_hash: str):  # pragma: no cover
        return None

    async def touch(self, session_id: int, *, expires_at) -> None:  # pragma: no cover
        return None

    async def revoke(self, session_id: int) -> None:  # pragma: no cover
        return None


@pytest.mark.asyncio
async def test_login_locks_after_repeated_failures() -> None:
    identity = LocalIdentity(
        username="aniu",
        password_hash=hash_password("correct-password"),
    )
    service = AuthAppService(
        identity_repo=FakeIdentity(identity),
        session_repo=FakeSessions(),
        password_hasher=hash_password,
        password_verifier=verify_password,
        login_throttle=AuthLoginThrottle(),
    )
    for _ in range(5):
        with pytest.raises(UnauthorizedError):
            await service.login("aniu", "wrong")
    with pytest.raises(UnauthorizedError, match="too many failed login attempts"):
        await service.login("aniu", "wrong")


@pytest.mark.asyncio
async def test_wrong_username_shares_the_single_identity_lockout() -> None:
    identity = LocalIdentity(
        username="aniu",
        password_hash=hash_password("correct-password"),
    )
    service = AuthAppService(
        identity_repo=FakeIdentity(identity),
        session_repo=FakeSessions(),
        password_hasher=hash_password,
        password_verifier=verify_password,
        login_throttle=AuthLoginThrottle(),
    )
    for attempt in range(5):
        with pytest.raises(UnauthorizedError):
            await service.login(f"not-aniu-{attempt}", "wrong")
    with pytest.raises(UnauthorizedError, match="too many failed login attempts"):
        await service.login("aniu", "correct-password")
