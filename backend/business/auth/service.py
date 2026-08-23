"""Authentication use cases for one local identity."""

from __future__ import annotations

import hashlib
import secrets
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from time import monotonic

from backend.business.auth import AuthSession, LocalIdentity
from backend.business.auth.ports import (
    AuthSessionRepositoryPort,
    LocalIdentityRepositoryPort,
)
from backend.business.shared import DomainError

SESSION_COOKIE = "aniu_session"
SESSION_TTL = timedelta(days=7)
SESSION_TOUCH_INTERVAL = timedelta(hours=1)
CSRF_HEADER = "X-CSRF-Token"
LOGIN_MAX_FAILURES = 5
LOGIN_LOCK_SECONDS = 60.0

PasswordHasher = Callable[[str], str]
PasswordVerifier = Callable[[str, str], bool]


class AuthError(DomainError):
    """Authentication failure."""


class UnauthorizedError(AuthError):
    pass


class ForbiddenError(AuthError):
    pass


@dataclass(frozen=True, slots=True)
class SessionPrincipal:
    identity: LocalIdentity
    session: AuthSession
    csrf_token: str | None = None


@dataclass(frozen=True, slots=True)
class LoginResult:
    principal: SessionPrincipal
    raw_session_token: str
    raw_csrf_token: str


@dataclass(slots=True)
class AuthLoginThrottle:
    """Process-local failure throttle shared by request-scoped auth services."""

    failure_count: int = 0
    lock_until: float = 0.0

    def raise_if_locked(self) -> None:
        remaining = self.lock_until - monotonic()
        if remaining > 0:
            raise UnauthorizedError(
                f"too many failed login attempts; retry in {int(remaining) + 1}s"
            )

    def register_failure(self) -> None:
        self.failure_count += 1
        if self.failure_count >= LOGIN_MAX_FAILURES:
            self.lock_until = monotonic() + LOGIN_LOCK_SECONDS
            self.failure_count = 0

    def reset(self) -> None:
        self.failure_count = 0
        self.lock_until = 0.0


def _hash_token(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _csrf_token(raw_session_token: str) -> str:
    return _hash_token(f"csrf:{raw_session_token}")


class AuthAppService:
    def __init__(
        self,
        *,
        identity_repo: LocalIdentityRepositoryPort,
        session_repo: AuthSessionRepositoryPort,
        password_hasher: PasswordHasher,
        password_verifier: PasswordVerifier,
        login_throttle: AuthLoginThrottle,
        committer: object | None = None,
    ) -> None:
        self._identity = identity_repo
        self._sessions = session_repo
        self._hash_password = password_hasher
        self._verify_password = password_verifier
        self._login_throttle = login_throttle
        self._committer = committer

    async def identity_initialized(self) -> bool:
        """Whether the local identity has been initialized."""

        return await self._identity.get() is not None

    async def setup(self, username: str, password: str) -> LoginResult:
        if await self._identity.get() is not None:
            raise AuthError("identity already initialized")
        username = username.strip()
        if not username:
            raise ValueError("username is required")
        identity = await self._identity.initialize(
            username=username,
            password_hash=self._hash_password(password),
        )
        self._login_throttle.reset()
        result = await self._create_session(identity)
        await self._commit()
        return result

    async def login(self, username: str, password: str) -> LoginResult:
        identity = await self._identity.get()
        if identity is None:
            raise UnauthorizedError("invalid username or password")
        self._login_throttle.raise_if_locked()
        supplied_username = username.strip()
        if not secrets.compare_digest(
            supplied_username, identity.username
        ) or not self._verify_password(password, identity.password_hash):
            self._login_throttle.register_failure()
            raise UnauthorizedError("invalid username or password")
        self._login_throttle.reset()

        result = await self._create_session(identity)
        await self._commit()
        return result

    async def resolve_session(
        self,
        raw_session_token: str | None,
    ) -> SessionPrincipal | None:
        if not raw_session_token:
            return None
        session = await self._sessions.get_by_token_hash(_hash_token(raw_session_token))
        if session is None:
            return None
        now = datetime.now(tz=UTC)
        if session.expires_at <= now:
            return None
        identity = await self._identity.get()
        if identity is None:
            return None
        if now - session.last_seen_at >= SESSION_TOUCH_INTERVAL:
            await self._sessions.touch(
                session.session_id,
                expires_at=now + SESSION_TTL,
            )
            await self._commit()
        return SessionPrincipal(
            identity=identity,
            session=session,
            csrf_token=_csrf_token(raw_session_token),
        )

    async def logout(self, raw_session_token: str | None) -> None:
        if not raw_session_token:
            return
        session = await self._sessions.get_by_token_hash(_hash_token(raw_session_token))
        if session is None:
            return
        await self._sessions.revoke(session.session_id)
        await self._commit()

    def verify_csrf(
        self,
        principal: SessionPrincipal,
        csrf_header: str | None,
    ) -> None:
        if not csrf_header:
            raise ForbiddenError("missing CSRF token")
        expected = principal.session.csrf_token_hash
        if not secrets.compare_digest(_hash_token(csrf_header), expected):
            raise ForbiddenError("invalid CSRF token")

    async def _create_session(self, identity: LocalIdentity) -> LoginResult:
        raw_session = secrets.token_urlsafe(32)
        raw_csrf = _csrf_token(raw_session)
        now = datetime.now(tz=UTC)
        session = await self._sessions.create(
            AuthSession(
                token_hash=_hash_token(raw_session),
                csrf_token_hash=_hash_token(raw_csrf),
                expires_at=now + SESSION_TTL,
                last_seen_at=now,
            )
        )
        return LoginResult(
            principal=SessionPrincipal(
                identity=identity,
                session=session,
                csrf_token=raw_csrf,
            ),
            raw_session_token=raw_session,
            raw_csrf_token=raw_csrf,
        )

    async def _commit(self) -> None:
        if self._committer is None:
            return
        commit = getattr(self._committer, "commit", None)
        if callable(commit):
            await commit()
