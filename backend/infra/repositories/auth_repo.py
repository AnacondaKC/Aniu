"""Repositories for the local identity and server-side sessions."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.business.auth import AuthSession, LocalIdentity
from backend.infra.db.models import AuthSessionModel, LocalIdentityModel, utc_now_iso

LOCAL_IDENTITY_ID = 1


def _dt(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


class LocalIdentityRepository:
    """Persistence adapter for the deployment's singleton identity."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self) -> LocalIdentity | None:
        model = await self._session.get(LocalIdentityModel, LOCAL_IDENTITY_ID)
        return None if model is None else self._to_domain(model)

    async def initialize(
        self,
        *,
        username: str,
        password_hash: str,
    ) -> LocalIdentity:
        if await self.get() is not None:
            raise ValueError("local identity already exists")
        now = utc_now_iso()
        model = LocalIdentityModel(
            id=LOCAL_IDENTITY_ID,
            username=username,
            password_hash=password_hash,
            created_at=now,
            updated_at=now,
        )
        self._session.add(model)
        await self._session.flush()
        return self._to_domain(model)

    def _to_domain(self, model: LocalIdentityModel) -> LocalIdentity:
        return LocalIdentity(
            username=model.username,
            password_hash=model.password_hash,
            created_at=_dt(model.created_at) or datetime.now(tz=UTC),
            updated_at=_dt(model.updated_at) or datetime.now(tz=UTC),
        )


class AuthSessionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, session: AuthSession) -> AuthSession:
        now = utc_now_iso()
        model = AuthSessionModel(
            token_hash=session.token_hash,
            csrf_token_hash=session.csrf_token_hash,
            expires_at=session.expires_at.isoformat(),
            last_seen_at=session.last_seen_at.isoformat(),
            revoked_at=None,
            created_at=now,
        )
        self._session.add(model)
        await self._session.flush()
        return self._to_domain(model)

    async def get_by_token_hash(self, token_hash: str) -> AuthSession | None:
        statement = (
            select(AuthSessionModel)
            .where(
                AuthSessionModel.token_hash == token_hash,
                AuthSessionModel.revoked_at.is_(None),
            )
            .limit(1)
        )
        model = (await self._session.scalars(statement)).first()
        return None if model is None else self._to_domain(model)

    async def touch(self, session_id: int, *, expires_at: datetime) -> None:
        now = utc_now_iso()
        await self._session.execute(
            update(AuthSessionModel)
            .where(AuthSessionModel.id == session_id)
            .values(last_seen_at=now, expires_at=expires_at.isoformat())
        )

    async def revoke(self, session_id: int) -> None:
        now = utc_now_iso()
        await self._session.execute(
            update(AuthSessionModel)
            .where(AuthSessionModel.id == session_id)
            .values(revoked_at=now)
        )

    def _to_domain(self, model: AuthSessionModel) -> AuthSession:
        return AuthSession(
            session_id=model.id,
            token_hash=model.token_hash,
            csrf_token_hash=model.csrf_token_hash,
            expires_at=_dt(model.expires_at) or datetime.now(tz=UTC),
            last_seen_at=_dt(model.last_seen_at) or datetime.now(tz=UTC),
            revoked_at=_dt(model.revoked_at),
            created_at=_dt(model.created_at) or datetime.now(tz=UTC),
        )
