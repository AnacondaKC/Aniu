"""Authentication domain entities for one local identity."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


@dataclass(slots=True)
class LocalIdentity:
    """The deployment's single local identity."""

    username: str
    password_hash: str
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class AuthSession:
    """One server-side session belonging to the local identity."""

    token_hash: str
    csrf_token_hash: str
    expires_at: datetime
    credential_fingerprint: str = ""
    last_seen_at: datetime = field(default_factory=utc_now)
    revoked_at: datetime | None = None
    session_id: int = 0
    created_at: datetime = field(default_factory=utc_now)
