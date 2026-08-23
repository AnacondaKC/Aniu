"""Tests for installation-local secret encryption and key rotation."""

from __future__ import annotations

from pathlib import Path

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import select

from backend.infra.db.models import SecretStoreModel
from backend.infra.repositories import SecretStoreRepository
from backend.infra.security import SecretCodec


def test_secret_codec_enforces_key_and_directory_permissions(tmp_path: Path) -> None:
    key_path = tmp_path / "private" / "master.key"
    codec = SecretCodec(key_path)

    encrypted = codec.encrypt("top-secret")

    assert codec.decrypt(encrypted) == "top-secret"
    assert key_path.stat().st_mode & 0o777 == 0o600
    assert key_path.parent.stat().st_mode & 0o777 == 0o700


def test_secret_codec_supports_previous_key_during_rotation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    old_key = Fernet.generate_key()
    new_key = Fernet.generate_key()
    old_token = Fernet(old_key).encrypt(b"old-secret").decode("ascii")
    monkeypatch.setenv("ANIU_MASTER_SECRET_KEY", new_key.decode("ascii"))
    monkeypatch.setenv(
        "ANIU_MASTER_SECRET_KEY_PREVIOUS",
        old_key.decode("ascii"),
    )

    codec = SecretCodec(tmp_path / "unused.key")

    assert codec.decrypt(old_token) == "old-secret"
    new_token = codec.encrypt("new-secret")
    assert Fernet(new_key).decrypt(new_token.encode("ascii")) == b"new-secret"


@pytest.mark.asyncio
async def test_secret_store_rotates_rows_to_active_key(
    session,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    old_key = Fernet.generate_key()
    new_key = Fernet.generate_key()
    monkeypatch.setenv("ANIU_MASTER_SECRET_KEY", old_key.decode("ascii"))
    monkeypatch.delenv("ANIU_MASTER_SECRET_KEY_PREVIOUS", raising=False)
    old_repo = SecretStoreRepository(
        session,
        secret_codec=SecretCodec(tmp_path / "old.key"),
    )
    await old_repo.set_secret("test", "owner", "token", "rotate-me")
    await session.commit()

    monkeypatch.setenv("ANIU_MASTER_SECRET_KEY", new_key.decode("ascii"))
    monkeypatch.setenv(
        "ANIU_MASTER_SECRET_KEY_PREVIOUS",
        old_key.decode("ascii"),
    )
    rotated_repo = SecretStoreRepository(
        session,
        secret_codec=SecretCodec(tmp_path / "new.key"),
    )
    count = await rotated_repo.rotate_all(key_version="local-v2")
    await session.commit()

    row = await session.scalar(select(SecretStoreModel))
    assert count == 1
    assert row is not None
    assert row.key_version == "local-v2"
    assert Fernet(new_key).decrypt(row.encrypted_value.encode("ascii")) == b"rotate-me"
