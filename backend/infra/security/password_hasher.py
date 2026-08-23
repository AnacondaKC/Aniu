"""Argon2 password hashing via pwdlib."""

from __future__ import annotations

from pwdlib import PasswordHash

_HASHER = PasswordHash.recommended()


def hash_password(password: str) -> str:
    if not password or len(password) < 8:
        raise ValueError("password must be at least 8 characters")
    return _HASHER.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    if not password or not password_hash:
        return False
    return _HASHER.verify(password, password_hash)
