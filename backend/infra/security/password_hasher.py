"""Argon2 password hashing via pwdlib."""

from __future__ import annotations

from pwdlib import PasswordHash

from backend.business.auth.token_policy import TOKEN_MIN_LENGTH

_HASHER = PasswordHash.recommended()


def hash_password(password: str) -> str:
    if not password or len(password) < TOKEN_MIN_LENGTH:
        raise ValueError(f"password must be at least {TOKEN_MIN_LENGTH} characters")
    return _HASHER.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    if not password or not password_hash:
        return False
    return _HASHER.verify(password, password_hash)
