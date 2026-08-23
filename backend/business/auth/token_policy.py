"""Shared validation rules for browser login tokens."""

from __future__ import annotations

TOKEN_MIN_LENGTH = 8
TOKEN_MAX_LENGTH = 4096


def normalize_token(token: str) -> str:
    """Trim and validate a token before it becomes an authentication credential."""

    normalized = token.strip()
    if len(normalized) < TOKEN_MIN_LENGTH:
        raise ValueError(f"token must be at least {TOKEN_MIN_LENGTH} characters")
    if len(normalized) > TOKEN_MAX_LENGTH:
        raise ValueError(f"token must not exceed {TOKEN_MAX_LENGTH} characters")
    return normalized
