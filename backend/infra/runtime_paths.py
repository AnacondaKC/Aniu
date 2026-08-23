"""Paths for Aniu's installation-local runtime state."""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RUNTIME_DIR = PROJECT_ROOT / ".aniu"


def resolve_runtime_dir() -> Path:
    """Return the directory used for the local database, key, and logs."""

    configured = os.getenv("ANIU_DATA_DIR", "").strip()
    if not configured:
        return DEFAULT_RUNTIME_DIR
    return Path(configured).expanduser().resolve()


def default_database_path() -> Path:
    """Return the default SQLite database path."""

    return resolve_runtime_dir() / "aniu.sqlite3"


def default_secret_key_path() -> Path:
    """Return the default installation secret key path."""

    return resolve_runtime_dir() / ".aniu-secret-key"


def default_log_file() -> Path:
    """Return the default rotating log path."""

    return resolve_runtime_dir() / "logs" / "aniu.log"


__all__ = [
    "DEFAULT_RUNTIME_DIR",
    "PROJECT_ROOT",
    "default_database_path",
    "default_log_file",
    "default_secret_key_path",
    "resolve_runtime_dir",
]
