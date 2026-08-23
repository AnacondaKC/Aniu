"""Tests for local runtime state path selection."""

from __future__ import annotations

from pathlib import Path

import pytest

from backend.infra.db.session import build_database_url
from backend.infra.observability import LoggingSettings
from backend.infra.runtime_paths import (
    DEFAULT_RUNTIME_DIR,
    default_database_path,
    default_log_file,
    default_secret_key_path,
    resolve_runtime_dir,
)
from backend.infra.security import SecretCodec


def test_default_runtime_paths_use_hidden_project_directory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ANIU_DATA_DIR", raising=False)
    monkeypatch.delenv("ANIU_DATABASE_URL", raising=False)
    monkeypatch.delenv("ANIU_LOG_FILE", raising=False)

    assert resolve_runtime_dir() == DEFAULT_RUNTIME_DIR
    assert default_database_path() == DEFAULT_RUNTIME_DIR / "aniu.sqlite3"
    assert default_secret_key_path() == DEFAULT_RUNTIME_DIR / ".aniu-secret-key"
    assert default_log_file() == DEFAULT_RUNTIME_DIR / "logs" / "aniu.log"
    assert build_database_url() == f"sqlite+aiosqlite:///{default_database_path()}"
    assert SecretCodec().key_path == default_secret_key_path()
    assert LoggingSettings.from_env(level="info").file_path == default_log_file()


def test_runtime_paths_allow_an_external_data_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runtime_dir = tmp_path / "outside-repository"
    monkeypatch.setenv("ANIU_DATA_DIR", str(runtime_dir))
    monkeypatch.delenv("ANIU_DATABASE_URL", raising=False)
    monkeypatch.delenv("ANIU_LOG_FILE", raising=False)

    assert resolve_runtime_dir() == runtime_dir
    assert build_database_url() == (
        f"sqlite+aiosqlite:///{runtime_dir / 'aniu.sqlite3'}"
    )
    assert SecretCodec().key_path == runtime_dir / ".aniu-secret-key"
    assert LoggingSettings.from_env(level="info").file_path == (
        runtime_dir / "logs" / "aniu.log"
    )
