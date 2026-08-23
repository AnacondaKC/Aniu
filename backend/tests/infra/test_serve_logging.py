"""Serve entrypoint logging-mode tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from backend.serve import _logging_settings


def test_reload_disables_process_local_file_rotation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    log_file = tmp_path / "aniu.log"
    monkeypatch.setenv("ANIU_LOG_FILE", str(log_file))

    stable = _logging_settings(level="info", reload=False)
    reloading = _logging_settings(level="info", reload=True)

    assert stable.file_path == log_file.resolve()
    assert reloading.file_path is None
