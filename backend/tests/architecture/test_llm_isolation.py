"""Executable isolation check for the LLM bottom layer."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_llm_package_and_tests_run_without_upper_layers(tmp_path: Path) -> None:
    backend_root = tmp_path / "backend"
    tests_root = tmp_path / "tests"
    backend_root.mkdir()
    tests_root.mkdir()
    (backend_root / "__init__.py").write_text(
        '"""Isolated package root."""\n', encoding="utf-8"
    )

    shutil.copytree(REPO_ROOT / "backend" / "llm", backend_root / "llm")
    shutil.copytree(REPO_ROOT / "backend" / "tests" / "llm", tests_root / "llm")
    shutil.copy2(REPO_ROOT / "backend" / "tests" / "conftest.py", tests_root)

    env = dict(os.environ)
    env["PYTHONPATH"] = str(tmp_path)
    env["PYTHONNOUSERSITE"] = "1"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "no:cacheprovider",
            "-c",
            "/dev/null",
            "tests/llm",
            "-q",
        ],
        cwd=tmp_path,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, result.stdout + result.stderr
