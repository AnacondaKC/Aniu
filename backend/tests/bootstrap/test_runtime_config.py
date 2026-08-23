"""Bootstrap runtime configuration tests."""

from __future__ import annotations

import pytest

from backend.bootstrap.runtime_config import RuntimeConfig, env_flag


def test_env_flag_strips_values_and_honors_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANIU_TEST_FLAG", " ON ")
    assert env_flag("ANIU_TEST_FLAG") is True

    monkeypatch.setenv("ANIU_TEST_FLAG", "off")
    assert env_flag("ANIU_TEST_FLAG") is False

    monkeypatch.delenv("ANIU_TEST_FLAG")
    assert env_flag("ANIU_TEST_FLAG", default=True) is True


def test_runtime_config_reads_llm_timeout_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANIU_LLM_READ_TIMEOUT_SECONDS", "600")
    assert RuntimeConfig.from_env().llm_read_timeout_seconds == 600.0

    monkeypatch.delenv("ANIU_LLM_READ_TIMEOUT_SECONDS", raising=False)
    assert RuntimeConfig.from_env().llm_read_timeout_seconds == 300.0

    with pytest.raises(ValueError, match="llm_read_timeout_seconds"):
        RuntimeConfig(llm_read_timeout_seconds=0)
