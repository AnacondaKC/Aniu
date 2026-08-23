"""Bootstrap runtime configuration tests."""

from __future__ import annotations

import pytest

from backend.bootstrap import runtime_config
from backend.bootstrap.runtime_config import (
    RuntimeConfig,
    env_flag,
    normalize_allowed_hosts,
)


def test_env_flag_strips_values_and_honors_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANIU_TEST_FLAG", " ON ")
    assert env_flag("ANIU_TEST_FLAG") is True

    monkeypatch.setenv("ANIU_TEST_FLAG", "off")
    assert env_flag("ANIU_TEST_FLAG") is False

    monkeypatch.delenv("ANIU_TEST_FLAG")
    assert env_flag("ANIU_TEST_FLAG", default=True) is True


def test_runtime_config_reads_auth_token_and_llm_timeout_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANIU_AUTH_TOKEN", "  configured-token  ")
    monkeypatch.setenv("ANIU_LLM_READ_TIMEOUT_SECONDS", "600")
    config = RuntimeConfig.from_env()
    assert config.auth_token == "configured-token"
    assert config.llm_read_timeout_seconds == 600.0

    monkeypatch.delenv("ANIU_LLM_READ_TIMEOUT_SECONDS", raising=False)
    assert RuntimeConfig.from_env().llm_read_timeout_seconds == 300.0

    with pytest.raises(ValueError, match="llm_read_timeout_seconds"):
        RuntimeConfig(llm_read_timeout_seconds=0)


def test_runtime_config_defaults_to_loopback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ANIU_LAN", raising=False)
    monkeypatch.delenv("ANIU_ALLOWED_HOSTS", raising=False)
    monkeypatch.setattr(
        runtime_config,
        "_discover_lan_ipv4_addresses",
        lambda: ("192.168.1.20",),
    )

    config = RuntimeConfig.from_env()

    assert config.lan_mode is False
    assert config.allowed_hosts == ("localhost", "127.0.0.1", "test")


def test_runtime_config_requires_an_explicit_lan_host_when_discovery_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANIU_LAN", "1")
    monkeypatch.delenv("ANIU_ALLOWED_HOSTS", raising=False)
    monkeypatch.setattr(runtime_config, "_discover_lan_ipv4_addresses", lambda: ())

    with pytest.raises(ValueError, match="non-loopback"):
        RuntimeConfig.from_env()


def test_runtime_config_rejects_short_auth_tokens() -> None:
    with pytest.raises(ValueError, match="at least 8"):
        RuntimeConfig(lan_mode=False, auth_token="short")


@pytest.mark.parametrize(
    "host",
    ("*.example.internal", "https://aniu.internal", "aniu.internal:8000"),
)
def test_allowed_hosts_reject_partial_wildcards_and_non_host_values(host: str) -> None:
    with pytest.raises(ValueError, match="allowed_hosts|invalid allowed host"):
        normalize_allowed_hosts((host,))


def test_allowed_hosts_normalize_standalone_wildcard() -> None:
    assert normalize_allowed_hosts(("aniu.internal", "*", "192.168.1.20")) == ("*",)


def test_runtime_config_reads_wildcard_host_from_lan_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANIU_LAN", "true")
    monkeypatch.setenv("ANIU_ALLOWED_HOSTS", "*")

    config = RuntimeConfig.from_env()

    assert config.lan_mode is True
    assert config.allowed_hosts == ("*",)


def test_allowed_hosts_normalize_exact_hosts() -> None:
    assert normalize_allowed_hosts(
        (" Aniu.Internal. ", "192.168.1.20", "aniu.internal")
    ) == ("aniu.internal", "192.168.1.20")
