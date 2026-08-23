"""Typed process configuration loaded once at the composition root."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from backend.infra.runtime_paths import PROJECT_ROOT

DEFAULT_FRONTEND_DIST = PROJECT_ROOT / "frontend" / "dist"


def env_flag(name: str, *, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _csv_env(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    value = os.getenv(name)
    if value is None:
        return default
    items = tuple(item.strip() for item in value.split(",") if item.strip())
    return items or default


_LOOPBACK_HOSTS: tuple[str, ...] = ("localhost", "127.0.0.1", "test", "::1")


def _with_loopback_hosts(hosts: tuple[str, ...]) -> tuple[str, ...]:
    """Ensure loopback hosts remain accepted for local probes and proxies."""

    merged = list(hosts)
    for host in _LOOPBACK_HOSTS:
        if host not in merged:
            merged.append(host)
    return tuple(merged)


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    """Infrastructure/runtime settings with one canonical source of defaults."""

    database_url: str | None = None
    scheduler_enabled: bool = False
    serve_frontend: bool = True
    lan_mode: bool = False
    allowed_hosts: tuple[str, ...] = ("localhost", "127.0.0.1", "test")
    cors_origins: tuple[str, ...] = (
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    )
    frontend_dist: Path = DEFAULT_FRONTEND_DIST
    # Read window for one LLM response; non-streaming calls need the full
    # generation time. See DEFAULT_LLM_READ_TIMEOUT_SECONDS in llm_client.
    llm_read_timeout_seconds: float = 300.0

    def __post_init__(self) -> None:
        if not self.allowed_hosts:
            raise ValueError("allowed_hosts must not be empty")
        if "*" in self.cors_origins:
            raise ValueError("credentialed CORS cannot use wildcard origin")
        if self.llm_read_timeout_seconds <= 0:
            raise ValueError("llm_read_timeout_seconds must be positive")
        if self.lan_mode:
            non_loopback = tuple(
                host for host in self.allowed_hosts if host not in _LOOPBACK_HOSTS
            )
            if not non_loopback:
                raise ValueError(
                    "LAN mode requires explicit ANIU_ALLOWED_HOSTS (comma-separated)"
                )

    @classmethod
    def from_env(cls) -> RuntimeConfig:
        lan_mode = env_flag("ANIU_LAN", default=False)
        allowed_hosts = _csv_env(
            "ANIU_ALLOWED_HOSTS",
            ("localhost", "127.0.0.1", "test"),
        )
        if lan_mode:
            # Keep loopback for /health probes, Vite proxy, and local tooling even
            # when LAN hosts are configured explicitly.
            allowed_hosts = _with_loopback_hosts(allowed_hosts)
        return cls(
            database_url=os.getenv("ANIU_DATABASE_URL") or None,
            scheduler_enabled=env_flag("ANIU_ENABLE_SCHEDULER", default=False),
            serve_frontend=env_flag("ANIU_SERVE_FRONTEND", default=True),
            lan_mode=lan_mode,
            allowed_hosts=allowed_hosts,
            cors_origins=_csv_env(
                "ANIU_CORS_ORIGINS",
                (
                    "http://localhost:5173",
                    "http://127.0.0.1:5173",
                ),
            ),
            frontend_dist=Path(
                os.getenv("ANIU_FRONTEND_DIST", str(DEFAULT_FRONTEND_DIST))
            ).resolve(),
            llm_read_timeout_seconds=float(
                os.getenv("ANIU_LLM_READ_TIMEOUT_SECONDS", "300")
            ),
        )


__all__ = ["RuntimeConfig", "env_flag"]
