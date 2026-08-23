"""Typed process configuration loaded once at the composition root."""

from __future__ import annotations

import ipaddress
import os
import re
import socket
from dataclasses import dataclass
from pathlib import Path

from backend.business.auth.token_policy import normalize_token
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
_HOST_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")


def normalize_allowed_hosts(hosts: tuple[str, ...]) -> tuple[str, ...]:
    """Normalize TrustedHost entries and permit only a standalone wildcard."""

    normalized: list[str] = []
    for raw_host in hosts:
        host = raw_host.strip().lower().rstrip(".")
        if not host:
            raise ValueError("allowed_hosts must not contain empty entries")
        if host == "*":
            return ("*",)
        if "*" in host:
            raise ValueError("allowed_hosts only permits '*' as a standalone value")
        try:
            host = str(ipaddress.ip_address(host))
        except ValueError:
            labels = host.split(".")
            if len(host) > 253 or any(
                not _HOST_LABEL.fullmatch(label) for label in labels
            ):
                raise ValueError(f"invalid allowed host: {raw_host}")
        if host not in normalized:
            normalized.append(host)
    return tuple(normalized)


def _discover_lan_ipv4_addresses() -> tuple[str, ...]:
    """Discover local non-public IPv4 addresses without sending network traffic."""

    candidates: list[str] = []
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
            connection.connect(("8.8.8.8", 80))
            candidates.append(connection.getsockname()[0])
    except OSError:
        pass
    try:
        candidates.extend(socket.gethostbyname_ex(socket.gethostname())[2])
    except OSError:
        pass

    addresses: list[str] = []
    for candidate in candidates:
        try:
            address = ipaddress.IPv4Address(candidate)
        except ipaddress.AddressValueError:
            continue
        if (
            address.is_loopback
            or address.is_unspecified
            or address.is_multicast
            or address.is_global
        ):
            continue
        rendered = str(address)
        if rendered not in addresses:
            addresses.append(rendered)
    return tuple(addresses)


def _with_loopback_hosts(hosts: tuple[str, ...]) -> tuple[str, ...]:
    """Ensure loopback hosts remain accepted for local probes and proxies."""

    merged = list(hosts)
    for host in _LOOPBACK_HOSTS:
        if host not in merged:
            merged.append(host)
    return tuple(merged)


def default_lan_allowed_hosts() -> tuple[str, ...]:
    """Return local LAN addresses plus loopback hosts for explicit LAN mode."""

    return _with_loopback_hosts(_discover_lan_ipv4_addresses())


def _allowed_hosts_from_env() -> tuple[str, ...] | None:
    value = os.getenv("ANIU_ALLOWED_HOSTS")
    if value is None:
        return None
    return normalize_allowed_hosts(tuple(value.split(",")))


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    """Infrastructure/runtime settings with one canonical source of defaults."""

    database_url: str | None = None
    # Optional deployment-wide login token. When omitted, the first local token
    # is stored as an Argon2 hash through the loopback-only setup flow.
    auth_token: str | None = None
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
        normalized_hosts = normalize_allowed_hosts(self.allowed_hosts)
        if not normalized_hosts:
            raise ValueError("allowed_hosts must not be empty")
        object.__setattr__(self, "allowed_hosts", normalized_hosts)
        if "*" in self.cors_origins:
            raise ValueError("credentialed CORS cannot use wildcard origin")
        if self.auth_token is not None:
            object.__setattr__(self, "auth_token", normalize_token(self.auth_token))
        if self.llm_read_timeout_seconds <= 0:
            raise ValueError("llm_read_timeout_seconds must be positive")
        if self.lan_mode:
            non_loopback = tuple(
                host for host in self.allowed_hosts if host not in _LOOPBACK_HOSTS
            )
            if not non_loopback:
                raise ValueError("LAN mode requires a non-loopback allowed host")

    @classmethod
    def from_env(cls) -> RuntimeConfig:
        lan_mode = env_flag("ANIU_LAN", default=False)
        configured_allowed_hosts = _allowed_hosts_from_env()
        allowed_hosts = configured_allowed_hosts or (
            default_lan_allowed_hosts()
            if lan_mode
            else ("localhost", "127.0.0.1", "test")
        )
        if lan_mode:
            # Keep loopback for /health probes, Vite proxy, and local tooling even
            # when LAN hosts are configured explicitly.
            allowed_hosts = _with_loopback_hosts(allowed_hosts)
        configured_auth_token = os.getenv("ANIU_AUTH_TOKEN")
        return cls(
            database_url=os.getenv("ANIU_DATABASE_URL") or None,
            auth_token=(configured_auth_token.strip() or None)
            if configured_auth_token is not None
            else None,
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


__all__ = [
    "RuntimeConfig",
    "default_lan_allowed_hosts",
    "env_flag",
    "normalize_allowed_hosts",
]
