"""Aniu 本地/内网启动入口。"""

from __future__ import annotations

import argparse
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

import uvicorn

from backend.bootstrap.runtime_config import (
    default_lan_allowed_hosts,
    env_flag,
    normalize_allowed_hosts,
)
from backend.infra.observability import LoggingSettings, build_logging_config
from backend.infra.runtime_paths import PROJECT_ROOT

BACKEND_DIR = PROJECT_ROOT / "backend"
DEFAULT_RELOAD_DELAY_SECONDS = 0.75


def _logging_settings(*, level: str, reload: bool) -> LoggingSettings:
    settings = LoggingSettings.from_env(level=level)
    if not reload:
        return settings
    # Uvicorn's reloader and worker are separate processes. Standard
    # RotatingFileHandler cannot coordinate rollover across them safely.
    return replace(settings, file_path=None)


def _non_negative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be greater than or equal to zero")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for serving the app."""

    lan_default = env_flag("ANIU_LAN", default=False)
    parser = argparse.ArgumentParser(
        description="Serve Aniu for LAN access or localhost-only access.",
    )
    parser.add_argument(
        "--host",
        default=os.getenv("ANIU_HOST"),
        help=(
            "Bind host. Defaults to ANIU_HOST or 127.0.0.1; use --lan to "
            "default to 0.0.0.0."
        ),
    )
    parser.add_argument(
        "--lan",
        action=argparse.BooleanOptionalAction,
        default=lan_default,
        help="Enable LAN mode and its exact TrustedHost allowlist (default: disabled).",
    )
    parser.add_argument(
        "--allowed-host",
        action="append",
        default=[],
        help="Allowed HTTP Host in LAN mode. Repeat or use comma-separated values.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("ANIU_PORT", "8000")),
        help="Bind port. Defaults to ANIU_PORT or 8000.",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        default=env_flag("ANIU_RELOAD"),
        help="Enable auto reload for development.",
    )
    parser.add_argument(
        "--reload-dir",
        action="append",
        default=[],
        metavar="PATH",
        help=(
            "Directory to watch in reload mode. Repeat for multiple directories. "
            "Defaults to backend."
        ),
    )
    parser.add_argument(
        "--reload-delay",
        type=_non_negative_float,
        default=_non_negative_float(
            os.getenv("ANIU_RELOAD_DELAY", str(DEFAULT_RELOAD_DELAY_SECONDS))
        ),
        metavar="SECONDS",
        help=(
            "Debounce interval for reload file changes. Defaults to "
            "ANIU_RELOAD_DELAY or 0.75 seconds."
        ),
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("ANIU_LOG_LEVEL", "info"),
        help="Uvicorn log level. Defaults to ANIU_LOG_LEVEL or info.",
    )
    parser.add_argument(
        "--data-dir",
        default=os.getenv("ANIU_DATA_DIR"),
        metavar="PATH",
        help=(
            "Directory for the database, secret key, and logs. "
            "Defaults to ANIU_DATA_DIR or .aniu."
        ),
    )
    return parser


def main() -> None:
    """Run the FastAPI application with localhost-first defaults."""

    parser = build_parser()
    args = parser.parse_args()
    if args.host is None:
        args.host = "0.0.0.0" if args.lan else "127.0.0.1"

    if args.lan:
        os.environ["ANIU_LAN"] = "1"
        if args.allowed_host:
            supplied_hosts = tuple(
                host for item in args.allowed_host for host in item.split(",")
            )
            try:
                allowed_hosts = normalize_allowed_hosts(supplied_hosts)
            except ValueError as exc:
                parser.error(str(exc))
            os.environ["ANIU_ALLOWED_HOSTS"] = ",".join(allowed_hosts)
        elif not os.getenv("ANIU_ALLOWED_HOSTS"):
            discovered_hosts = default_lan_allowed_hosts()
            if not any(
                host not in {"localhost", "127.0.0.1", "::1", "test"}
                for host in discovered_hosts
            ):
                parser.error(
                    "LAN mode requires --allowed-host or ANIU_ALLOWED_HOSTS when no "
                    "private IPv4 address is available"
                )
            os.environ["ANIU_ALLOWED_HOSTS"] = ",".join(discovered_hosts)
    else:
        os.environ["ANIU_LAN"] = "0"
    if args.data_dir:
        os.environ["ANIU_DATA_DIR"] = str(Path(args.data_dir).expanduser().resolve())

    logging_settings = _logging_settings(level=args.log_level, reload=args.reload)

    options: dict[str, Any] = {
        "app_dir": str(PROJECT_ROOT),
        "host": args.host,
        "port": args.port,
        "reload": args.reload,
        "log_level": args.log_level,
        "log_config": build_logging_config(logging_settings),
    }
    if args.reload:
        reload_dirs = (
            [Path(path).resolve() for path in args.reload_dir]
            if args.reload_dir
            else [BACKEND_DIR]
        )
        options["reload_dirs"] = [str(path) for path in reload_dirs]
        options["reload_delay"] = args.reload_delay

    uvicorn.run("backend.main:app", **options)


if __name__ == "__main__":
    main()
