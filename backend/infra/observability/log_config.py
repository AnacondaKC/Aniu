"""One structured logging configuration for the application and Uvicorn."""

from __future__ import annotations

import json
import logging
import os
import re
import stat
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
from io import TextIOWrapper
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, cast

from backend.infra.runtime_paths import default_log_file

DEFAULT_LOG_FILE = default_log_file()
DEFAULT_LOG_MAX_BYTES = 10 * 1024 * 1024
DEFAULT_LOG_BACKUP_COUNT = 5
PRIVATE_LOG_FILE_MODE = 0o600
PRIVATE_LOG_DIRECTORY_MODE = 0o700
PRIVATE_LOG_OPEN_FLAGS = (
    os.O_CREAT | os.O_APPEND | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0)
)
REDACTED = "[REDACTED]"

SENSITIVE_KEYS = frozenset(
    {
        "authorization",
        "cookie",
        "set_cookie",
        "x_api_key",
        "api_key",
        "apikey",
        "password",
        "passwd",
        "secret",
        "client_secret",
        "credential",
        "credentials",
        "token",
        "access_token",
        "refresh_token",
        "auth_token",
    }
)
SENSITIVE_KEY_SUFFIXES = (
    "_api_key",
    "_password",
    "_passwd",
    "_secret",
    "_credential",
    "_token",
)
STRUCTURED_FIELDS = (
    "request_id",
    "user_id",
    "run_id",
    "job_id",
    "schedule_id",
    "worker_id",
    "event_seq",
    "stage_id",
    "step_id",
    "tool_call_id",
    "provider",
    "model",
    "protocol",
    "llm_mode",
    "duration_ms",
    "error_code",
    "attempt",
    "max_attempts",
    "retry_count",
    "input_bytes",
    "output_bytes",
    "system_prompt_bytes",
    "user_prompt_bytes",
    "message_count",
    "tool_definition_count",
    "tool_call_count",
    "max_output_tokens",
    "status",
    "trigger_source",
    "current_state",
)
HEALTH_ACCESS_PATHS = frozenset({"/health", "/health/live"})
LOG_RECORD_BUILTINS = frozenset(logging.makeLogRecord({}).__dict__)

BEARER_PATTERN = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+")
OPENAI_KEY_PATTERN = re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b")
TEXT_SENSITIVE_KEY_PATTERN = (
    r"authorization|cookie|set[-_]cookie|x[-_]api[-_]key|client[-_]secret|"
    r"access[-_]token|refresh[-_]token|auth[-_]token|session[-_]token|id[-_]token|"
    r"(?:[a-z0-9]+_)*(?:api[_-]?key|password|passwd|secret|credential|token)"
)
SENSITIVE_ASSIGNMENT_PREFIX = (
    rf"(?P<key_quote>(?:[\\]?[\"'])?)(?P<key>\b(?:{TEXT_SENSITIVE_KEY_PATTERN})\b)"
    rf"(?P=key_quote)(?P<separator>\s*[:=]\s*)"
)
AUTHORIZATION_HEADER_PATTERN = re.compile(
    r"(?i)\b(authorization)(\s*[:=]\s*)([^\r\n,]+)"
)
COOKIE_HEADER_PATTERN = re.compile(r"(?i)\b(cookie|set-cookie)(\s*[:=]\s*)([^\r\n]+)")
SENSITIVE_ASSIGNMENT_START_PATTERN = re.compile(rf"(?i){SENSITIVE_ASSIGNMENT_PREFIX}")
UNQUOTED_SECRET_DELIMITERS = frozenset(" \t\r\n,;&")


def _normalize_key(key: object) -> str:
    return str(key).strip().lower().replace("-", "_")


def _is_sensitive_key(key: object) -> bool:
    normalized = _normalize_key(key)
    return normalized in SENSITIVE_KEYS or normalized.endswith(SENSITIVE_KEY_SUFFIXES)


def _quoted_value_end(value: str, start: int, quote: str) -> int | None:
    index = start
    while index < len(value):
        if value[index] == "\\":
            index += 2
            continue
        if value[index] == quote:
            return index
        index += 1
    return None


def _redact_sensitive_assignments(value: str) -> str:
    parts: list[str] = []
    cursor = 0
    for match in SENSITIVE_ASSIGNMENT_START_PATTERN.finditer(value):
        if match.start() < cursor:
            continue
        value_start = match.end()
        parts.append(value[cursor:value_start])
        if value_start >= len(value):
            parts.append(REDACTED)
            cursor = value_start
            continue

        quote = value[value_start]
        if quote in {'"', "'"}:
            value_end = _quoted_value_end(value, value_start + 1, quote)
            parts.append(f"{quote}{REDACTED}")
            if value_end is None:
                cursor = len(value)
                break
            parts.append(quote)
            cursor = value_end + 1
            continue

        value_end = value_start
        while (
            value_end < len(value)
            and value[value_end] not in UNQUOTED_SECRET_DELIMITERS
        ):
            value_end += 1
        parts.append(REDACTED)
        cursor = value_end

    parts.append(value[cursor:])
    return "".join(parts)


def redact_text(value: str) -> str:
    """Redact common secret shapes without changing ordinary token counters."""

    redacted = AUTHORIZATION_HEADER_PATTERN.sub(
        lambda match: f"{match.group(1)}{match.group(2)}{REDACTED}",
        value,
    )
    redacted = COOKIE_HEADER_PATTERN.sub(
        lambda match: f"{match.group(1)}{match.group(2)}{REDACTED}",
        redacted,
    )
    redacted = _redact_sensitive_assignments(redacted)
    redacted = BEARER_PATTERN.sub(f"Bearer {REDACTED}", redacted)
    return OPENAI_KEY_PATTERN.sub(REDACTED, redacted)


def _open_private_log_descriptor(path: str | Path) -> int:
    descriptor = os.open(path, PRIVATE_LOG_OPEN_FLAGS, PRIVATE_LOG_FILE_MODE)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OSError("log path is not a regular file")
        os.fchmod(descriptor, PRIVATE_LOG_FILE_MODE)
    except Exception:
        os.close(descriptor)
        raise
    return descriptor


class PrivateRotatingFileHandler(RotatingFileHandler):
    """Keep every newly created rollover file private, not only the first one."""

    def _open(self) -> TextIOWrapper:
        descriptor = _open_private_log_descriptor(self.baseFilename)
        try:
            return cast(
                TextIOWrapper,
                open(
                    descriptor,
                    self.mode,
                    encoding=self.encoding,
                    errors=self.errors,
                    closefd=True,
                ),
            )
        except Exception:
            try:
                os.close(descriptor)
            except OSError:
                pass
            raise


def redact_value(value: object, *, key: object | None = None) -> object:
    if key is not None and _is_sensitive_key(key):
        return REDACTED
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, dict):
        return {
            str(item_key): redact_value(item_value, key=item_key)
            for item_key, item_value in value.items()
        }
    if isinstance(value, tuple):
        return tuple(redact_value(item) for item in value)
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    if isinstance(value, set):
        return [redact_value(item) for item in sorted(value, key=str)]
    return value


class RedactionFilter(logging.Filter):
    """Apply the same secret redaction policy to every handler."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not isinstance(record.msg, str):
            record.msg = redact_value(record.msg)
        if isinstance(record.args, dict):
            record.args = {
                key: redact_value(value, key=key) for key, value in record.args.items()
            }
        elif isinstance(record.args, tuple):
            record.args = tuple(redact_value(value) for value in record.args)
        for key, value in tuple(record.__dict__.items()):
            if key not in LOG_RECORD_BUILTINS:
                setattr(record, key, redact_value(value, key=key))
        return True


class HealthAccessFilter(logging.Filter):
    """Drop successful high-frequency liveness access logs, but retain failures."""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name != "uvicorn.access":
            return True
        args = record.args
        if not isinstance(args, tuple) or len(args) < 5:
            return True
        path = str(args[2]).split("?", 1)[0]
        status_code = args[4]
        return not (
            path in HEALTH_ACCESS_PATHS
            and isinstance(status_code, int)
            and 200 <= status_code < 300
        )


def _json_default(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, BaseException):
        return redact_text(str(value))
    return str(value)


class StructuredJsonFormatter(logging.Formatter):
    """Emit one JSON object per line with a stable field contract."""

    def format(self, record: logging.LogRecord) -> str:
        message = redact_text(record.getMessage())
        payload: dict[str, object] = {
            "timestamp": datetime.now(tz=UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": message,
        }
        if record.name == "uvicorn.access":
            args = record.args
            if isinstance(args, tuple) and len(args) >= 5:
                payload.update(
                    {
                        "event": "http_access",
                        "client": str(args[0]),
                        "method": str(args[1]),
                        "path": redact_text(str(args[2])),
                        "http_version": str(args[3]),
                        "status_code": args[4],
                    }
                )
        for field in STRUCTURED_FIELDS:
            if hasattr(record, field):
                payload[field] = redact_value(getattr(record, field), key=field)
        if record.exc_info:
            payload["exception"] = redact_text(self.formatException(record.exc_info))
        if record.stack_info:
            payload["stack"] = redact_text(self.formatStack(record.stack_info))
        return json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            default=_json_default,
        )


@dataclass(frozen=True, slots=True)
class LoggingSettings:
    level: str
    file_path: Path | None
    max_bytes: int = DEFAULT_LOG_MAX_BYTES
    backup_count: int = DEFAULT_LOG_BACKUP_COUNT

    @classmethod
    def from_env(cls, *, level: str) -> LoggingSettings:
        raw_file = os.getenv("ANIU_LOG_FILE", str(default_log_file())).strip()
        file_path = None if raw_file.lower() in {"", "off", "none"} else Path(raw_file)
        return cls(
            level=level.upper(),
            file_path=(
                None if file_path is None else file_path.expanduser().absolute()
            ),
            max_bytes=_positive_env_int("ANIU_LOG_MAX_BYTES", DEFAULT_LOG_MAX_BYTES),
            backup_count=_positive_env_int(
                "ANIU_LOG_BACKUP_COUNT", DEFAULT_LOG_BACKUP_COUNT
            ),
        )


def _positive_env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 1:
        raise ValueError(f"{name} must be >= 1")
    return value


def _prepare_log_file(path: Path) -> Path | None:
    try:
        path.parent.mkdir(
            parents=True,
            exist_ok=True,
            mode=PRIVATE_LOG_DIRECTORY_MODE,
        )
        descriptor = _open_private_log_descriptor(path)
        os.close(descriptor)
        return path
    except OSError as exc:
        warning = {
            "timestamp": datetime.now(tz=UTC).isoformat(timespec="milliseconds"),
            "level": "WARNING",
            "logger": "backend.infra.observability",
            "message": "rotating log file disabled",
            "error_code": "LOG_FILE_UNAVAILABLE",
            "error": redact_text(str(exc)),
        }
        print(
            json.dumps(warning, ensure_ascii=False, separators=(",", ":")),
            file=sys.stderr,
        )
        return None


def build_logging_config(settings: LoggingSettings) -> dict[str, Any]:
    """Build a dictConfig consumed by Uvicorn before application startup."""

    handlers: dict[str, dict[str, object]] = {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "structured",
            "filters": ["redaction", "health_access"],
            "stream": "ext://sys.stdout",
        }
    }
    handler_names = ["console"]
    if settings.file_path is not None:
        file_path = _prepare_log_file(settings.file_path)
        if file_path is not None:
            handlers["rotating_file"] = {
                "class": (
                    "backend.infra.observability.log_config.PrivateRotatingFileHandler"
                ),
                "formatter": "structured",
                "filters": ["redaction", "health_access"],
                "filename": str(file_path),
                "maxBytes": settings.max_bytes,
                "backupCount": settings.backup_count,
                "encoding": "utf-8",
            }
            handler_names.append("rotating_file")

    return {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {
            "redaction": {"()": RedactionFilter},
            "health_access": {"()": HealthAccessFilter},
        },
        "formatters": {
            "structured": {"()": StructuredJsonFormatter},
        },
        "handlers": handlers,
        "loggers": {
            "uvicorn": {
                "handlers": handler_names,
                "level": settings.level,
                "propagate": False,
            },
            "uvicorn.access": {
                "handlers": handler_names,
                "level": settings.level,
                "propagate": False,
            },
        },
        "root": {
            "handlers": handler_names,
            "level": settings.level,
        },
    }


__all__ = [
    "HealthAccessFilter",
    "LoggingSettings",
    "PrivateRotatingFileHandler",
    "RedactionFilter",
    "StructuredJsonFormatter",
    "build_logging_config",
    "redact_text",
    "redact_value",
]
