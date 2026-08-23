"""Shared throttling and retry policy for MX upstream requests."""

from __future__ import annotations

import asyncio
import json
import math
import re
import time
from dataclasses import dataclass, field

_MX_RATE_LIMIT_MARKERS = (
    "请求频率过高",
    "请求过于频繁",
    "操作过于频繁",
    "too many requests",
    "too frequent",
    "rate limit",
    "rate limited",
    "rate_limit",
)
_MX_DAILY_QUOTA_MARKERS = (
    "调用次数已达到上限",
    "账户已经进入休眠",
    "daily quota",
    "quota exceeded",
)
_MAX_RETRY_AFTER_SECONDS = 10.0
_MX_SENSITIVE_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)\b(?:api[-_]?key|access[-_]?token|auth[-_]?token|token|secret)"
    r"\b(\s*[:=]\s*)([^\s,;}\]]+)"
)
_MX_BEARER_PATTERN = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+")


@dataclass(slots=True)
class MxRequestGate:
    """Serialize MX requests and keep a small gap between request starts."""

    min_interval: float = 0.25
    _lock: asyncio.Lock = field(init=False, default_factory=asyncio.Lock)
    _last_started_at: float | None = field(init=False, default=None)
    _blocked_until: float = field(init=False, default=0.0)
    _owner: asyncio.Task[object] | None = field(init=False, default=None)

    async def __aenter__(self) -> MxRequestGate:
        await self._lock.acquire()
        try:
            now = time.monotonic()
            delay = max(
                self._blocked_until - now,
                0.0
                if self._last_started_at is None
                else self.min_interval - (now - self._last_started_at),
            )
            if delay > 0:
                await asyncio.sleep(delay)
            self._last_started_at = time.monotonic()
            self._owner = asyncio.current_task()
            return self
        except BaseException:
            self._lock.release()
            raise

    def defer(self, delay_seconds: float) -> None:
        if self._owner is not asyncio.current_task():
            raise RuntimeError("MxRequestGate.defer() requires the gate lock")
        self._blocked_until = max(
            self._blocked_until,
            time.monotonic() + max(0.0, delay_seconds),
        )

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self._owner = None
        self._lock.release()


def is_mx_rate_limit_message(message: str) -> bool:
    normalized = message.casefold()
    return any(marker in normalized for marker in _MX_RATE_LIMIT_MARKERS)


def is_mx_daily_quota_message(message: str) -> bool:
    normalized = message.casefold()
    return any(marker in normalized for marker in _MX_DAILY_QUOTA_MARKERS)


def mx_redact_text(value: str, *, api_key: str | None = None) -> str:
    """Redact MX credentials from compact upstream diagnostics."""

    redacted = value
    if api_key and api_key.strip():
        redacted = redacted.replace(api_key, "[redacted]")
    redacted = _MX_BEARER_PATTERN.sub("Bearer [redacted]", redacted)
    return _MX_SENSITIVE_ASSIGNMENT_PATTERN.sub(r"\1[redacted]", redacted)


def mx_http_error_detail(
    response_text: str,
    *,
    api_key: str | None = None,
) -> str | None:
    detail: object
    try:
        payload = json.loads(response_text)
    except ValueError:
        detail = response_text
    else:
        if isinstance(payload, dict):
            detail = (
                payload.get("message") or payload.get("msg") or payload.get("error")
            )
        else:
            detail = payload
    if detail in (None, ""):
        return None
    text = " ".join(str(detail).split())
    text = mx_redact_text(text, api_key=api_key)
    return text[:500] or None


def mx_retry_delay(
    retry_index: int,
    *,
    base_delay: float,
    retry_after: str | None = None,
) -> float:
    """Return a bounded Retry-After or exponential-backoff delay."""

    if retry_after:
        parsed = -1.0
        try:
            parsed = float(retry_after)
        except ValueError:
            pass
        if math.isfinite(parsed) and parsed >= 0:
            return min(parsed, _MAX_RETRY_AFTER_SECONDS)
    exponent: int = max(0, retry_index - 1)
    return min(
        max(0.0, base_delay) * (2.0**exponent),
        _MAX_RETRY_AFTER_SECONDS,
    )


__all__ = [
    "MxRequestGate",
    "is_mx_daily_quota_message",
    "is_mx_rate_limit_message",
    "mx_http_error_detail",
    "mx_redact_text",
    "mx_retry_delay",
]
