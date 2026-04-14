from __future__ import annotations

import time
from collections import defaultdict
from threading import Lock
from typing import Any

from fastapi import HTTPException, Request, status


class _RateBucket:
    __slots__ = ("timestamps",)

    def __init__(self) -> None:
        self.timestamps: list[float] = []

    def hit(self, now: float, window: float, limit: int) -> bool:
        cutoff = now - window
        self.timestamps = [t for t in self.timestamps if t > cutoff]
        if len(self.timestamps) >= limit:
            return False
        self.timestamps.append(now)
        return True


class RateLimiter:
    """Simple in-memory sliding-window rate limiter keyed by client IP + path."""

    def __init__(self) -> None:
        self._buckets: dict[str, _RateBucket] = defaultdict(_RateBucket)
        self._lock = Lock()
        self._last_cleanup = time.monotonic()
        self._cleanup_interval = 300.0  # cleanup stale entries every 5 min

    def check(self, key: str, window: float, limit: int) -> bool:
        now = time.monotonic()
        with self._lock:
            if now - self._last_cleanup > self._cleanup_interval:
                self._cleanup(now)
                self._last_cleanup = now
            return self._buckets[key].hit(now, window, limit)

    def _cleanup(self, now: float) -> None:
        stale_keys = [
            k
            for k, v in self._buckets.items()
            if not v.timestamps or v.timestamps[-1] < now - 600
        ]
        for k in stale_keys:
            del self._buckets[k]


_limiter = RateLimiter()

# Rate limit rules: path_prefix -> (window_seconds, max_requests)
_RULES: dict[str, tuple[float, int]] = {
    "/api/aniu/login": (60.0, 10),
    "/api/aniu/run": (60.0, 5),
    "/api/aniu/chat": (60.0, 20),
}


def get_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


async def rate_limit_middleware(request: Request, call_next: Any) -> Any:
    path = request.url.path
    rule = _RULES.get(path)
    if rule is not None:
        window, limit = rule
        client_ip = get_client_ip(request)
        key = f"{client_ip}:{path}"
        if not _limiter.check(key, window, limit):
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="请求过于频繁，请稍后再试。",
            )
    return await call_next(request)
