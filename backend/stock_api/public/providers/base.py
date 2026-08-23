"""Internal helpers shared by fixed public provider adapters."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from typing import TypeVar
from urllib.parse import urlencode

from backend.stock_api.public.cancellation import CancellationToken as AbortSignal
from backend.stock_api.public.contracts import ProviderName
from backend.stock_api.public.http import PublicHttpRequest, PublicHttpTransport

_TextResult = TypeVar("_TextResult")

USER_AGENT = "Mozilla/5.0 (compatible; AniuBot/0.1)"


def build_url(origin: str, path: str, parameters: Mapping[str, object]) -> str:
    query = urlencode(
        [(key, str(value)) for key, value in parameters.items() if value is not None]
    )
    return f"{origin.rstrip('/')}/{path.lstrip('/')}" + (f"?{query}" if query else "")


def public_headers(*, referer: str | None = None, html: bool = False) -> dict[str, str]:
    headers = {
        "Accept": "text/html,application/xhtml+xml"
        if html
        else "application/json, text/plain, */*",
        "User-Agent": USER_AGENT,
    }
    if referer:
        headers["Referer"] = referer
    return headers


class FixedPublicAdapter:
    """Base class that keeps network requests in provider-owned adapters."""

    provider: ProviderName

    def __init__(self, transport: PublicHttpTransport) -> None:
        self._transport = transport

    async def _json(
        self,
        *,
        operation: str,
        endpoint: str,
        url: str,
        parameters: Mapping[str, object],
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
        headers: Mapping[str, str],
        encoding: str = "utf-8",
        lane: str | None = None,
        fallback_urls: tuple[str, ...] = (),
        method: str = "GET",
        body: str | None = None,
    ) -> object:
        return await self._transport.request_json(
            PublicHttpRequest(
                provider=self.provider,
                operation=operation,
                endpoint=endpoint,
                url=url,
                parameters=parameters,
                headers=headers,
                encoding=encoding,
                lane=lane,
                fallback_urls=fallback_urls,
                method=method,
                body=body,
            ),
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def _text(
        self,
        *,
        operation: str,
        endpoint: str,
        url: str,
        parameters: Mapping[str, object],
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
        headers: Mapping[str, str],
        encoding: str = "utf-8",
        lane: str | None = None,
        fallback_urls: tuple[str, ...] = (),
        parser: Callable[[str], _TextResult] | None = None,
    ) -> str | _TextResult:
        return await self._transport.request_text(
            PublicHttpRequest(
                provider=self.provider,
                operation=operation,
                endpoint=endpoint,
                url=url,
                parameters=parameters,
                headers=headers,
                encoding=encoding,
                lane=lane,
                fallback_urls=fallback_urls,
            ),
            parser=parser,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )


def parse_jsonp(value: str) -> object:
    source = value.lstrip("\ufeff").strip().rstrip(";").strip()
    assignment = re.fullmatch(r"(?:var\s+)?[A-Za-z_$][\w$.]*\s*=\s*([\s\S]+)", source)
    callback = re.fullmatch(r"[A-Za-z_$][\w$.]*\s*\(\s*([\s\S]*)\s*\)", source)
    return json.loads(
        (
            assignment.group(1)
            if assignment
            else callback.group(1)
            if callback
            else source
        ).strip()
    )


__all__ = [
    "FixedPublicAdapter",
    "USER_AGENT",
    "build_url",
    "parse_jsonp",
    "public_headers",
]
