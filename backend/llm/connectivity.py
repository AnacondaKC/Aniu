"""Protocol-aware model connectivity checks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, cast

import httpx

from backend.llm.contracts import (
    ModelCatalogItem,
    ModelProtocol,
)
from backend.llm.errors import (
    LLMConfigurationError,
    LLMIntegrationError,
    provider_error,
)
from backend.llm.provider_config import ModelProviderConfig
from backend.llm.providers.http import (
    _absolute_url,
    _auth_headers,
    _normalize_base_url,
)
from backend.llm.registry import ProviderRegistry, default_provider_registry


def _truncate_message(message: str, limit: int = 280) -> str:
    trimmed = " ".join(message.split())
    if len(trimmed) <= limit:
        return trimmed
    return f"{trimmed[: limit - 3]}..."


def _extract_error_message(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        payload = response.text

    if isinstance(payload, dict):
        if isinstance(payload.get("error"), dict):
            message = payload["error"].get("message")
            if isinstance(message, str) and message.strip():
                return _truncate_message(message)
        for key in ("message", "detail", "error"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return _truncate_message(value)

    if isinstance(payload, str) and payload.strip():
        return _truncate_message(payload)
    return f"provider returned status {response.status_code}"


@dataclass(slots=True)
class ModelConnectivityTester:
    """Query provider model catalogs over the configured protocol."""

    timeout: float = 30.0
    transport: httpx.AsyncBaseTransport | None = None
    registry: ProviderRegistry = field(default_factory=default_provider_registry)
    _client: httpx.AsyncClient | None = field(default=None, init=False, repr=False)

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                transport=self.transport,
            )
        return self._client

    async def aclose(self) -> None:
        client = self._client
        self._client = None
        if client is not None and not client.is_closed:
            await client.aclose()

    async def list_models(
        self,
        *,
        protocol: ModelProtocol,
        base_url: str,
        api_key: str,
        provider_config: ModelProviderConfig | None = None,
    ) -> list[ModelCatalogItem]:
        normalized_base_url = _normalize_base_url(protocol, base_url)
        resolved_config = provider_config or ModelProviderConfig()
        request = self._build_models_request(
            protocol=protocol,
            base_url=normalized_base_url,
            api_key=api_key,
            provider_config=resolved_config,
        )
        request["url"] = _absolute_url(normalized_base_url, str(request["url"]))

        try:
            response = await self._ensure_client().request(**cast(Any, request))
            response.raise_for_status()
            payload = response.json()
        except httpx.HTTPError as exc:
            error = provider_error(exc)
            if isinstance(exc, httpx.HTTPStatusError):
                error.args = (
                    "model list request failed:"
                    f" {_extract_error_message(exc.response)}",
                )
            else:
                error.args = (f"model list request failed: {error}",)
            raise error from exc
        except ValueError as exc:
            raise LLMIntegrationError("model list returned non-JSON response") from exc

        if not isinstance(payload, dict):
            raise LLMIntegrationError("model list response must be a JSON object")
        return self._parse_models(payload)

    @staticmethod
    def _build_models_request(
        *,
        protocol: ModelProtocol,
        base_url: str,
        api_key: str,
        provider_config: ModelProviderConfig,
    ) -> dict[str, Any]:
        if not api_key.strip():
            raise LLMConfigurationError("llm_api_key is not configured")
        headers = _auth_headers(
            protocol=protocol,
            base_url=base_url,
            api_key=api_key,
            provider_config=provider_config,
        )
        if protocol is ModelProtocol.CLAUDE_API:
            headers["anthropic-version"] = "2023-06-01"
        return {
            "method": "GET",
            "url": "/models",
            "headers": headers,
        }

    @staticmethod
    def _parse_models(payload: dict[str, Any]) -> list[ModelCatalogItem]:
        data = payload.get("data")
        if not isinstance(data, list):
            raise LLMIntegrationError("model list response missing data array")
        items = []
        for entry in data:
            if not isinstance(entry, dict):
                continue
            model = str(entry.get("id") or "").strip()
            if not model:
                continue
            items.append(
                ModelCatalogItem(
                    model=model,
                    label=model,
                    provider_id=model,
                )
            )
        return items


__all__ = ["ModelConnectivityTester"]
