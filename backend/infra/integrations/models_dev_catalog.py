"""Cached models.dev catalog lookup for model editor prefills."""

from __future__ import annotations

import asyncio
import math
from typing import Any, cast

import httpx

from backend.business.settings import ModelsDevModel
from backend.llm.thinking import THINKING_EFFORTS, ThinkingEffort

MODELS_DEV_CATALOG_URL = "https://models.dev/catalog.json"


class ModelsDevCatalog:
    """Fetch one process-wide catalog snapshot and resolve exact identities."""

    def __init__(
        self,
        *,
        client: httpx.AsyncClient | None = None,
        catalog_url: str = MODELS_DEV_CATALOG_URL,
    ) -> None:
        self._client = client or httpx.AsyncClient(timeout=10.0)
        self._owns_client = client is None
        self._catalog_url = catalog_url
        self._etag = ""
        self._catalog: dict[str, Any] | None = None
        self._lock = asyncio.Lock()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def lookup(self, model_name: str) -> ModelsDevModel | None:
        query = model_name.strip()
        if not query:
            return None
        catalog = await self._load_catalog()
        models = _mapping(catalog.get("models"))
        canonical_id = _resolve_canonical_id(models, query)
        if canonical_id is None:
            return None
        canonical = _mapping(models.get(canonical_id))
        provider_id, separator, provider_model_id = canonical_id.partition("/")
        if not separator or not provider_id or not provider_model_id:
            return None

        providers = _mapping(catalog.get("providers"))
        provider = _mapping(_case_insensitive_get(providers, provider_id))
        provider_models = _mapping(provider.get("models"))
        provider_model = _mapping(
            _case_insensitive_get(provider_models, provider_model_id)
        )
        limit = _mapping(canonical.get("limit")) or _mapping(
            provider_model.get("limit")
        )
        context_window = _positive_int(limit.get("context"))
        max_output = _positive_int(limit.get("output"))
        if context_window is None or max_output is None:
            return None

        raw_reasoning_options = (
            provider_model["reasoning_options"]
            if "reasoning_options" in provider_model
            else canonical.get("reasoning_options")
        )
        reasoning_efforts = _thinking_efforts(raw_reasoning_options)
        cost = _mapping(provider_model.get("cost"))
        return ModelsDevModel(
            # Keep the channel-facing identifier exactly as entered. Gateways may
            # require the provider prefix even when direct providers do not.
            model_name=query,
            label=str(
                canonical.get("name") or provider_model.get("name") or provider_model_id
            ),
            provider_id=canonical_id,
            context_window_tokens=context_window,
            max_output_tokens=max_output,
            input_price_per_million=_price(cost.get("input")),
            output_price_per_million=_price(cost.get("output")),
            cache_read_price_per_million=_price(cost.get("cache_read")),
            cache_write_price_per_million=_price(cost.get("cache_write")),
            thinking_efforts=reasoning_efforts or (),
        )

    async def _load_catalog(self) -> dict[str, Any]:
        async with self._lock:
            headers = {"If-None-Match": self._etag} if self._etag else {}
            response = await self._client.get(self._catalog_url, headers=headers)
            if response.status_code == httpx.codes.NOT_MODIFIED:
                if self._catalog is None:
                    raise RuntimeError(
                        "models.dev returned 304 without a cached catalog"
                    )
                return self._catalog
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError("models.dev catalog must be a JSON object")
            if not isinstance(payload.get("models"), dict) or not isinstance(
                payload.get("providers"), dict
            ):
                raise RuntimeError("models.dev catalog is missing models or providers")
            self._catalog = payload
            self._etag = response.headers.get("ETag", "").strip()
            return payload


def _thinking_efforts(value: object) -> tuple[ThinkingEffort, ...]:
    """Map models.dev effort options to the presets exposed by this product."""

    if not isinstance(value, list):
        return ()
    supported: set[str] = set()
    saw_effort_options = False
    for raw_option in value:
        option = _mapping(raw_option)
        if option.get("type") != "effort":
            continue
        saw_effort_options = True
        values = option.get("values")
        if isinstance(values, list):
            supported.update(item for item in values if isinstance(item, str))
    if not saw_effort_options:
        return ()
    return tuple(effort for effort in THINKING_EFFORTS if effort in supported)


def _mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items()}


def _case_insensitive_get(values: dict[str, Any], key: str) -> object | None:
    if key in values:
        return cast(object, values[key])
    matches = [
        value
        for candidate, value in values.items()
        if candidate.casefold() == key.casefold()
    ]
    return cast(object, matches[0]) if len(matches) == 1 else None


def _resolve_canonical_id(models: dict[str, Any], query: str) -> str | None:
    exact = [model_id for model_id in models if model_id.casefold() == query.casefold()]
    if len(exact) == 1:
        return exact[0]
    tails = [
        model_id
        for model_id in models
        if model_id.partition("/")[2].casefold() == query.casefold()
    ]
    return tails[0] if len(tails) == 1 else None


def _positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, float) and value.is_integer() and value > 0:
        return int(value)
    return None


def _price(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    result = float(value)
    return result if result >= 0 and math.isfinite(result) else None


__all__ = ["MODELS_DEV_CATALOG_URL", "ModelsDevCatalog"]
