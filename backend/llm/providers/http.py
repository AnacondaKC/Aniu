"""HTTP and JSON helpers shared by LLM protocol drivers."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qsl, urlparse, urlunparse

import httpx

from backend.llm.contracts import ModelProtocol, ProviderJsonObject
from backend.llm.errors import LLMConfigurationError, invalid_response
from backend.llm.provider_config import ModelAuthMode, ModelProviderConfig
from backend.llm.sanitize import sanitize_json


def _absolute_url(base_url: str, path: str) -> str:
    parsed = httpx.URL(base_url)
    merged = parsed.path.rstrip("/") + "/" + str(path).lstrip("/")
    return str(parsed.copy_with(path=merged))


def _normalize_base_path(protocol: ModelProtocol, path: str) -> str:
    normalized = path.rstrip("/")
    suffix = (
        "/messages" if protocol is ModelProtocol.CLAUDE_API else "/chat/completions"
    )
    if normalized.endswith(suffix):
        normalized = normalized[: -len(suffix)]
    if protocol is ModelProtocol.CLAUDE_API:
        if not normalized:
            return "/v1"
        return normalized if normalized.endswith("/v1") else f"{normalized}/v1"
    return normalized or "/v1"


def _normalize_base_url(protocol: ModelProtocol, base_url: str) -> str:
    parsed = urlparse(base_url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise LLMConfigurationError("llm_base_url must be a valid http or https URL")
    normalized_path = _normalize_base_path(protocol, parsed.path)
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            normalized_path,
            parsed.params,
            parsed.query,
            "",
        )
    )


def _sdk_base_url(protocol: ModelProtocol, base_url: str) -> str:
    normalized = _normalize_base_url(protocol, base_url)
    parsed = urlparse(normalized)
    path = parsed.path.rstrip("/")
    if protocol is ModelProtocol.CLAUDE_API and path.endswith("/v1"):
        path = path[:-3] or ""
    return urlunparse((parsed.scheme, parsed.netloc, path, parsed.params, "", ""))


def _query_items(base_url: str) -> tuple[tuple[str, str], ...]:
    parsed = urlparse(base_url.strip())
    return tuple(parse_qsl(parsed.query, keep_blank_values=True))


def _sdk_endpoint(protocol: ModelProtocol, base_url: str) -> str:
    return _sdk_base_url(protocol, base_url)


def _resolved_auth_mode(
    protocol: ModelProtocol,
    base_url: str,
    provider_config: ModelProviderConfig,
) -> ModelAuthMode:
    if provider_config.auth_mode is not ModelAuthMode.AUTO:
        return provider_config.auth_mode
    if protocol is ModelProtocol.CLAUDE_API:
        return ModelAuthMode.API_KEY
    parsed = urlparse(base_url.strip())
    host = (parsed.hostname or "").lower()
    azure_host = host.endswith((".openai.azure.com", ".services.ai.azure.com"))
    azure_path = "/openai/deployments/" in parsed.path.lower()
    return ModelAuthMode.API_KEY if azure_host or azure_path else ModelAuthMode.BEARER


def _auth_headers(
    *,
    protocol: ModelProtocol,
    base_url: str,
    api_key: str,
    provider_config: ModelProviderConfig,
) -> dict[str, str]:
    key = _validate_api_key(api_key)
    mode = _resolved_auth_mode(protocol, base_url, provider_config)
    if mode is ModelAuthMode.BEARER:
        return {"Authorization": f"Bearer {key}"}
    header = "x-api-key" if protocol is ModelProtocol.CLAUDE_API else "api-key"
    return {header: key}


def _validate_api_key(api_key: str) -> str:
    normalized = api_key.strip()
    if not normalized:
        raise LLMConfigurationError("llm_api_key is not configured")
    return normalized


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str) and text.strip():
                    parts.append(text.strip())
            elif isinstance(item, str) and item.strip():
                parts.append(item.strip())
        return "\n".join(parts).strip()
    raise invalid_response(
        f"llm text content has unsupported type: {type(value).__name__}"
    )


def _parse_tool_arguments(value: Any) -> ProviderJsonObject:
    if value is None:
        return {}
    parsed: object
    if isinstance(value, dict):
        parsed = value
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return {}
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise invalid_response(
                "tool arguments contain invalid JSON", cause=exc
            ) from exc
    else:
        raise invalid_response("tool arguments must be a JSON object")
    if not isinstance(parsed, dict):
        raise invalid_response("tool arguments must decode to a JSON object")
    try:
        sanitized = sanitize_json(parsed)
    except LLMConfigurationError as exc:
        raise invalid_response(
            "tool arguments contain invalid JSON", cause=exc
        ) from exc
    if not isinstance(sanitized, dict):
        raise invalid_response("tool arguments must decode to a JSON object")
    return sanitized


__all__ = [
    "_absolute_url",
    "_auth_headers",
    "_coerce_text",
    "_normalize_base_url",
    "_parse_tool_arguments",
    "_query_items",
    "_resolved_auth_mode",
    "_sdk_base_url",
    "_sdk_endpoint",
    "_validate_api_key",
]
