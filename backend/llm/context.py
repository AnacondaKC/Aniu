"""Public provider-aware context construction and estimation APIs."""

from __future__ import annotations

import json
from typing import Any

from backend.llm.contracts import LLMChatMessage, LLMToolDefinition, ModelProtocol
from backend.llm.provider_config import ModelProviderConfig
from backend.llm.providers.context_payload import provider_context_payload


def build_provider_context_payload(
    *,
    protocol: ModelProtocol,
    messages: list[LLMChatMessage],
    tools: list[LLMToolDefinition],
    model: str,
    provider_config: ModelProviderConfig | None = None,
) -> dict[str, Any]:
    """Build the provider-visible context payload for a model request."""

    return provider_context_payload(
        protocol=protocol,
        messages=messages,
        tools=tools,
        model=model,
        provider_config=provider_config or ModelProviderConfig(),
    )


def estimate_tokens(text: str) -> int:
    """Conservatively estimate mixed ASCII/CJK text before a model request."""

    if not text:
        return 0
    ascii_chars = sum(1 for char in text if ord(char) < 128)
    non_ascii_chars = len(text) - ascii_chars
    return max(1, (ascii_chars + 3) // 4 + non_ascii_chars)


def estimate_messages_tokens(messages: list[LLMChatMessage]) -> int:
    """Estimate normalized internal messages when no provider runtime is known."""

    total = 0
    for message in messages:
        total += _estimate_serialized_payload(message)
    return total


def estimate_provider_request_tokens(
    messages: list[LLMChatMessage],
    tools: list[LLMToolDefinition],
    *,
    protocol: ModelProtocol | None = None,
    model: str = "",
    provider_config: ModelProviderConfig | None = None,
) -> int:
    """Estimate the complete provider-visible context for one request.

    Without a resolved provider runtime, this falls back to the normalized
    transcript and tool definitions. The fallback keeps preflight checks usable
    for lightweight callers; real model requests should pass all provider fields.
    """

    if protocol is None:
        return estimate_messages_tokens(messages) + _estimate_serialized_payload(tools)
    try:
        payload = build_provider_context_payload(
            protocol=protocol,
            messages=messages,
            tools=tools,
            model=model,
            provider_config=provider_config,
        )
    except (KeyError, TypeError, ValueError):
        # Preserve a conservative preflight estimate. Driver validation remains
        # the authority for malformed provider input at the transport boundary.
        return estimate_messages_tokens(messages) + _estimate_serialized_payload(tools)
    return _estimate_serialized_payload(payload)


def estimate_provider_message_tokens(
    messages: list[LLMChatMessage],
    *,
    protocol: ModelProtocol | None = None,
    model: str = "",
    provider_config: ModelProviderConfig | None = None,
) -> int:
    """Estimate provider-visible messages without fixed tool-schema overhead."""

    return estimate_provider_request_tokens(
        messages,
        [],
        protocol=protocol,
        model=model,
        provider_config=provider_config,
    )


def _estimate_serialized_payload(value: object) -> int:
    serialized = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )
    return estimate_tokens(serialized) + 4


__all__ = [
    "build_provider_context_payload",
    "estimate_messages_tokens",
    "estimate_provider_message_tokens",
    "estimate_provider_request_tokens",
    "estimate_tokens",
]
