"""Ports exposed by the LLM layer."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol

from backend.llm.abort import AbortSignal
from backend.llm.contracts import (
    LLMChatMessage,
    LLMChatResponse,
    LLMToolDefinition,
    ModelCatalogItem,
    ModelProtocol,
)
from backend.llm.events import AssistantMessageEventStream
from backend.llm.provider_config import ModelProviderConfig
from backend.llm.thinking import ThinkingEffort


class LLMClientPort(Protocol):
    async def generate_text(
        self,
        *,
        protocol: ModelProtocol,
        base_url: str,
        api_key: str,
        model: str,
        system_prompt: str | None,
        user_prompt: str,
        temperature: float,
        top_p: float = 1.0,
        max_output_tokens: int = 32_768,
        thinking_effort: ThinkingEffort | None = None,
        provider_config: ModelProviderConfig | None = None,
        on_text_delta: Callable[[str], Awaitable[None]] | None = None,
        on_reasoning_delta: Callable[[str], Awaitable[None]] | None = None,
        abort_signal: AbortSignal | None = None,
    ) -> str: ...

    def stream_chat(
        self,
        *,
        protocol: ModelProtocol,
        base_url: str,
        api_key: str,
        model: str,
        messages: list[LLMChatMessage],
        temperature: float,
        tools: list[LLMToolDefinition] | None = None,
        top_p: float = 1.0,
        max_output_tokens: int = 32_768,
        thinking_effort: ThinkingEffort | None = None,
        provider_config: ModelProviderConfig | None = None,
        abort_signal: AbortSignal | None = None,
    ) -> AssistantMessageEventStream: ...

    async def chat(
        self,
        *,
        protocol: ModelProtocol,
        base_url: str,
        api_key: str,
        model: str,
        messages: list[LLMChatMessage],
        temperature: float,
        tools: list[LLMToolDefinition] | None = None,
        top_p: float = 1.0,
        max_output_tokens: int = 32_768,
        thinking_effort: ThinkingEffort | None = None,
        provider_config: ModelProviderConfig | None = None,
        on_text_delta: Callable[[str], Awaitable[None]] | None = None,
        on_reasoning_delta: Callable[[str], Awaitable[None]] | None = None,
        abort_signal: AbortSignal | None = None,
    ) -> LLMChatResponse: ...


class ModelConnectivityTesterPort(Protocol):
    async def list_models(
        self,
        *,
        protocol: ModelProtocol,
        base_url: str,
        api_key: str,
        provider_config: ModelProviderConfig | None = None,
    ) -> list[ModelCatalogItem]: ...


__all__ = ["AbortSignal", "LLMClientPort", "ModelConnectivityTesterPort"]
