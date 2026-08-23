"""Stream-first LLM facade for OpenAI Chat and Anthropic Messages."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import httpx

from backend.llm.abort import AbortSignal, throw_if_aborted
from backend.llm.contracts import (
    AssistantMessage,
    LLMChatMessage,
    LLMChatResponse,
    LLMToolDefinition,
    ModelProtocol,
    StopReason,
    chat_response_from_assistant,
)
from backend.llm.errors import LLMErrorCode, LLMIntegrationError, invalid_response
from backend.llm.events import (
    AssistantMessageEventStream,
    Completed,
    Failed,
    ReasoningDelta,
    TextDelta,
)
from backend.llm.provider_config import ModelProviderConfig
from backend.llm.providers.http import _query_items, _validate_api_key
from backend.llm.providers.types import DriverRequest, ProviderStreamFailure
from backend.llm.registry import ProviderRegistry, default_provider_registry
from backend.llm.retry import RetryPolicy
from backend.llm.thinking import ThinkingEffort

TextDeltaHandler = Callable[[str], Awaitable[None]] | None
DEFAULT_LLM_READ_TIMEOUT_SECONDS = 300.0
DEFAULT_LLM_CONNECT_TIMEOUT_SECONDS = 10.0


class LLMClient:
    """Process-scoped connection pool and provider-neutral stream facade."""

    def __init__(
        self,
        *,
        read_timeout: float = DEFAULT_LLM_READ_TIMEOUT_SECONDS,
        connect_timeout: float = DEFAULT_LLM_CONNECT_TIMEOUT_SECONDS,
        transport: httpx.AsyncBaseTransport | None = None,
        registry: ProviderRegistry | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self.timeout = httpx.Timeout(
            connect=connect_timeout,
            read=read_timeout,
            write=30.0,
            pool=connect_timeout,
        )
        self.transport = transport
        self._registry = registry or default_provider_registry()
        self._retry_policy = retry_policy or RetryPolicy()
        self._clients: dict[tuple[tuple[str, str], ...], httpx.AsyncClient] = {}

    def _ensure_client(self, base_url: str = "") -> httpx.AsyncClient:
        query_items = _query_items(base_url)
        client = self._clients.get(query_items)
        if client is None:
            client = httpx.AsyncClient(
                timeout=self.timeout,
                transport=self.transport,
                params=query_items,
            )
            self._clients[query_items] = client
        return client

    async def aclose(self) -> None:
        clients = tuple(self._clients.values())
        self._clients.clear()
        for client in clients:
            if not client.is_closed:
                await client.aclose()

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
    ) -> AssistantMessageEventStream:
        _validate_api_key(api_key)
        throw_if_aborted(abort_signal)
        driver = self._registry.require(protocol)
        request = DriverRequest(
            protocol=protocol,
            base_url=base_url,
            api_key=api_key,
            model=model,
            messages=messages,
            tools=tools or [],
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            thinking_effort=thinking_effort,
            provider_config=provider_config or ModelProviderConfig(),
        )
        stream = AssistantMessageEventStream(protocol=protocol, model=model)

        async def produce() -> None:
            try:
                message = await driver.stream_message(
                    client=self._ensure_client(request.base_url),
                    request=request,
                    emit=stream.emit,
                    abort_signal=abort_signal,
                    retry_policy=self._retry_policy,
                )
            except asyncio.CancelledError:
                raise
            except ProviderStreamFailure as exc:
                await stream.emit(Failed(exc.error, exc.message))
            except Exception as exc:
                aborted = bool(abort_signal is not None and abort_signal.aborted)
                failed_message = AssistantMessage(
                    protocol=protocol,
                    model=model,
                    content=(),
                    stop_reason=(StopReason.ABORTED if aborted else StopReason.ERROR),
                    error_message=str(exc) or type(exc).__name__,
                )
                await stream.emit(Failed(exc, failed_message))
            else:
                await stream.emit(Completed(message))

        stream.attach(asyncio.create_task(produce(), name=f"llm:{protocol}:{model}"))
        return stream

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
        on_text_delta: TextDeltaHandler | None = None,
        on_reasoning_delta: TextDeltaHandler | None = None,
        abort_signal: AbortSignal | None = None,
    ) -> LLMChatResponse:
        stream = self.stream_chat(
            protocol=protocol,
            base_url=base_url,
            api_key=api_key,
            model=model,
            messages=messages,
            temperature=temperature,
            tools=tools,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            thinking_effort=thinking_effort,
            provider_config=provider_config,
            abort_signal=abort_signal,
        )
        async for event in stream:
            if isinstance(event, TextDelta) and on_text_delta is not None:
                await on_text_delta(event.delta)
            elif isinstance(event, ReasoningDelta) and on_reasoning_delta is not None:
                await on_reasoning_delta(event.delta)
            elif isinstance(event, Completed):
                return chat_response_from_assistant(event.message)
            elif isinstance(event, Failed):
                raise event.error
        raise RuntimeError("LLM stream ended without a terminal event")

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
        on_text_delta: TextDeltaHandler | None = None,
        on_reasoning_delta: TextDeltaHandler | None = None,
        abort_signal: AbortSignal | None = None,
    ) -> str:
        messages: list[LLMChatMessage] = []
        if system_prompt and system_prompt.strip():
            messages.append({"role": "system", "content": system_prompt.strip()})
        messages.append({"role": "user", "content": user_prompt})
        response = await self.chat(
            protocol=protocol,
            base_url=base_url,
            api_key=api_key,
            model=model,
            messages=messages,
            temperature=temperature,
            tools=[],
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            thinking_effort=thinking_effort,
            provider_config=provider_config,
            on_text_delta=on_text_delta,
            on_reasoning_delta=on_reasoning_delta,
            abort_signal=abort_signal,
        )
        content = response["content"]
        if response.get("stop_reason") is StopReason.CONTEXT_OVERFLOW:
            raise LLMIntegrationError(
                "text generation stopped because the provider context window "
                "was exceeded",
                error_code=LLMErrorCode.CONTEXT_OVERFLOW,
            )
        if response.get("stop_reason") is StopReason.LENGTH:
            raise invalid_response(
                "text generation reached the provider token limit before completion"
            )
        if content is None:
            raise RuntimeError("text generation completed without text")
        return content


__all__ = [
    "DEFAULT_LLM_CONNECT_TIMEOUT_SECONDS",
    "DEFAULT_LLM_READ_TIMEOUT_SECONDS",
    "LLMClient",
]
