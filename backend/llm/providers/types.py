"""Internal provider-driver contracts."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Protocol

import httpx

from backend.llm.abort import AbortSignal
from backend.llm.contracts import (
    AssistantMessage,
    LLMChatMessage,
    LLMToolDefinition,
    ModelProtocol,
)
from backend.llm.events import LLMEvent
from backend.llm.provider_config import ModelProviderConfig
from backend.llm.retry import RetryPolicy
from backend.llm.thinking import ThinkingEffort

EventSink = Callable[[LLMEvent], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class DriverRequest:
    protocol: ModelProtocol
    base_url: str
    api_key: str
    model: str
    messages: list[LLMChatMessage]
    tools: list[LLMToolDefinition]
    temperature: float
    top_p: float
    max_output_tokens: int
    thinking_effort: ThinkingEffort | None = None
    provider_config: ModelProviderConfig = field(default_factory=ModelProviderConfig)


class ProviderStreamFailure(Exception):
    """Carry a normalized error together with the provider's partial message."""

    def __init__(self, error: Exception, message: AssistantMessage) -> None:
        super().__init__(str(error) or type(error).__name__)
        self.error = error
        self.message = message


class ModelProtocolDriver(Protocol):
    protocol: ModelProtocol

    async def stream_message(
        self,
        *,
        client: httpx.AsyncClient,
        request: DriverRequest,
        emit: EventSink,
        abort_signal: AbortSignal | None,
        retry_policy: RetryPolicy,
    ) -> AssistantMessage: ...

    def build_text_request(
        self,
        *,
        base_url: str = "",
        api_key: str,
        model: str,
        system_prompt: str | None,
        user_prompt: str,
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        provider_config: ModelProviderConfig = ModelProviderConfig(),
    ) -> dict[str, object]: ...

    def parse_text_response(self, payload: dict[str, object]) -> str: ...


__all__ = [
    "DriverRequest",
    "EventSink",
    "ModelProtocolDriver",
    "ProviderStreamFailure",
]
