"""Runtime context used by the generic agent loop."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from backend.agent.kernel.runtime_config import LlmRuntimeConfig
from backend.llm import AbortSignal, LLMClientPort

AgentEventSink = Callable[[str, dict[str, object]], Awaitable[None]]
AgentStreamSink = Callable[[str, str], Awaitable[None]]
ToolAuthorization = Callable[[object, dict[str, object]], str | None]


@dataclass(slots=True)
class AgentContext:
    """Dependencies frozen for one generic AgentHarness prompt run."""

    runtime: LlmRuntimeConfig | None
    llm_client: LLMClientPort
    system_prompt: str = ""
    tool_registry: object | None = None
    abort_signal: AbortSignal | None = None
    event_sink: AgentEventSink | None = None
    stream_sink: AgentStreamSink | None = None
    tool_authorizer: ToolAuthorization | None = None
    label: str = "agent"


__all__ = [
    "AgentContext",
    "AgentEventSink",
    "AgentStreamSink",
    "ToolAuthorization",
]
