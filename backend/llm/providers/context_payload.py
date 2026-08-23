"""Provider-facing context payload construction shared by drivers and budgeting."""

from __future__ import annotations

from typing import Any

from backend.llm.contracts import LLMChatMessage, LLMToolDefinition, ModelProtocol
from backend.llm.provider_config import ModelProviderConfig
from backend.llm.providers.extract import _claude_tools_spec, _openai_tool_spec
from backend.llm.providers.messages import (
    _claude_messages,
    _claude_system_prompt,
    _openai_chat_messages,
)


def openai_replays_reasoning_content(
    *,
    model: str,
    provider_config: ModelProviderConfig,
) -> bool:
    """Return the same reasoning replay decision used for OpenAI-compatible calls."""

    override = provider_config.openai.replay_reasoning_content
    return "deepseek" in model.lower() if override is None else override


def provider_context_payload(
    *,
    protocol: ModelProtocol,
    messages: list[LLMChatMessage],
    tools: list[LLMToolDefinition],
    model: str,
    provider_config: ModelProviderConfig,
) -> dict[str, Any]:
    """Build the context-bearing portion of the provider request body.

    Model name, streaming flags, sampling controls, and output caps are request
    metadata. This function intentionally contains only the material that the
    provider interprets as model context: system instructions, messages, and tools.
    """

    if protocol is ModelProtocol.OPENAI_CHAT_COMPLETIONS:
        payload: dict[str, Any] = {
            "messages": _openai_chat_messages(
                messages,
                replay_reasoning_content=openai_replays_reasoning_content(
                    model=model,
                    provider_config=provider_config,
                ),
            )
        }
        if tools:
            payload["tools"] = [_openai_tool_spec(tool) for tool in tools]
            payload["tool_choice"] = "auto"
        return payload

    if protocol is ModelProtocol.CLAUDE_API:
        payload = {"messages": _claude_messages(messages)}
        system = _claude_system_prompt(messages)
        if system is not None:
            payload["system"] = system
        if tools:
            payload["tools"] = _claude_tools_spec(tools)
        return payload

    raise ValueError(f"unsupported model protocol: {protocol}")


__all__ = ["openai_replays_reasoning_content", "provider_context_payload"]
