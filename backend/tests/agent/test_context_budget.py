"""Tests for complete-message context budget validation."""

from __future__ import annotations

import pytest

from backend.agent.kernel.context_budget import (
    ContextBudgetConfig,
    ContextBudgetExceededError,
    ensure_context_budget,
)
from backend.agent.kernel.runtime_config import LlmRuntimeConfig
from backend.llm import (
    ModelProtocol,
    TextContent,
    estimate_messages_tokens,
    estimate_provider_request_tokens,
)


def test_complete_tool_result_is_preserved_when_context_fits() -> None:
    tool_content = "字" * 50_000
    messages = [
        {"role": "system", "content": "system"},
        {"role": "user", "content": "task"},
        {"role": "tool", "content": tool_content, "tool_call_id": "1"},
    ]

    fitted = ensure_context_budget(
        messages,
        config=ContextBudgetConfig(context_window_tokens=128_000),
    )

    assert fitted is messages
    assert fitted[2]["content"] == tool_content
    assert estimate_messages_tokens(fitted) <= int(128_000 * 0.8)


def test_context_budget_fails_without_truncating_messages() -> None:
    tool_content = "t" * 20_000
    messages = [
        {"role": "system", "content": "s" * 20_000},
        {"role": "user", "content": "u" * 20_000},
        {"role": "tool", "content": tool_content, "tool_call_id": "1"},
    ]

    with pytest.raises(ContextBudgetExceededError, match="were not truncated"):
        ensure_context_budget(
            messages,
            config=ContextBudgetConfig(context_window_tokens=1_000),
        )

    assert messages[2]["content"] == tool_content


def test_reasoning_and_structured_content_are_counted() -> None:
    small = [{"role": "assistant", "content": [{"type": "text", "text": "x"}]}]
    large = [
        {
            "role": "assistant",
            "content": [{"type": "text", "text": "x" * 20_000}],
            "reasoning": "r" * 200_000,
            "reasoning_details": [{"type": "thinking", "text": "z" * 20_000}],
        }
    ]

    assert estimate_messages_tokens(large) > estimate_messages_tokens(small) + 50_000


def test_outgoing_output_cap_never_exceeds_remaining_context() -> None:
    config = ContextBudgetConfig(
        context_window_tokens=256_000,
        max_output_tokens=256_000,
    )

    outgoing_cap = config.output_tokens_for_input(config.token_budget)

    assert outgoing_cap == config.output_reserve_tokens
    assert outgoing_cap < config.max_output_tokens
    assert (
        config.token_budget + outgoing_cap + config.safety_margin_tokens
        <= config.context_window_tokens
    )


def test_provider_estimate_avoids_internal_duplicates() -> None:
    runtime = LlmRuntimeConfig(
        protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        base_url="https://example.invalid",
        api_key="test",
        model="test-model",
    )
    messages = [
        {
            "role": "assistant",
            "content": "brief reply",
            "content_blocks": (TextContent("x" * 8_000),),
        }
    ]
    tools = [
        {
            "name": "lookup",
            "description": "Look up a value.",
            "parameters": {"type": "object", "properties": {}},
        }
    ]

    transformed_messages = estimate_provider_request_tokens(
        messages,
        [],
        protocol=runtime.protocol,
        model=runtime.model,
        provider_config=runtime.provider_config,
    )
    transformed_with_tools = estimate_provider_request_tokens(
        messages,
        tools,
        protocol=runtime.protocol,
        model=runtime.model,
        provider_config=runtime.provider_config,
    )

    assert transformed_messages < estimate_messages_tokens(messages)
    assert transformed_with_tools > transformed_messages
