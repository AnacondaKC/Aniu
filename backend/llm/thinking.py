"""Normalized thinking-effort presets shared by settings and provider drivers."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

ThinkingEffort = Literal["minimal", "low", "medium", "high", "xhigh", "max"]
THINKING_EFFORTS: tuple[ThinkingEffort, ...] = (
    "minimal",
    "low",
    "medium",
    "high",
    "xhigh",
    "max",
)

# Budget-based providers require a minimum reasoning budget. The provider driver
# caps these values against the request's configured output allowance.
THINKING_EFFORT_TOKEN_BUDGETS: dict[ThinkingEffort, int] = {
    "minimal": 1_024,
    "low": 2_048,
    "medium": 4_096,
    "high": 8_192,
    "xhigh": 16_384,
    "max": 32_768,
}


def coerce_thinking_effort(value: object | None) -> ThinkingEffort | None:
    """Validate one optional preset value."""

    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("thinking_effort must be text or null")
    normalized = value.strip().lower()
    if normalized not in THINKING_EFFORTS:
        raise ValueError(f"unsupported thinking_effort: {value}")
    return normalized


def normalize_thinking_efforts(value: object | None) -> tuple[ThinkingEffort, ...]:
    """Canonicalize one model's enabled preset list."""

    if value is None:
        return ()
    if isinstance(value, str) or not isinstance(value, Iterable):
        raise ValueError("thinking_efforts must be an array")
    selected = {coerce_thinking_effort(item) for item in value}
    if None in selected:
        raise ValueError("thinking_efforts cannot contain null")
    return tuple(effort for effort in THINKING_EFFORTS if effort in selected)


def uses_adaptive_anthropic_thinking(model_name: str) -> bool:
    """Return whether a Claude-compatible model requires adaptive thinking."""

    model = model_name.lower()
    return any(
        marker in model
        for marker in (
            "opus-4-6",
            "opus-4.6",
            "opus-4-7",
            "opus-4.7",
            "opus-4-8",
            "opus-4.8",
            "opus-5",
            "opus.5",
            "sonnet-4-6",
            "sonnet-4.6",
            "sonnet-5",
            "sonnet.5",
            "fable-5",
            "kimi-for-coding",
            "kimi-k3",
        )
    )


def anthropic_adaptive_effort(effort: ThinkingEffort) -> str:
    """Map the shared preset vocabulary to Anthropic adaptive effort values."""

    return "low" if effort in {"minimal", "low"} else effort


def thinking_budget_tokens(effort: ThinkingEffort, max_output_tokens: int) -> int:
    """Return a valid Anthropic-style thinking budget for one output cap."""

    available = max_output_tokens - 1_024
    if available < 1_024:
        raise ValueError(
            "max_output_tokens must be at least 2048 when thinking is enabled"
        )
    return min(THINKING_EFFORT_TOKEN_BUDGETS[effort], available)


__all__ = [
    "THINKING_EFFORTS",
    "THINKING_EFFORT_TOKEN_BUDGETS",
    "ThinkingEffort",
    "coerce_thinking_effort",
    "normalize_thinking_efforts",
    "anthropic_adaptive_effort",
    "thinking_budget_tokens",
    "uses_adaptive_anthropic_thinking",
]
