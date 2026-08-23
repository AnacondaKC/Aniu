"""Provider usage normalization."""

from __future__ import annotations

from backend.llm.contracts import Usage


def _integer(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return max(0, value)
    if isinstance(value, float):
        return max(0, int(value))
    return 0


def openai_usage(value: object) -> Usage:
    if value is None:
        return Usage()
    dump = getattr(value, "model_dump", None)
    raw = dump() if callable(dump) else value
    if not isinstance(raw, dict):
        return Usage()
    prompt_tokens = _integer(raw.get("prompt_tokens") or raw.get("input_tokens"))
    output_tokens = _integer(raw.get("completion_tokens") or raw.get("output_tokens"))
    prompt_details = raw.get("prompt_tokens_details")
    completion_details = raw.get("completion_tokens_details")
    cache_read = (
        _integer(prompt_details.get("cached_tokens"))
        if isinstance(prompt_details, dict)
        else 0
    )
    reasoning = (
        _integer(completion_details.get("reasoning_tokens"))
        if isinstance(completion_details, dict)
        and completion_details.get("reasoning_tokens") is not None
        else None
    )
    input_tokens = max(0, prompt_tokens - cache_read)
    total = _integer(raw.get("total_tokens")) or (
        input_tokens + output_tokens + cache_read
    )
    return Usage(
        input=input_tokens,
        output=output_tokens,
        cache_read=cache_read,
        reasoning=reasoning,
        total_tokens=total,
    )


def anthropic_usage(value: object) -> Usage:
    if value is None:
        return Usage()
    dump = getattr(value, "model_dump", None)
    raw = dump() if callable(dump) else value
    if not isinstance(raw, dict):
        return Usage()
    input_tokens = _integer(raw.get("input_tokens"))
    output_tokens = _integer(raw.get("output_tokens"))
    cache_read = _integer(raw.get("cache_read_input_tokens"))
    cache_write = _integer(raw.get("cache_creation_input_tokens"))
    return Usage(
        input=input_tokens,
        output=output_tokens,
        cache_read=cache_read,
        cache_write=cache_write,
        total_tokens=input_tokens + output_tokens + cache_read + cache_write,
    )


__all__ = ["anthropic_usage", "openai_usage"]
