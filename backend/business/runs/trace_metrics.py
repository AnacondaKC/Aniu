"""Lightweight metrics derived from raw run-trace payloads."""

from __future__ import annotations

import json
from typing import Any

_TRACE_STAGE_STATUSES = frozenset(
    {"pending", "running", "completed", "degraded", "failed", "skipped"}
)


def _token_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _step_metrics(
    step_type: object,
    content: object,
    data: object,
) -> tuple[int, int, int]:
    tool_calls = 1 if step_type == "tool" else 0
    thinking = 1 if step_type == "thinking" else 0
    characters = 0
    payload = data if isinstance(data, dict) else {}
    if step_type == "tool":
        characters += len(_token_text(payload.get("arguments")))
        model_content_characters = payload.get("model_content_characters")
        if type(model_content_characters) is int and model_content_characters >= 0:
            characters += model_content_characters
        else:
            characters += len(_token_text(payload.get("result")))
        characters += len(_token_text(payload.get("error")))
    elif step_type == "thinking":
        characters += len(content if isinstance(content, str) else "")
    elif step_type == "prompt":
        prompt_text = "".join(
            (
                _token_text(payload.get("system_message")),
                _token_text(payload.get("user_message")),
            )
        )
        characters += len(prompt_text or (content if isinstance(content, str) else ""))
    elif step_type == "result":
        if isinstance(content, str) and content:
            characters += len(content)
        else:
            characters += len(_token_text(payload.get("summary")))
    return tool_calls, thinking, characters


def _completed_trade_count(stage: dict[str, Any]) -> int:
    if stage.get("key") != "run" or stage.get("status") != "completed":
        return 0
    steps = stage.get("steps")
    if not isinstance(steps, list):
        return 0
    for step in reversed(steps):
        if not isinstance(step, dict) or step.get("type") != "result":
            continue
        data = step.get("data")
        count = data.get("trade_count") if isinstance(data, dict) else None
        if type(count) is int and count >= 0:
            return count
    return 0


def run_stage_status_from_trace_payload(trace: dict[str, Any] | None) -> str | None:
    stages = (trace or {}).get("stages")
    if not isinstance(stages, list):
        return None
    for stage in reversed(stages):
        if not isinstance(stage, dict) or stage.get("key") != "run":
            continue
        status = stage.get("status")
        return (
            status
            if isinstance(status, str) and status in _TRACE_STAGE_STATUSES
            else None
        )
    return None


def metrics_from_trace_payload(
    trace: dict[str, Any] | None,
) -> tuple[int, int, int, int]:
    tool_calls_count = 0
    thinking_count = 0
    token_characters = 0
    trade_count = 0
    stages = (trace or {}).get("stages")
    if not isinstance(stages, list):
        return 0, 0, 0, 0
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        trade_count += _completed_trade_count(stage)
        steps = stage.get("steps")
        if not isinstance(steps, list):
            continue
        for step in steps:
            if not isinstance(step, dict):
                continue
            tool, thinking, characters = _step_metrics(
                step.get("type"),
                step.get("content"),
                step.get("data"),
            )
            tool_calls_count += tool
            thinking_count += thinking
            token_characters += characters
    estimated_tokens = (
        max(1, (token_characters + 3) // 4) if token_characters > 0 else 0
    )
    return tool_calls_count, thinking_count, estimated_tokens, trade_count


__all__ = ["metrics_from_trace_payload", "run_stage_status_from_trace_payload"]
