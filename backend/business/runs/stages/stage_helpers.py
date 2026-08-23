"""Shared helpers for stage prompt preparation and stage events."""

from __future__ import annotations

from backend.business.runs import RunEventType
from backend.business.runs.execution import RunExecutionContext
from backend.business.shared import ServiceConfigurationError

__all__ = [
    "emit_stage_prompt_prepared",
    "emit_tool_loop_event",
    "require_llm_runtime",
]


def require_llm_runtime(context: RunExecutionContext, *, stage_name: str) -> None:
    """Fail fast when a stage's main LLM runtime is not configured.

    Production stages read the runtime from ``context.llm_runtime``;
    without it, they would emit prompt-prepared events first and only fail inside
    the LLM call. This guard raises before any emit so callers see the
    misconfiguration immediately.
    """

    runtime = context.llm_runtime
    if runtime is None:
        raise ServiceConfigurationError(
            f"{stage_name} stage requires configured llm runtime"
        )


async def emit_stage_prompt_prepared(
    context: RunExecutionContext,
    *,
    stage_name: str,
    phase: str,
    title: str,
    summary: str,
    display_prompt: str,
    payload: dict[str, object] | None = None,
    user_message: str | None = None,
) -> None:
    """Notify the host that a stage prompt is ready for the model."""

    sink = context.stage_prompt_prepared_sink
    if not callable(sink):
        return
    body: dict[str, object] = {
        "stage_name": stage_name,
        "phase": phase,
        "title": title,
        "summary": summary,
        "prompt": display_prompt,
        "prompt_chars": len(display_prompt),
    }
    if payload is not None:
        body["payload"] = payload
    if user_message is not None:
        body["user_message"] = user_message
    await sink(stage_name, body)


async def emit_tool_loop_event(
    context: RunExecutionContext,
    *,
    stage_name: str,
    event_type: RunEventType,
    payload: dict[str, object],
) -> None:
    """Forward a stage-local event to the host tool-loop sink when present."""

    sink = context.tool_loop_event_sink
    if not callable(sink):
        return
    await sink(stage_name, event_type, payload)
