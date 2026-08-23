"""Generic tool-call normalization, authorization, and execution."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Literal, Protocol

from backend.agent.actors.tool_loop import ToolLoopResult, ToolLoopStatus
from backend.agent.events import AgentEventType
from backend.agent.json import json_safe
from backend.agent.kernel.context import AgentContext
from backend.llm import LLMToolCall, ProviderJsonObject


class ToolCallRegistry(Protocol):
    def get(self, name: str) -> object: ...

    async def call(self, name: str, **kwargs: object) -> object: ...


def normalize_tool_call(raw_call: object, index: int) -> LLMToolCall:
    if not isinstance(raw_call, dict):
        return {"id": f"tool-call-{index}", "name": "", "arguments": {}}
    raw_id = str(raw_call.get("id", "")).strip()
    arguments = raw_call.get("arguments", {})
    return {
        "id": raw_id or f"tool-call-{index}",
        "name": str(raw_call.get("name", "")).strip(),
        "arguments": arguments if isinstance(arguments, dict) else {},
    }


def trace_tool_arguments(
    registry: object | None,
    tool_name: str,
    arguments: object,
) -> ProviderJsonObject:
    safe_arguments = json_safe(arguments)
    fallback = safe_arguments if isinstance(safe_arguments, dict) else {}
    getter = getattr(registry, "get", None)
    if not callable(getter):
        return fallback
    try:
        tool = getter(tool_name)
    except Exception:
        return fallback
    presenter = getattr(tool, "trace_arguments", None)
    if not callable(presenter):
        return fallback
    try:
        presented = json_safe(presenter(arguments))
    except Exception:
        return fallback
    return presented if isinstance(presented, dict) else fallback


async def emit_agent_event(
    context: AgentContext,
    event_type: AgentEventType,
    payload: dict[str, object],
) -> None:
    if context.event_sink is not None:
        await context.event_sink(event_type.value, payload)


def event_type_for_result(result: ToolLoopResult) -> AgentEventType:
    if result.status == "ok":
        return AgentEventType.TOOL_CALL_COMPLETED
    if result.status == "blocked":
        return AgentEventType.TOOL_CALL_BLOCKED
    return AgentEventType.TOOL_CALL_FAILED


def build_call_result(
    *,
    tool_call_id: str,
    tool_name: str,
    arguments: ProviderJsonObject,
    status: ToolLoopStatus,
    iteration: int,
    sequence: int,
    content: object | None = None,
    details: dict[str, object] | None = None,
    error: str | None = None,
) -> ToolLoopResult:
    return ToolLoopResult(
        tool_call_id=tool_call_id,
        tool_name=tool_name,
        arguments=arguments,
        status=status,
        content=content,
        details=details or {},
        error=error,
        iteration=iteration,
        sequence=sequence,
    )


async def execute_tool_call(
    context: AgentContext,
    registry: ToolCallRegistry | None,
    raw_call: object,
    *,
    normalized_call: LLMToolCall,
    iteration: int,
    sequence: int,
) -> ToolLoopResult:
    tool_call_id = str(normalized_call.get("id", "")).strip()

    def result(
        *,
        tool_name: str,
        arguments: ProviderJsonObject,
        status: ToolLoopStatus,
        content: object | None = None,
        details: dict[str, object] | None = None,
        error: str | None = None,
    ) -> ToolLoopResult:
        return build_call_result(
            tool_call_id=tool_call_id,
            tool_name=tool_name,
            arguments=arguments,
            status=status,
            iteration=iteration,
            sequence=sequence,
            content=content,
            details=details,
            error=error,
        )

    if not isinstance(raw_call, dict):
        return result(
            tool_name="",
            arguments={},
            status="error",
            error="tool call must be an object",
        )

    tool_name = str(raw_call.get("name", "")).strip()
    arguments = raw_call.get("arguments", {})
    if not isinstance(arguments, dict):
        return result(
            tool_name=tool_name,
            arguments={},
            status="error",
            content=arguments,
            error="arguments must be an object",
        )
    safe_args = json_safe(arguments)
    if not isinstance(safe_args, dict):
        safe_args = {}
    if registry is None:
        return result(
            tool_name=tool_name,
            arguments=safe_args,
            status="error",
            error="tool registry is not configured",
        )
    try:
        tool = registry.get(tool_name)
    except Exception:
        tool = None
    if tool is None:
        return result(
            tool_name=tool_name,
            arguments=safe_args,
            status="blocked",
            error="tool is not available",
        )
    safe_args = trace_tool_arguments(registry, tool_name, arguments)
    if context.tool_authorizer is not None:
        reason = context.tool_authorizer(tool, arguments)
        if reason:
            return result(
                tool_name=tool_name,
                arguments=safe_args,
                status="blocked",
                error=reason,
            )

    def stock_api_details() -> dict[str, object]:
        reader = getattr(registry, "stock_api_call_details", None)
        if not callable(reader):
            return {}
        raw_details = reader(tool_call_id)
        if not isinstance(raw_details, (list, tuple)) or not raw_details:
            return {}
        return {
            "stock_api_calls": [
                dict(item) for item in raw_details if isinstance(item, dict)
            ]
        }

    try:
        call_idempotently = getattr(registry, "call_idempotently", None)
        if callable(call_idempotently):
            call_result = await call_idempotently(
                tool_name,
                tool_call_id=tool_call_id,
                abort_signal=context.abort_signal,
                **arguments,
            )
        else:
            call_with_abort = getattr(registry, "call_with_abort", None)
            if callable(call_with_abort):
                call_result = await call_with_abort(
                    tool_name,
                    abort_signal=context.abort_signal,
                    **arguments,
                )
            else:
                call_result = await registry.call(tool_name, **arguments)
    except Exception as exc:
        return result(
            tool_name=tool_name,
            arguments=safe_args,
            status="error",
            details=stock_api_details(),
            error=str(exc),
        )
    if (
        isinstance(call_result, dict)
        and str(call_result.get("status") or "") == "error"
    ):
        return result(
            tool_name=tool_name,
            arguments=safe_args,
            status="error",
            content=call_result,
            details=stock_api_details(),
            error=str(call_result.get("error") or "tool returned error"),
        )
    return result(
        tool_name=tool_name,
        arguments=safe_args,
        status="ok",
        content=call_result,
        details=stock_api_details(),
    )


ToolExecutionMode = Literal["parallel", "sequential"]
ToolResultCallback = Callable[[ToolLoopResult], Awaitable[None]]


def _tool_execution_mode(
    registry: ToolCallRegistry | None,
    normalized_call: LLMToolCall,
) -> ToolExecutionMode:
    if registry is None:
        return "parallel"
    try:
        tool = registry.get(str(normalized_call.get("name") or ""))
    except Exception:
        return "parallel"
    resolver = getattr(tool, "execution_mode_for", None)
    if callable(resolver):
        mode = resolver(normalized_call.get("arguments", {}))
    else:
        mode = getattr(tool, "execution_mode", "parallel")
    return "sequential" if str(mode) == "sequential" else "parallel"


def _aborted_result(
    normalized_call: LLMToolCall,
    *,
    iteration: int,
    sequence: int,
) -> ToolLoopResult:
    arguments = normalized_call.get("arguments", {})
    safe_arguments = json_safe(arguments)
    return build_call_result(
        tool_call_id=str(normalized_call.get("id") or ""),
        tool_name=str(normalized_call.get("name") or ""),
        arguments=safe_arguments if isinstance(safe_arguments, dict) else {},
        status="error",
        error="operation aborted before tool execution",
        iteration=iteration,
        sequence=sequence,
    )


async def execute_tool_call_batch(
    context: AgentContext,
    registry: ToolCallRegistry | None,
    calls: list[tuple[object, LLMToolCall, int]],
    *,
    iteration: int,
    max_concurrency: int,
    on_result: ToolResultCallback | None = None,
) -> list[ToolLoopResult]:
    """Prepare in source order, execute by policy, return source-ordered results."""

    sequential = any(
        _tool_execution_mode(registry, normalized_call) == "sequential"
        for _, normalized_call, _ in calls
    )
    semaphore = asyncio.Semaphore(1 if sequential else max(1, max_concurrency))
    result_callback_lock = asyncio.Lock()

    async def execute_one(
        raw_call: object, normalized_call: LLMToolCall, sequence: int
    ) -> ToolLoopResult:
        async with semaphore:
            signal = context.abort_signal
            if signal is not None and signal.aborted:
                result = _aborted_result(
                    normalized_call,
                    iteration=iteration,
                    sequence=sequence,
                )
            else:
                result = await execute_tool_call(
                    context,
                    registry,
                    raw_call,
                    normalized_call=normalized_call,
                    iteration=iteration,
                    sequence=sequence,
                )
            if on_result is not None:
                async with result_callback_lock:
                    await on_result(result)
            return result

    if sequential:
        results: list[ToolLoopResult] = []
        for raw_call, normalized_call, sequence in calls:
            results.append(await execute_one(raw_call, normalized_call, sequence))
        return results
    return await asyncio.gather(
        *(
            execute_one(raw_call, normalized_call, sequence)
            for raw_call, normalized_call, sequence in calls
        )
    )
