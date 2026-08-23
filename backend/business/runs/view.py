"""Public run-trace projection used by REST and SSE clients."""

from __future__ import annotations

from typing import Any

from backend.business.runs import RunTrace, TraceStage, TraceStep
from backend.business.runs.tool_presentation import (
    summarize_public_tool_arguments,
    tool_display_name,
    tool_source,
)
from backend.business.shared.json_utils import json_safe
from backend.business.shared.serialization import serialize_context
from backend.business.shared.stock_api_source import normalize_public_stock_operation_id

PUBLIC_STEP_TYPES = frozenset({"thinking", "tool", "result", "status"})
TOOL_TARGET_MAX_CHARS = 120
TRACE_STOCK_API_PROVIDERS = frozenset({"mx", "eastmoney", "tencent", "sina"})
_MODEL_TOOL_CONTENT_DROP_KEYS = frozenset(
    {
        "command",
        "duration_ms",
        "location",
        "base_dir",
        "tool_call_id",
        "iteration",
        "sequence",
        "details",
        "arguments",
        "record_type",
    }
)


def _as_record(value: object) -> dict[str, Any] | None:
    return dict(value) if isinstance(value, dict) else None


def _read_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _slim_model_tool_content(content: object) -> object:
    """Mirror the compact result payload appended to an Agent tool message."""

    if not isinstance(content, dict):
        return json_safe(content)
    return {
        str(key): json_safe(value)
        for key, value in content.items()
        if key not in _MODEL_TOOL_CONTENT_DROP_KEYS
    }


def _model_content_characters(step: TraceStep, data: dict[str, Any]) -> int | None:
    """Return the pre-compaction Agent-context length, including legacy traces."""

    recorded = data.get("model_content_characters")
    if type(recorded) is int and recorded >= 0:
        return recorded
    if "result" not in data:
        return None

    result = data["result"]
    if step.status == "completed":
        return len(serialize_context(_slim_model_tool_content(result)))

    tool_name = data.get("tool_name")
    if not isinstance(tool_name, str):
        tool_name = step.title or "工具调用"
    model_content: dict[str, object] = {
        "status": "blocked" if step.status == "blocked" else "error",
        "tool": tool_name,
    }
    error = data.get("error")
    if isinstance(error, str) and error:
        model_content["error"] = error
    if result is not None:
        model_content["content"] = _slim_model_tool_content(result)

    return len(serialize_context(model_content))


def _project_stock_api_calls(data: dict[str, Any]) -> list[dict[str, object]]:
    raw_calls = data.get("stock_api_calls")
    if not isinstance(raw_calls, (list, tuple)):
        return []
    projected: list[dict[str, object]] = []
    for raw_call in raw_calls:
        record = _as_record(raw_call)
        if record is None:
            continue
        provider = _read_text(record.get("provider"))
        operation_id = _read_text(record.get("operation_id"))
        if provider not in TRACE_STOCK_API_PROVIDERS or operation_id is None:
            continue
        if provider == "mx":
            interface_name = _read_text(record.get("interface_name")) or operation_id
            interface_identifier = (
                _read_text(record.get("interface_identifier")) or operation_id
            )
        else:
            operation_id = normalize_public_stock_operation_id(operation_id)
            interface_name = "公开数据"
            interface_identifier = operation_id
        response_characters = record.get("response_characters")
        projected.append(
            {
                "call_id": _read_text(record.get("call_id")) or "",
                "provider": provider,
                "interface_name": interface_name,
                "interface_identifier": interface_identifier,
                "operation_id": operation_id,
                "parameters": record.get("parameters") if provider == "mx" else {},
                "response_characters": (
                    response_characters
                    if isinstance(response_characters, int)
                    else None
                ),
                "status": _read_text(record.get("status")) or "failed",
                "duration_ms": (
                    record.get("duration_ms")
                    if isinstance(record.get("duration_ms"), int)
                    else 0
                ),
                "error_message": _read_text(record.get("error_message")),
            }
        )
    return projected


def _tool_target_text(value: object) -> str | None:
    text = _read_text(value)
    if text is not None:
        return text
    if not isinstance(value, (list, tuple)):
        return None
    items = [item.strip() for item in value if isinstance(item, str) and item.strip()]
    if not items:
        return None
    rendered = "、".join(items[:6])
    return f"{rendered} 等" if len(items) > 6 else rendered


def _public_tool_target(arguments: object) -> str | None:
    record = _as_record(arguments)
    if record is None:
        return None
    for key in (
        "instrument",
        "symbols",
        "symbol",
        "query",
        "keyword",
        "instruction",
        "stock_code",
        "stock_codes",
        "index_code",
    ):
        value = _tool_target_text(record.get(key))
        if value is not None:
            normalized = " ".join(value.split())
            if len(normalized) <= TOOL_TARGET_MAX_CHARS:
                return normalized
            return normalized[: TOOL_TARGET_MAX_CHARS - 1].rstrip() + "…"
    security = _as_record(record.get("security"))
    if security is not None:
        return _read_text(security.get("code"))
    return None


def _tool_intent_line(step: TraceStep) -> str:
    data = dict(step.data or {})
    tool_name = _read_text(data.get("tool_name")) or step.title or "工具调用"
    label = tool_display_name(tool_name)
    if label == tool_name and step.title != "工具调用":
        label = step.title
    arguments = data.get("arguments")
    target = _public_tool_target(arguments)
    return f"{label} · {target}" if target is not None else label


def _project_step(step: TraceStep) -> dict[str, object]:
    tool_call: dict[str, object] | None = None
    if step.type == "tool":
        intent_line = _tool_intent_line(step)
        data = dict(step.data or {})
        tool_name = _read_text(data.get("tool_name")) or step.title or "工具调用"
        arguments = data.get("arguments")
        argument_record = _as_record(arguments)
        display_name = tool_display_name(tool_name)
        if tool_name == "call_interface" and argument_record is not None:
            display_name = (
                _read_text(argument_record.get("interface_name")) or display_name
            )
        tool_call = {
            "call_id": _read_text(data.get("tool_call_id")) or step.step_id,
            "intent_line": intent_line,
            "source": tool_source(tool_name),
            "tool_name": tool_name,
            "display_name": display_name,
            "query_parameters": summarize_public_tool_arguments(
                tool_name, argument_record
            ),
        }
        stock_api_calls = _project_stock_api_calls(data)
        if stock_api_calls:
            tool_call["stock_api_calls"] = stock_api_calls
            model_content_characters = _model_content_characters(step, data)
            if model_content_characters is not None:
                tool_call["model_content_characters"] = model_content_characters
    return {
        "step_id": step.step_id,
        "type": step.type,
        "title": step.title,
        "status": step.status,
        "summary": step.summary if step.type != "tool" else None,
        "content": step.content if step.type in {"thinking", "result"} else None,
        "tool_call": tool_call,
        "started_at": step.started_at.isoformat()
        if step.started_at is not None
        else None,
        "ended_at": step.ended_at.isoformat() if step.ended_at is not None else None,
    }


def _project_stage_steps(stage: TraceStage) -> list[dict[str, object]]:
    return [
        _project_step(step) for step in stage.steps if step.type in PUBLIC_STEP_TYPES
    ]


def project_run_trace(trace: RunTrace) -> dict[str, object]:
    """Return the minimal trace contract required by the run workbench."""

    return {
        "schema_version": trace.schema_version,
        "event_seq": trace.event_seq,
        "current_stage_id": trace.current_stage_id,
        "stages": [
            {
                "stage_id": stage.stage_id,
                "key": stage.key,
                "status": stage.status,
                "started_at": stage.started_at.isoformat()
                if stage.started_at is not None
                else None,
                "ended_at": stage.ended_at.isoformat()
                if stage.ended_at is not None
                else None,
                "steps": _project_stage_steps(stage),
            }
            for stage in trace.stages
        ],
    }


__all__ = ["project_run_trace"]
