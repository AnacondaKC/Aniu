"""Summary input assembly and deterministic evidence budgeting."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy

from backend.business.runs.execution import RunReport
from backend.business.shared.serialization import serialize_context
from backend.llm import estimate_tokens

_WRITE_TOOL_NAMES = frozenset({"trade", "cancel"})
_MIN_TOOL_EXCERPT_CHARS = 512
_DEFAULT_TOOL_EXCERPT_CHARS = 8_000


class SummaryEvidenceTooLargeError(ValueError):
    """Required report or execution evidence cannot fit the summary context."""


def _reasoning_segments(report: RunReport) -> list[str]:
    segments: list[str] = []
    for message in report.transcript:
        if message.get("role") != "assistant":
            continue
        reasoning = message.get("reasoning")
        if reasoning is None:
            continue
        text = (
            reasoning.strip()
            if isinstance(reasoning, str)
            else serialize_context(reasoning)
        )
        if text:
            segments.append(text)
    return segments


def _tool_record(activity: dict[str, object]) -> dict[str, object]:
    record = deepcopy(activity)
    # ``result`` duplicates ``content`` in successful activity records.
    record.pop("result", None)
    record.pop("stage_name", None)
    return record


def _is_required_tool_record(record: dict[str, object]) -> bool:
    return (
        str(record.get("tool_name") or "") in _WRITE_TOOL_NAMES
        or str(record.get("status") or "") != "ok"
    )


def _truncate_tool_content(record: dict[str, object], limit: int) -> None:
    content = record.get("content")
    rendered = serialize_context(content)
    if limit > 0 and len(rendered) <= limit:
        return
    if limit <= 0:
        record["content"] = {
            "truncated": True,
            "original_characters": len(rendered),
            "omitted": "查询工具返回已省略",
        }
        record.pop("details", None)
        return
    head = max(1, limit * 2 // 3)
    tail = max(1, limit - head)
    record["content"] = {
        "truncated": True,
        "original_characters": len(rendered),
        "retained_characters": head + tail,
        "excerpt": rendered[:head] + "\n...[工具返回已截断]...\n" + rendered[-tail:],
    }
    details = record.get("details")
    if isinstance(details, dict):
        record["details"] = {
            key: value
            for key, value in details.items()
            if key in {"stock_api_calls"}
        }


def _payload(
    report: RunReport,
    reasoning: list[dict[str, object]],
    tools: list[dict[str, object]],
    omissions: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "run_report_markdown": report.content,
        "reasoning_segments": reasoning,
        "tool_calls": tools,
    }
    if omissions:
        payload["evidence_omissions"] = omissions
    return payload


def build_summary_stage_payload(
    report: RunReport,
    *,
    max_characters: int | None = None,
    max_tokens: int | None = None,
) -> dict[str, object]:
    """Build the richest payload that fits without clipping required evidence."""

    fits = _payload_fits_budget(
        max_characters=max_characters,
        max_tokens=max_tokens,
    )
    if fits is None:
        raise SummaryEvidenceTooLargeError("summary input budget is empty")
    reasoning = [
        {"index": index, "content": text}
        for index, text in enumerate(_reasoning_segments(report), start=1)
    ]
    original_tools = [_tool_record(dict(item)) for item in report.tool_activity]
    required_tools = [
        item for item in original_tools if _is_required_tool_record(item)
    ]
    required_payload = _payload(report, [], required_tools)
    if not fits(required_payload):
        raise SummaryEvidenceTooLargeError(
            "run report and required trade/error evidence exceed summary budget"
        )

    if fits(_payload(report, reasoning, original_tools)):
        return _payload(report, reasoning, original_tools)

    tools = original_tools
    excerpt_limit = _DEFAULT_TOOL_EXCERPT_CHARS
    while True:
        tools = deepcopy(original_tools)
        for record in tools:
            if not _is_required_tool_record(record):
                _truncate_tool_content(record, excerpt_limit)
        current = _payload(report, reasoning, tools)
        if fits(current):
            return current
        if excerpt_limit == 0:
            break
        excerpt_limit = (
            0
            if excerpt_limit <= _MIN_TOOL_EXCERPT_CHARS
            else max(_MIN_TOOL_EXCERPT_CHARS, excerpt_limit // 2)
        )

    omissions: dict[str, object] = {}
    indexed_tools = list(enumerate(tools, start=1))
    removed_tool_indexes: list[int] = []
    while not fits(
        _payload(
            report,
            reasoning,
            [record for _, record in indexed_tools],
            omissions,
        )
    ):
        optional_position = next(
            (
                position
                for position, (_, record) in enumerate(indexed_tools)
                if not _is_required_tool_record(record)
            ),
            None,
        )
        if optional_position is None:
            break
        original_index, _ = indexed_tools.pop(optional_position)
        removed_tool_indexes.append(original_index)
        omissions["query_tool_calls"] = {
            "count": len(removed_tool_indexes),
            "first_activity_index": removed_tool_indexes[0],
            "last_activity_index": removed_tool_indexes[-1],
        }
    tools = [record for _, record in indexed_tools]

    removed_reasoning_indexes: list[int] = []
    while reasoning and not fits(_payload(report, reasoning, tools, omissions)):
        removed = reasoning.pop(0)
        index = removed.get("index")
        if isinstance(index, int):
            removed_reasoning_indexes.append(index)
        omissions["reasoning_segments"] = {
            "count": len(removed_reasoning_indexes),
            "first_index": removed_reasoning_indexes[0],
            "last_index": removed_reasoning_indexes[-1],
        }
    result = _payload(report, reasoning, tools, omissions)
    if not fits(result):
        raise SummaryEvidenceTooLargeError(
            "summary evidence exceeds budget after optional evidence truncation"
        )
    return result


def _payload_fits_budget(
    *,
    max_characters: int | None,
    max_tokens: int | None,
) -> Callable[[dict[str, object]], bool] | None:
    if max_characters is not None and max_tokens is not None:
        raise ValueError("summary budget must use characters or tokens, not both")
    if max_tokens is not None:
        if max_tokens < 1:
            return None
        return lambda payload: estimate_tokens(serialize_context(payload)) <= max_tokens
    if max_characters is None or max_characters < 1:
        return None
    return lambda payload: len(serialize_context(payload)) <= max_characters
