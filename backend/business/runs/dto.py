"""DTOs returned by run application use cases."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from backend.business.runs import StrategyRun, metrics_from_trace_payload
from backend.business.runs.view import project_run_trace

SummaryRenderMode = Literal["markdown", "html"]


@dataclass(frozen=True, slots=True)
class RunSummaryDTO:
    run_id: int
    task_id: int
    trigger_source: str
    schedule_id: int | None
    status: str
    current_state: str
    summary: str | None
    summary_render_mode: SummaryRenderMode
    started_at: datetime
    completed_at: datetime | None
    tool_calls_count: int
    thinking_count: int
    total_tokens: int
    trade_count: int


@dataclass(frozen=True, slots=True)
class RunDetailDTO(RunSummaryDTO):
    trace: dict[str, object]
    failure_reason: str | None = None


def _trace_metrics(run: StrategyRun) -> tuple[int, int, int, int]:
    return metrics_from_trace_payload(run.trace.as_dict())


def to_run_summary_dto(run: StrategyRun) -> RunSummaryDTO:
    tool_calls_count, thinking_count, total_tokens, trade_count = _trace_metrics(run)
    return RunSummaryDTO(
        run_id=run.run_id,
        task_id=run.run_id,
        trigger_source=run.trigger_source.value,
        schedule_id=run.schedule_id,
        status=run.status.value,
        current_state=run.current_state.value,
        summary=run.summary,
        summary_render_mode=run.summary_render_mode,  # type: ignore[arg-type]
        started_at=run.started_at,
        completed_at=run.completed_at,
        tool_calls_count=tool_calls_count,
        thinking_count=thinking_count,
        total_tokens=total_tokens,
        trade_count=trade_count,
    )


def run_summary_dto_from_row(row: dict[str, object]) -> RunSummaryDTO:
    run_id = int(str(row["run_id"]))
    schedule_raw = row.get("schedule_id")
    summary_raw = row.get("summary")
    return RunSummaryDTO(
        run_id=run_id,
        task_id=run_id,
        trigger_source=str(row["trigger_source"]),
        schedule_id=None if schedule_raw is None else int(str(schedule_raw)),
        status=str(row["status"]),
        current_state=str(row["current_state"]),
        summary=None if summary_raw is None else str(summary_raw),
        summary_render_mode=str(row.get("summary_render_mode") or "markdown"),  # type: ignore[arg-type]
        started_at=row["started_at"],  # type: ignore[arg-type]
        completed_at=row.get("completed_at"),  # type: ignore[arg-type]
        tool_calls_count=int(str(row["tool_calls_count"])),
        thinking_count=int(str(row["thinking_count"])),
        total_tokens=int(str(row["total_tokens"])),
        trade_count=int(str(row["trade_count"])),
    )


def to_run_detail_dto(run: StrategyRun) -> RunDetailDTO:
    summary = to_run_summary_dto(run)
    return RunDetailDTO(
        **{
            field_name: getattr(summary, field_name)
            for field_name in summary.__dataclass_fields__
        },
        trace=project_run_trace(run.trace),
        failure_reason=run.failure_reason,
    )
