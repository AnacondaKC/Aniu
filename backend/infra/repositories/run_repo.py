"""Repositories for strategy runs and their trace snapshots."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.business.runs import (
    INITIAL_STATE,
    RUN_TASK_TYPE,
    RunReportRecord,
    RunTrace,
    StrategyRun,
    StrategySnapshot,
    bound_trace_payload,
    metrics_from_trace_payload,
)
from backend.business.runs.job import RunJobStatus
from backend.business.settings import AniuAgentPrompt
from backend.business.shared.enums import RunState, RunStatus, TriggerSource
from backend.infra.db.models import RunJobModel, StrategyRunModel
from backend.infra.repositories.task_numbering import next_task_id

_MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")


def _trace_projection_and_metrics(
    run: StrategyRun,
) -> tuple[dict[str, object], tuple[int, int, int, int]]:
    """Serialize trace once for persistence and lightweight list projections."""

    payload = run.trace.as_dict()
    return bound_trace_payload(payload), metrics_from_trace_payload(payload)


def _extract_run_report(trace: dict[str, Any]) -> str | None:
    for stage in trace.get("stages", []):
        if not isinstance(stage, dict):
            continue
        if stage.get("stage_id") != "Run" and stage.get("key") != "run":
            continue
        steps = stage.get("steps", [])
        if not isinstance(steps, list):
            continue
        for step in reversed(steps):
            if not isinstance(step, dict) or step.get("type") != "result":
                continue
            content = step.get("content")
            if isinstance(content, str) and content.strip():
                return content
    return None


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _deserialize_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _serialize_snapshot(snapshot: StrategySnapshot) -> dict[str, object]:
    return {
        "prompt_version": snapshot.prompt_version,
        "risk_rules_version": snapshot.risk_rules_version,
        "prompt_profile": snapshot.prompt_profile.as_dict(),
        "stage_settings": {
            stage_id: settings.as_dict()
            for stage_id, settings in snapshot.stage_settings.items()
        },
        "stage_models": {
            stage_id: model.as_dict()
            for stage_id, model in snapshot.stage_models.items()
        },
        "captured_at": snapshot.captured_at.isoformat(),
    }


def _run_values(run: StrategyRun) -> dict[str, object]:
    trace_json, metrics = _trace_projection_and_metrics(run)
    tool_calls, thinking, tokens, trade_count = metrics
    return {
        "trigger_source": run.trigger_source.value,
        "schedule_id": run.schedule_id,
        "status": run.status.value,
        "current_state": run.current_state.value,
        "snapshot_json": _serialize_snapshot(run.snapshot),
        "trace_json": trace_json,
        "summary": run.summary,
        "summary_render_mode": run.summary_render_mode,
        "failure_reason": run.failure_reason,
        "tool_calls_count": tool_calls,
        "thinking_count": thinking,
        "total_tokens": tokens,
        "trade_count": trade_count,
        "started_at": run.started_at.isoformat(),
        "completed_at": _serialize_datetime(run.completed_at),
    }


def _deserialize_snapshot(payload: dict[str, Any]) -> StrategySnapshot:
    return StrategySnapshot(
        prompt_version=str(payload["prompt_version"]),
        risk_rules_version=str(payload["risk_rules_version"]),
        prompt_profile=AniuAgentPrompt.from_mapping(payload["prompt_profile"]),
        stage_settings=payload["stage_settings"],
        stage_models=payload["stage_models"],
        captured_at=datetime.fromisoformat(str(payload["captured_at"])),
    )


def _deserialize_trace(payload: dict[str, Any] | None) -> RunTrace:
    return RunTrace.from_mapping(payload)


class RunRepository:
    """Persistence adapter for strategy runs and their trace snapshots."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def next_run_id(
        self,
        reference_date: date | None = None,
        task_type: int = RUN_TASK_TYPE,
    ) -> int:
        return await next_task_id(
            self._session,
            reference_date,
            task_type=task_type,
        )

    async def get_running_run(self) -> StrategyRun | None:
        statement = (
            select(StrategyRunModel)
            .where(StrategyRunModel.status == RunStatus.RUNNING.value)
            .order_by(StrategyRunModel.id.asc())
            .limit(1)
        )
        model = (await self._session.scalars(statement)).first()
        if model is None:
            return None
        return self._to_domain(model)

    async def add(self, run: StrategyRun) -> StrategyRun:
        trace_json, metrics = _trace_projection_and_metrics(run)
        tool_calls, thinking, tokens, trade_count = metrics
        model = StrategyRunModel(
            id=run.run_id,
            trigger_source=run.trigger_source.value,
            schedule_id=run.schedule_id,
            status=run.status.value,
            current_state=run.current_state.value,
            snapshot_json=_serialize_snapshot(run.snapshot),
            trace_json=trace_json,
            summary=run.summary,
            summary_render_mode=run.summary_render_mode,
            failure_reason=run.failure_reason,
            tool_calls_count=tool_calls,
            thinking_count=thinking,
            total_tokens=tokens,
            trade_count=trade_count,
            started_at=run.started_at.isoformat(),
            completed_at=_serialize_datetime(run.completed_at),
        )
        self._session.add(model)
        await self._session.flush()
        return self._to_domain(model)

    async def save(self, run: StrategyRun) -> StrategyRun:
        model = await self._session.get(StrategyRunModel, run.run_id)
        if model is None:
            return await self.add(run)

        for field, value in _run_values(run).items():
            setattr(model, field, value)
        await self._session.flush()
        return self._to_domain(model)

    async def save_fenced(
        self,
        run: StrategyRun,
        *,
        worker_id: str,
        claim_token: str,
        now: datetime | None = None,
    ) -> bool:
        """Persist a run only while its worker claim is still live."""

        now_iso = (now or datetime.now(tz=UTC)).isoformat()
        claim_exists = (
            select(RunJobModel.run_id)
            .where(
                RunJobModel.run_id == run.run_id,
                RunJobModel.worker_id == worker_id,
                RunJobModel.claim_token == claim_token,
                RunJobModel.status.in_(
                    (
                        RunJobStatus.LEASED.value,
                        RunJobStatus.CANCEL_REQUESTED.value,
                    )
                ),
                RunJobModel.lease_expires_at.is_not(None),
                RunJobModel.lease_expires_at > now_iso,
            )
            .exists()
        )
        result = await self._session.execute(
            update(StrategyRunModel)
            .where(
                StrategyRunModel.id == run.run_id,
                claim_exists,
            )
            .values(**_run_values(run))
        )
        if getattr(result, "rowcount", None) != 1:
            return False
        await self._session.flush()
        return True

    async def get_by_id(self, run_id: int) -> StrategyRun | None:
        model = await self._session.get(StrategyRunModel, run_id)
        if model is None:
            return None
        return self._to_domain(model)

    def _list_statement(
        self,
        *,
        limit: int,
        offset: int,
        started_date: date | None,
        columns: tuple[Any, ...] | None = None,
    ) -> Any:
        statement = select(*(columns or (StrategyRunModel,)))
        if started_date is not None:
            start_at = datetime.combine(started_date, time.min, tzinfo=UTC).isoformat()
            end_at = datetime.combine(
                started_date + timedelta(days=1),
                time.min,
                tzinfo=UTC,
            ).isoformat()
            statement = statement.where(
                StrategyRunModel.started_at >= start_at,
                StrategyRunModel.started_at < end_at,
            )
        return (
            statement.order_by(StrategyRunModel.id.desc()).limit(limit).offset(offset)
        )

    async def list_runs(
        self,
        limit: int = 100,
        offset: int = 0,
        started_date: date | None = None,
    ) -> list[StrategyRun]:
        statement = self._list_statement(
            limit=limit,
            offset=offset,
            started_date=started_date,
        )
        models = list((await self._session.scalars(statement)).all())
        return [self._to_domain(model) for model in models]

    async def list_run_summaries(
        self,
        limit: int = 100,
        offset: int = 0,
        started_date: date | None = None,
    ) -> list[dict[str, object]]:
        """List run headers from denormalized projection columns only.

        Neither ``trace_json`` nor ``snapshot_json`` is selected, so list pages
        stay O(header size) even when traces are multi-megabyte.
        """

        statement = self._list_statement(
            limit=limit,
            offset=offset,
            started_date=started_date,
            columns=(
                StrategyRunModel.id,
                StrategyRunModel.trigger_source,
                StrategyRunModel.schedule_id,
                StrategyRunModel.status,
                StrategyRunModel.current_state,
                StrategyRunModel.summary,
                StrategyRunModel.summary_render_mode,
                StrategyRunModel.started_at,
                StrategyRunModel.completed_at,
                StrategyRunModel.tool_calls_count,
                StrategyRunModel.thinking_count,
                StrategyRunModel.total_tokens,
                StrategyRunModel.trade_count,
            ),
        )
        rows = list((await self._session.execute(statement)).all())
        return [
            {
                "run_id": row.id,
                "trigger_source": row.trigger_source,
                "schedule_id": row.schedule_id,
                "status": row.status,
                "current_state": row.current_state or INITIAL_STATE.value,
                "summary": row.summary,
                "summary_render_mode": row.summary_render_mode,
                "started_at": datetime.fromisoformat(row.started_at),
                "completed_at": _deserialize_datetime(row.completed_at),
                "tool_calls_count": int(row.tool_calls_count or 0),
                "thinking_count": int(row.thinking_count or 0),
                "total_tokens": int(row.total_tokens or 0),
                "trade_count": int(row.trade_count or 0),
            }
            for row in rows
        ]

    async def list_completed_reports(
        self,
        target_date: date,
        limit: int = 100,
        offset: int = 0,
    ) -> list[RunReportRecord]:
        local_start = datetime.combine(
            target_date, time.min, tzinfo=_MARKET_TIMEZONE
        ).astimezone(UTC)
        local_end = datetime.combine(
            target_date + timedelta(days=1), time.min, tzinfo=_MARKET_TIMEZONE
        ).astimezone(UTC)
        statement = (
            select(
                StrategyRunModel.id,
                StrategyRunModel.started_at,
                StrategyRunModel.completed_at,
                StrategyRunModel.trace_json,
            )
            .where(
                StrategyRunModel.status == RunStatus.COMPLETED.value,
                StrategyRunModel.completed_at.is_not(None),
                StrategyRunModel.completed_at >= local_start.isoformat(),
                StrategyRunModel.completed_at < local_end.isoformat(),
            )
            .order_by(StrategyRunModel.completed_at.asc(), StrategyRunModel.id.asc())
            .limit(limit)
            .offset(offset)
        )
        rows = list((await self._session.execute(statement)).all())
        reports: list[RunReportRecord] = []
        for row in rows:
            content = _extract_run_report(dict(row.trace_json or {}))
            if not content:
                continue
            reports.append(
                RunReportRecord(
                    run_id=int(row.id),
                    started_at=datetime.fromisoformat(row.started_at),
                    completed_at=_deserialize_datetime(row.completed_at),
                    content=content,
                )
            )
        return reports

    async def delete(self, run_id: int) -> None:
        await self._session.execute(
            delete(StrategyRunModel).where(StrategyRunModel.id == run_id)
        )

    def _to_domain(self, model: StrategyRunModel) -> StrategyRun:
        current_state_value = model.current_state or INITIAL_STATE.value
        return StrategyRun(
            run_id=model.id,
            trigger_source=TriggerSource(model.trigger_source),
            schedule_id=model.schedule_id,
            snapshot=_deserialize_snapshot(dict(model.snapshot_json)),
            status=RunStatus(model.status),
            summary=model.summary,
            summary_render_mode=model.summary_render_mode,
            failure_reason=model.failure_reason,
            started_at=datetime.fromisoformat(model.started_at),
            completed_at=_deserialize_datetime(model.completed_at),
            current_state=RunState(current_state_value),
            trace=_deserialize_trace(dict(model.trace_json or {})),
        )
