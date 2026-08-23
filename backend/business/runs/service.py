"""Request-side use cases for strategy runs."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from backend.business.runs import (
    ACTIVE_JOB_STATUSES,
    RUN_TASK_TYPE,
    StageModelSnapshot,
    StrategyRun,
    StrategySnapshot,
)
from backend.business.runs.abort_registry import ActiveRunAbortRegistry
from backend.business.runs.commands import StartRunCommand
from backend.business.runs.dto import (
    RunDetailDTO,
    RunSummaryDTO,
    run_summary_dto_from_row,
    to_run_detail_dto,
)
from backend.business.runs.ports import RunJobRepositoryPort, RunRepositoryPort
from backend.business.runs.queries import GetRunDetailQuery, ListRunsQuery
from backend.business.runs.trace_support import RunTraceSupport
from backend.business.settings import (
    STRATEGY_STAGE_IDS,
    AppSettings,
    ModelProfileRepositoryPort,
    SelectedModelRepositoryPort,
)
from backend.business.settings.ports import SettingsRepositoryPort
from backend.business.settings.resolver import ModelSelectionResolver
from backend.business.shared import (
    CommitterPort,
    ConcurrentRunError,
    RunDeletionNotAllowedError,
    RunNotFoundError,
    ServiceConfigurationError,
)
from backend.business.shared.enums import RunState, RunStatus

NowProvider = Callable[[], datetime]
ExecutionGuard = Callable[[], Awaitable[None]]
RunSnapshotPublisher = Callable[[int, object], Awaitable[None]]
logger = logging.getLogger(__name__)


class RunService:
    """Create, query, abort, and delete strategy runs."""

    def __init__(
        self,
        run_repo: RunRepositoryPort,
        settings_repo: SettingsRepositoryPort,
        model_profile_repo: ModelProfileRepositoryPort,
        selected_model_repo: SelectedModelRepositoryPort,
        *,
        run_job_repo: RunJobRepositoryPort,
        abort_registry: ActiveRunAbortRegistry,
        committer: CommitterPort | None = None,
        snapshot_publisher: RunSnapshotPublisher | None = None,
        now_provider: NowProvider | None = None,
    ) -> None:
        self._run_repo = run_repo
        self._settings_repo = settings_repo
        self._run_job_repo = run_job_repo
        self._abort_registry = abort_registry
        self._committer = committer
        self._now_provider = now_provider or (lambda: datetime.now(tz=UTC))
        self._model_resolver = ModelSelectionResolver(
            model_profile_repo=model_profile_repo,
            selected_model_repo=selected_model_repo,
        )
        self._trace = RunTraceSupport(
            run_repo=run_repo,
            committer=committer,
            snapshot_publisher=snapshot_publisher,
        )

    async def create_run(
        self,
        command: StartRunCommand,
        *,
        execution_guard: ExecutionGuard | None = None,
    ) -> RunDetailDTO:
        active_run = await self._run_repo.get_running_run()
        if active_run is not None:
            raise ConcurrentRunError(active_run.run_id)
        active_job = await self._run_job_repo.get_active_job()
        if active_job is not None:
            raise ConcurrentRunError(active_job.run_id)

        settings, stage_models = await self._load_settings_and_models()
        run = StrategyRun(
            run_id=await self._run_repo.next_run_id(
                self._now_provider().date(), RUN_TASK_TYPE
            ),
            trigger_source=command.trigger_source,
            schedule_id=command.schedule_id,
            snapshot=self._build_snapshot(command, settings, stage_models),
        )
        try:
            run = await self._run_repo.add(run)
            await self._run_job_repo.create_pending(run.run_id)
            if execution_guard is not None:
                await execution_guard()
            await self._commit()
        except Exception as exc:
            rollback = getattr(self._committer, "rollback", None)
            if callable(rollback):
                await rollback()
            active_job = await self._run_job_repo.get_active_job()
            if active_job is not None:
                raise ConcurrentRunError(active_job.run_id) from exc
            active_run = await self._run_repo.get_running_run()
            if active_run is not None:
                raise ConcurrentRunError(active_run.run_id) from exc
            raise
        logger.info(
            "run_submitted",
            extra={
                "run_id": run.run_id,
                "job_id": run.run_id,
                "trigger_source": run.trigger_source.value,
                "current_state": run.current_state.value,
                "status": "pending",
            },
        )
        await self._trace.publish(run)
        stored = await self._run_repo.get_by_id(run.run_id)
        if stored is None:
            raise RunNotFoundError(run.run_id)
        return to_run_detail_dto(stored)

    async def list_runs(self, query: ListRunsQuery) -> list[RunSummaryDTO]:
        rows = await self._run_repo.list_run_summaries(
            limit=query.limit,
            offset=query.offset,
            started_date=query.started_date,
        )
        return [run_summary_dto_from_row(row) for row in rows]

    async def get_run_detail(self, query: GetRunDetailQuery) -> RunDetailDTO:
        run = await self._run_repo.get_by_id(query.run_id)
        if run is None:
            raise RunNotFoundError(query.run_id)
        return to_run_detail_dto(run)

    async def delete_run(self, run_id: int) -> None:
        run = await self._run_repo.get_by_id(run_id)
        if run is None:
            raise RunNotFoundError(run_id)
        if run.status is RunStatus.RUNNING:
            raise RunDeletionNotAllowedError(run_id, "run_is_active")
        await self._run_repo.delete(run_id)
        await self._commit()

    async def abort_run(self, run_id: int, reason: str = "user_requested") -> None:
        job = await self._run_job_repo.request_cancel(run_id, reason=reason)
        if job is None or job.status not in ACTIVE_JOB_STATUSES:
            run = await self._run_repo.get_running_run()
            if run is None or run.run_id != run_id:
                raise RunNotFoundError(run_id)
            if self._abort_registry.abort(run_id, reason):
                return
            await self._record_aborted_run(run, reason)
            return
        self._abort_registry.abort(run_id, reason)
        if getattr(job.status, "value", job.status) == "CANCELLED":
            run = await self._run_repo.get_by_id(run_id)
            if run is not None and run.status is RunStatus.RUNNING:
                await self._record_aborted_run(run, reason)
            else:
                await self._commit()
            return
        await self._commit()

    def _build_snapshot(
        self,
        command: StartRunCommand,
        settings: AppSettings,
        stage_models: dict[str, StageModelSnapshot],
    ) -> StrategySnapshot:
        if command.model_name is not None and (
            command.model_name != stage_models["Run"].model_name
        ):
            raise ServiceConfigurationError(
                "model_name 已废弃；请通过 Run 阶段模型设置选择模型"
            )
        return StrategySnapshot(
            prompt_version=command.prompt_version,
            risk_rules_version=command.risk_rules_version,
            prompt_profile=settings.prompt_profile,
            stage_settings=settings.stage_settings,
            stage_models=stage_models,
        )

    async def _load_settings_and_models(
        self,
    ) -> tuple[AppSettings, dict[str, StageModelSnapshot]]:
        settings = await self._settings_repo.get() or AppSettings()
        strategy_stages = {
            stage_id: settings.stage_settings[stage_id]
            for stage_id in STRATEGY_STAGE_IDS
        }
        missing = [
            stage_id
            for stage_id, stage in strategy_stages.items()
            if stage.model_selected_model_id is None
        ]
        if missing:
            raise ServiceConfigurationError(
                "请先为每个交易阶段选择模型：" + "、".join(missing)
            )
        selected_ids = {
            stage.model_selected_model_id
            for stage in strategy_stages.values()
            if stage.model_selected_model_id is not None
        }
        resolved_models = await self._model_resolver.resolve_many(selected_ids)
        models: dict[str, StageModelSnapshot] = {}
        for stage_id, stage in strategy_stages.items():
            selected_model_id = stage.model_selected_model_id
            resolved = (
                None
                if selected_model_id is None
                else resolved_models.get(selected_model_id)
            )
            if resolved is None:
                raise ServiceConfigurationError(f"{stage_id} 阶段选择的模型不存在")
            profile = resolved.profile
            if not profile.enabled or not profile.base_url or not profile.api_key:
                raise ServiceConfigurationError(f"{stage_id} 阶段模型渠道不可用")
            models[stage_id] = StageModelSnapshot(
                stage_id=stage_id,
                selected_model_id=resolved.selected.selected_model_id,
                channel_profile_id=profile.profile_id,
                channel_revision=profile.revision,
                credential_owner_created_at=profile.created_at.isoformat(),
                protocol=profile.protocol.value,
                base_url=profile.base_url,
                model_name=resolved.selected.model_name,
                context_window_tokens=resolved.selected.context_window_tokens,
                max_output_tokens=resolved.selected.max_output_tokens,
                provider_config_json=profile.provider_config.as_json(),
            )
        return settings, models

    async def _record_aborted_run(self, run: StrategyRun, reason: str) -> None:
        previous_state = run.current_state.value
        run.abort()
        await self._run_repo.save(run)
        recorder = self._trace.make_recorder(run)
        message = f"运行已中止：strategy run aborted: run_id={run.run_id}"
        if previous_state not in {RunState.SUMMARY.value, RunState.FAILED.value}:
            await recorder.fail_stage(previous_state, message)
        await recorder.enter_stage("Summary")
        await recorder.set_status_step(
            "Summary",
            title="最终状态",
            summary=message,
            data={
                "error_message": f"strategy run aborted: run_id={run.run_id}",
                "reason": reason,
                "orphaned": True,
                "total_duration_ms": max(
                    0,
                    int((self._now_provider() - run.started_at).total_seconds() * 1000),
                ),
            },
            status="failed",
        )
        await recorder.fail_stage("Summary", message)
        await self._commit()

    async def _commit(self) -> None:
        if self._committer is not None:
            await self._committer.commit()
