"""Recorder that maintains the single persisted run trace."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime
from time import perf_counter
from typing import Any

from backend.business.runs import StrategyRun, TraceStage, TraceStep
from backend.business.runs.dto import to_run_detail_dto
from backend.business.runs.pipeline_stages import (
    TRACE_STAGE_META,
    trace_key_for_stage_name,
)
from backend.business.runs.run_events import (
    StageCompleted,
    StageDegraded,
    StageEntered,
    StageFailed,
    StageSkipped,
    ToolCallFinished,
    ToolCallStarted,
)
from backend.business.runs.tool_presentation import (
    summarize_tool_arguments as _summarize_tool_arguments,
)
from backend.business.runs.tool_presentation import (
    summarize_tool_result as _summarize_tool_result,
)
from backend.business.runs.tool_presentation import (
    tool_step_title as _tool_step_title,
)
from backend.business.runs.trace_reducer import reduce_trace
from backend.business.shared import json_safe

PersistRun = Callable[[StrategyRun], Awaitable[StrategyRun]]
PublishSnapshot = Callable[[int, object], Awaitable[None]]
Commit = Callable[[], Awaitable[object]]
HasSnapshotSubscribers = Callable[[int], bool]
STREAM_PERSIST_INTERVAL_SECONDS = 1.0
COMMIT_THROTTLE_SECONDS = 1.0
# One tool-call cycle fires several publishes within milliseconds (start,
# finish, stage summary). Full-snapshot SSE frames within this window collapse
# into one trailing checkpoint; persistence and commits are NOT coalesced.
CHECKPOINT_COALESCE_SECONDS = 0.35


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


class RunTraceRecorder:
    """Mutate one run trace; publish full snapshots only at stage/tool boundaries."""

    def __init__(
        self,
        *,
        run: StrategyRun,
        persist_run: PersistRun,
        publish_snapshot: PublishSnapshot,
        commit: Commit | None = None,
        has_snapshot_subscribers: HasSnapshotSubscribers | None = None,
    ) -> None:
        self._run = run
        self._persist_run = persist_run
        self._publish_snapshot = publish_snapshot
        self._commit = commit
        self._has_snapshot_subscribers = has_snapshot_subscribers
        self._last_stream_persist_at = 0.0
        self._last_commit_at = 0.0
        self._last_snapshot_publish_at = 0.0
        self._snapshot_flush_task: asyncio.Task[None] | None = None

    async def publish(self, *, force_commit: bool = True) -> None:
        self._run.trace.updated_at = utc_now()
        await self._persist_run(self._run)
        # Commit every event when forced (stage transitions, terminal states)
        # or when the throttle window has elapsed for mid-stage updates.
        if self._commit is not None and (
            force_commit
            or perf_counter() - self._last_commit_at >= COMMIT_THROTTLE_SECONDS
        ):
            await self._commit()
            self._last_commit_at = perf_counter()
        await self._publish_snapshot_coalesced()
        self._last_stream_persist_at = perf_counter()

    async def _publish_snapshot_coalesced(self) -> None:
        is_terminal = str(self._run.status) != "RUNNING"
        if is_terminal:
            task = self._snapshot_flush_task
            if task is not None and not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            self._snapshot_flush_task = None
            self._last_snapshot_publish_at = perf_counter()
            await self._publish_snapshot(self._run.run_id, to_run_detail_dto(self._run))
            return
        # Full snapshots are expensive. Only emit them when a client is
        # subscribed; live text/thinking continues via ``trace_step_delta``.
        if self._has_snapshot_subscribers is not None and not (
            self._has_snapshot_subscribers(self._run.run_id)
        ):
            return
        now = perf_counter()
        if now - self._last_snapshot_publish_at >= CHECKPOINT_COALESCE_SECONDS:
            self._last_snapshot_publish_at = now
            await self._publish_snapshot(self._run.run_id, to_run_detail_dto(self._run))
            return
        # Burst frame: schedule one trailing flush so the newest state still
        # ships within the coalesce window (terminal frames included).
        if self._snapshot_flush_task is None or self._snapshot_flush_task.done():
            self._snapshot_flush_task = asyncio.create_task(
                self._flush_snapshot_later()
            )

    async def _flush_snapshot_later(self) -> None:
        try:
            await asyncio.sleep(CHECKPOINT_COALESCE_SECONDS)
            self._last_snapshot_publish_at = perf_counter()
            await self._publish_snapshot(self._run.run_id, to_run_detail_dto(self._run))
        except Exception:
            # Best-effort observability; a background flush must never raise.
            return

    async def aclose(self) -> None:
        task = self._snapshot_flush_task
        self._snapshot_flush_task = None
        if task is None:
            return
        if not task.done():
            task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def _persist_stream_if_due(self) -> None:
        """Throttle mid-stream DB writes without broadcasting a full snapshot."""

        if (
            perf_counter() - self._last_stream_persist_at
            < STREAM_PERSIST_INTERVAL_SECONDS
        ):
            return
        self._run.trace.updated_at = utc_now()
        await self._persist_run(self._run)
        if self._commit is not None and (
            perf_counter() - self._last_commit_at >= COMMIT_THROTTLE_SECONDS
        ):
            await self._commit()
            self._last_commit_at = perf_counter()
        self._last_stream_persist_at = perf_counter()

    async def publish_stream_if_due(self) -> None:
        await self._persist_stream_if_due()

    async def set_stage_summary(self, stage_name: str, summary: str) -> None:
        stage = self._ensure_stage(stage_name)
        stage.summary = summary
        # This update may follow a just-persisted event and precede a long LLM
        # call with no further DB writes.  Committing lazily here would hold the
        # SQLite write lock across that whole call (10-60s), starving every
        # other writer (StockApi call logs, run heartbeats) past their
        # busy_timeout.  Always commit so the write lock is released
        # immediately; summary updates are cheap and infrequent.
        await self.publish(force_commit=True)

    async def enter_stage(self, stage_name: str) -> None:
        self._run.trace = reduce_trace(
            self._run.trace,
            StageEntered(run_id=self._run.run_id, stage_name=stage_name),
        )
        await self.publish()

    async def complete_stage(self, stage_name: str, summary: str | None = None) -> None:
        self._run.trace = reduce_trace(
            self._run.trace,
            StageCompleted(
                run_id=self._run.run_id,
                stage_name=stage_name,
                summary=summary,
            ),
        )
        await self.publish()

    async def skip_stage(self, stage_name: str, summary: str) -> None:
        self._run.trace = reduce_trace(
            self._run.trace,
            StageSkipped(
                run_id=self._run.run_id,
                stage_name=stage_name,
                summary=summary,
            ),
        )
        await self.publish()

    async def degrade_stage(self, stage_name: str, summary: str) -> None:
        self._run.trace = reduce_trace(
            self._run.trace,
            StageDegraded(
                run_id=self._run.run_id,
                stage_name=stage_name,
                summary=summary,
            ),
        )
        await self.publish()

    async def fail_stage(self, stage_name: str, summary: str | None = None) -> None:
        self._run.trace = reduce_trace(
            self._run.trace,
            StageFailed(
                run_id=self._run.run_id,
                stage_name=stage_name,
                summary=summary,
            ),
        )
        await self.publish()

    async def set_input_step(
        self,
        stage_name: str,
        *,
        title: str,
        summary: str,
        data: dict[str, Any] | None,
        step_id: str = "input",
    ) -> None:
        await self._set_step(
            stage_name,
            step_id=step_id,
            type="input",
            title=title,
            status="completed",
            summary=summary,
            data=data,
            ended_now=True,
        )

    async def set_step(
        self,
        stage_name: str,
        *,
        step_id: str,
        type: str,
        title: str,
        status: str,
        summary: str | None = None,
        content: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        await self._set_step(
            stage_name,
            step_id=step_id,
            type=type,
            title=title,
            status=status,
            summary=summary,
            content=content,
            data=data,
            ended_now=status != "running",
        )

    async def set_prompt_step(
        self,
        stage_name: str,
        *,
        title: str,
        summary: str,
        prompt: str | None,
        data: dict[str, Any] | None,
        step_id: str = "prompt",
    ) -> None:
        payload = dict(data or {})
        # Keep the actual LLM input canonical inside the private persisted trace.
        user_message = payload.get("user_message")
        if (
            (not isinstance(user_message, str) or not user_message.strip())
            and isinstance(prompt, str)
            and prompt.strip()
        ):
            payload["user_message"] = prompt
        await self._set_step(
            stage_name,
            step_id=step_id,
            type="prompt",
            title=title,
            status="completed",
            summary=summary,
            content=prompt,
            data=payload,
            ended_now=True,
        )

    async def patch_prompt_llm_messages(
        self,
        stage_name: str,
        *,
        system_message: str | None = None,
        user_message: str | None = None,
        tool_definitions: list[object] | None = None,
    ) -> None:
        """Attach actual system/user messages sent to the model onto prompt steps."""
        stage = self._ensure_stage(stage_name)
        prompt_steps = [step for step in stage.steps if step.type == "prompt"]
        if not prompt_steps:
            return
        any_changed = False
        for step in prompt_steps:
            data = dict(step.data or {}) if isinstance(step.data, dict) else {}
            step_changed = False
            if (
                system_message is not None
                and system_message.strip()
                and data.get("system_message") != system_message
            ):
                data["system_message"] = system_message
                step_changed = True
            if (
                user_message is not None
                and user_message.strip()
                and data.get("user_message") != user_message
            ):
                data["user_message"] = user_message
                step_changed = True
            if tool_definitions and data.get("tool_definitions") != tool_definitions:
                data["tool_definitions"] = tool_definitions
                step_changed = True
            if step_changed:
                step.data = json_safe(data)
                any_changed = True
        if any_changed:
            await self.publish()

    async def append_thinking(
        self,
        stage_name: str,
        delta: str,
        *,
        publish_stream: bool = True,
    ) -> tuple[str, str] | None:
        if not delta:
            return None
        stage = self._ensure_stage(stage_name)
        step = self._ensure_step(
            stage, step_id="thinking", type="thinking", title="深度思考"
        )
        self._move_generated_steps_to_end(stage)
        now = utc_now()
        if step.started_at is None:
            step.started_at = now
        step.status = "running"
        step.content = f"{step.content or ''}{delta}"
        step.summary = "模型正在思考"
        self._run.trace.updated_at = now
        if publish_stream:
            await self._persist_stream_if_due()
        return stage.stage_id, step.step_id

    async def append_segmented_thinking(
        self,
        stage_name: str,
        delta: str,
        *,
        title: str = "深度思考",
        step_id_prefix: str = "thinking",
        publish_stream: bool = True,
    ) -> tuple[str, str] | None:
        if not delta:
            return None
        stage = self._ensure_stage(stage_name)
        step = self._find_running_step(
            stage,
            "thinking",
            step_id_prefix=step_id_prefix,
        )
        if step is None:
            step = self._append_segmented_step(
                stage,
                type="thinking",
                title=title,
                step_id_prefix=step_id_prefix,
            )
        self._move_generated_steps_to_end(stage)
        now = utc_now()
        if step.started_at is None:
            step.started_at = now
        step.status = "running"
        step.content = f"{step.content or ''}{delta}"
        step.summary = "模型正在思考"
        self._run.trace.updated_at = now
        if publish_stream:
            await self._persist_stream_if_due()
        return stage.stage_id, step.step_id

    async def close_running_segmented_thinking(self, stage_name: str) -> bool:
        stage = self._ensure_stage(stage_name)
        step = self._find_running_step(stage, "thinking")
        if step is None:
            return False
        now = utc_now()
        step.status = "completed"
        step.ended_at = now
        await self.publish()
        return True

    async def start_tool_call_step(
        self,
        stage_name: str,
        *,
        tool_call_id: str,
        tool_name: str,
        arguments: dict[str, Any] | None,
    ) -> None:
        await self.close_running_segmented_thinking(stage_name)
        await self.discard_result_draft(stage_name)
        safe_arguments = json_safe(arguments or {})
        self._run.trace = reduce_trace(
            self._run.trace,
            ToolCallStarted(
                run_id=self._run.run_id,
                stage_name=stage_name,
                tool_call_id=tool_call_id,
                tool_name=tool_name,
                arguments=(safe_arguments if isinstance(safe_arguments, dict) else {}),
                title=_tool_step_title(tool_name),
                summary=_summarize_tool_arguments(arguments) or "正在调用工具",
            ),
        )
        await self.publish()

    async def finish_tool_call_step(
        self,
        stage_name: str,
        *,
        tool_call_id: str,
        tool_name: str,
        status: str,
        arguments: dict[str, Any] | None,
        result: object | None,
        error: str | None,
        model_content_characters: int | None = None,
        stock_api_calls: (
            list[dict[str, Any]] | tuple[dict[str, Any], ...] | None
        ) = None,
    ) -> None:
        safe_arguments = json_safe(arguments or {})
        self._run.trace = reduce_trace(
            self._run.trace,
            ToolCallFinished(
                run_id=self._run.run_id,
                stage_name=stage_name,
                tool_call_id=tool_call_id,
                tool_name=tool_name,
                status=status,
                arguments=(safe_arguments if isinstance(safe_arguments, dict) else {}),
                error=error,
                model_content_characters=model_content_characters,
                stock_api_calls=tuple(stock_api_calls or ()),
                title=_tool_step_title(tool_name),
                summary=_summarize_tool_result(
                    result,
                    error,
                    _summarize_tool_arguments(arguments),
                ),
            ),
        )
        await self.publish()

    async def append_step_content(
        self,
        stage_name: str,
        *,
        step_id: str,
        type: str,
        title: str,
        delta: str,
        summary: str | None = None,
        update_summary_from_content: bool = False,
        publish_stream: bool = True,
    ) -> tuple[str, str] | None:
        if not delta:
            return None
        stage = self._ensure_stage(stage_name)
        step = self._ensure_step(stage, step_id=step_id, type=type, title=title)
        now = utc_now()
        if step.started_at is None:
            step.started_at = now
        step.status = "running"
        step.content = f"{step.content or ''}{delta}"
        if update_summary_from_content:
            content_line = (
                (step.content or "").split("\n")[0].strip().lstrip("#").strip()
            )
            step.summary = (
                content_line[:120] if content_line else (summary or "正在生成")
            )
        elif summary is not None:
            step.summary = summary
        self._run.trace.updated_at = now
        if publish_stream:
            await self._persist_stream_if_due()
        return stage.stage_id, step.step_id

    async def discard_result_draft(self, stage_name: str) -> bool:
        """Drop intermediate result text once tools resume (final answer comes later).

        Streaming models often emit assistant text before tool_calls in the same
        turn. That text is not the stage report and must not stay visible while
        tools run or get mixed into the real final answer stream.
        """

        stage = self._latest_stage_for_key(trace_key_for_stage_name(stage_name))
        if stage is None:
            return False
        step = self._find_step(stage, "result")
        if step is None:
            return False
        if step.status not in {"running", "pending"}:
            return False
        if not (step.content or "").strip() and step.status == "pending":
            return False
        step.content = None
        step.summary = None
        step.status = "pending"
        step.started_at = None
        step.ended_at = None
        step.data = None
        await self.publish()
        return True

    async def set_result_step(
        self,
        stage_name: str,
        *,
        title: str,
        summary: str | None,
        content: str | None,
        data: dict[str, Any] | None,
        step_id: str = "result",
        status: str = "completed",
    ) -> None:
        await self._close_open_steps(stage_name)
        await self._set_step(
            stage_name,
            step_id=step_id,
            type="result",
            title=title,
            status=status,
            summary=summary,
            content=content,
            data=dict(data or {}),
            ended_now=True,
        )

    async def set_status_step(
        self,
        stage_name: str,
        *,
        title: str,
        summary: str,
        data: dict[str, Any] | None,
        status: str = "completed",
        step_id: str = "status",
    ) -> None:
        await self._set_step(
            stage_name,
            step_id=step_id,
            type="status",
            title=title,
            status=status,
            summary=summary,
            data=data,
            ended_now=True,
        )

    async def _close_open_steps(self, stage_name: str) -> None:
        stage = self._ensure_stage(stage_name)
        now = utc_now()
        changed = False
        for step in stage.steps:
            if step.status == "running":
                step.status = "completed"
                step.ended_at = now
                changed = True
        if changed:
            await self.publish(force_commit=False)

    async def _set_step(
        self,
        stage_name: str,
        *,
        step_id: str,
        type: str,
        title: str,
        status: str,
        summary: str | None = None,
        content: str | None = None,
        data: dict[str, Any] | None = None,
        ended_now: bool = False,
    ) -> None:
        stage = self._ensure_stage(stage_name)
        step = self._ensure_step(stage, step_id=step_id, type=type, title=title)
        now = utc_now()
        if step.started_at is None:
            step.started_at = now
        step.type = type
        step.title = title
        step.status = status
        step.summary = summary
        step.content = content
        step.data = None if data is None else json_safe(dict(data))
        if ended_now:
            step.ended_at = now
        self._move_generated_steps_to_end(stage)
        await self.publish()

    def _ensure_stage(self, stage_name: str) -> TraceStage:
        """Resolve a pipeline stage, creating its trace entry when absent."""

        key = trace_key_for_stage_name(stage_name)
        existing = self._latest_stage_for_key(key)
        if existing is not None:
            return existing
        title, description = TRACE_STAGE_META[key]
        stage = TraceStage(
            stage_id=f"{key}:na",
            key=key,
            round=None,
            title=title,
            description=description,
        )
        self._run.trace.stages.append(stage)
        return stage

    def _latest_stage_for_key(self, key: str) -> TraceStage | None:
        for stage in reversed(self._run.trace.stages):
            if stage.key == key:
                return stage
        return None

    def _ensure_step(
        self, stage: TraceStage, *, step_id: str, type: str, title: str
    ) -> TraceStep:
        existing = self._find_step(stage, step_id)
        if existing is not None:
            return existing
        step = TraceStep(step_id=step_id, type=type, title=title)
        stage.steps.append(step)
        return step

    def _find_step(self, stage: TraceStage, step_id: str) -> TraceStep | None:
        return next((step for step in stage.steps if step.step_id == step_id), None)

    def _move_step_to_end(self, stage: TraceStage, step_id: str) -> None:
        step = self._find_step(stage, step_id)
        if step is None or stage.steps[-1] is step:
            return
        stage.steps.remove(step)
        stage.steps.append(step)

    def _move_generated_steps_to_end(self, stage: TraceStage) -> None:
        self._move_step_to_end(stage, "result")

    def _find_running_step(
        self,
        stage: TraceStage,
        step_type: str,
        *,
        step_id_prefix: str | None = None,
    ) -> TraceStep | None:
        for step in reversed(stage.steps):
            if step.type == step_type and step.status == "running":
                if step_id_prefix is not None and not step.step_id.startswith(
                    f"{step_id_prefix}:"
                ):
                    continue
                return step
        return None

    def _append_segmented_step(
        self,
        stage: TraceStage,
        *,
        type: str,
        title: str,
        step_id_prefix: str | None = None,
    ) -> TraceStep:
        prefix = step_id_prefix or type
        index = (
            sum(1 for step in stage.steps if step.step_id.startswith(f"{prefix}:")) + 1
        )
        step = TraceStep(
            step_id=f"{prefix}:{index}",
            type=type,
            title=title,
        )
        stage.steps.append(step)
        return step
