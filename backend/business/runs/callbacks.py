"""Map two-stage runtime callbacks into the persisted trace and SSE stream."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from backend.business.runs import RunEventType, StrategyRun
from backend.business.runs.pipeline_stages import (
    is_tool_capable_stage,
    result_title_for_stage,
    streaming_summary_for_stage,
)
from backend.business.runs.runtime import RunRuntimeState
from backend.business.runs.traces import RunTraceRecorder
from backend.business.shared import RunNotFoundError
from backend.business.shared.enums import RunState

TraceStepDeltaPublisher = Callable[..., Awaitable[None]]
_RESULT_DATA_DROP_KEYS = frozenset({"tool_activity", "run_content", "content"})


def _coerce_dict_arg(payload: dict[str, Any], key: str) -> dict[str, Any] | None:
    value = payload.get(key)
    return dict(value) if isinstance(value, dict) else None


def _coerce_stock_api_calls(payload: dict[str, Any]) -> list[dict[str, Any]]:
    details = payload.get("details")
    if not isinstance(details, dict):
        return []
    calls = details.get("stock_api_calls")
    if not isinstance(calls, (list, tuple)):
        return []
    return [dict(call) for call in calls if isinstance(call, dict)]


def _coerce_model_content_characters(payload: dict[str, Any]) -> int | None:
    value = payload.get("model_content_characters")
    return value if type(value) is int and value >= 0 else None


def _slim_result_data(
    payload: dict[str, Any],
    *,
    drop_summary: bool = False,
) -> dict[str, Any]:
    data = {
        key: value
        for key, value in payload.items()
        if key not in _RESULT_DATA_DROP_KEYS
    }
    if drop_summary:
        data.pop("summary", None)
    return data


class RunExecutionCallbacks:
    def __init__(
        self,
        *,
        runtime: RunRuntimeState,
        publish_trace_step_delta: TraceStepDeltaPublisher | None = None,
    ) -> None:
        self._runtime = runtime
        self._publish_trace_step_delta = publish_trace_step_delta
        self._compaction_counts: dict[str, int] = {}

    def bind_runtime(self, runtime: RunRuntimeState) -> None:
        self._runtime = runtime

    def _require_active_run(self, run_id: int) -> StrategyRun:
        active = self._runtime.active_run
        if active is None or active.run_id != run_id:
            raise RunNotFoundError(run_id)
        return active

    def _require_trace_recorder(self, run_id: int) -> RunTraceRecorder:
        self._require_active_run(run_id)
        recorder = self._runtime.trace_recorder
        if recorder is None:
            raise RunNotFoundError(run_id)
        return recorder

    async def on_state_entered(self, run_id: int, state_name: str) -> None:
        recorder = self._require_trace_recorder(run_id)
        if state_name in {RunState.COMPLETED.value, RunState.FAILED.value}:
            return
        await recorder.enter_stage(state_name)
        self._runtime.stage_tool_calls = 0
        self._compaction_counts[state_name] = 0
        await recorder.set_stage_summary(
            state_name,
            "正在生成 HTML 总结"
            if state_name == RunState.SUMMARY.value
            else "正在执行任务",
        )

    async def on_state_completed(
        self,
        run_id: int,
        state_name: str,
        state_output: dict[str, Any],
        duration_ms: int,
    ) -> None:
        recorder = self._require_trace_recorder(run_id)
        payload = dict(state_output)
        payload["duration_ms"] = duration_ms
        if state_name == RunState.RUN.value:
            tool_count = int(payload.get("tool_calls_count") or 0)
            failure_count = int(payload.get("tool_failure_count") or 0)
            trade_count = int(payload.get("trade_count") or 0)
            summary = (
                f"调用工具 {tool_count} 次，失败 {failure_count} 次，"
                f"成功交易 {trade_count} 次"
            )
            await recorder.close_running_segmented_thinking(state_name)
            await recorder.set_result_step(
                state_name,
                title="生成 Markdown 运行报告",
                summary=summary,
                content=(
                    None
                    if payload.get("run_content") is None
                    else str(payload.get("run_content"))
                ),
                data=_slim_result_data(payload),
            )
            await recorder.complete_stage(state_name, summary)
            return
        if state_name == RunState.SUMMARY.value:
            summary = str(payload.get("summary") or "HTML 总结已生成")
            await recorder.set_result_step(
                state_name,
                title="生成 HTML 总结",
                summary="HTML 总结已生成",
                content=summary,
                data=_slim_result_data(payload, drop_summary=True),
            )
            await recorder.complete_stage(state_name, "HTML 总结已生成")

    async def on_state_skipped(
        self,
        run_id: int,
        state_name: str,
        summary: str,
    ) -> None:
        await self._require_trace_recorder(run_id).skip_stage(state_name, summary)

    async def on_state_degraded(
        self,
        run_id: int,
        state_name: str,
        summary: str,
    ) -> None:
        recorder = self._require_trace_recorder(run_id)
        await recorder.set_status_step(
            state_name,
            step_id="markdown_fallback",
            title="回退 Markdown",
            summary=summary,
            data={"reason": "summary_generation_failed"},
        )
        await recorder.degrade_stage(state_name, summary)

    async def on_tool_loop_event(
        self,
        run_id: int,
        stage_name: str,
        event_type: RunEventType,
        payload: dict[str, Any],
    ) -> None:
        recorder = self._require_trace_recorder(run_id)
        if event_type is RunEventType.CONTEXT_COMPACTED:
            index = self._compaction_counts.get(stage_name, 0) + 1
            self._compaction_counts[stage_name] = index
            await recorder.set_status_step(
                stage_name,
                step_id=f"context_compaction:{index}",
                title="压缩模型上下文",
                summary="上下文过长，已压缩后继续执行。",
                data=dict(payload),
            )
            return
        if is_tool_capable_stage(stage_name):
            if event_type is RunEventType.TOOL_LOOP_STARTED:
                system_message = payload.get("system_message")
                user_message = payload.get("user_message")
                definitions = payload.get("tool_definitions")
                await recorder.patch_prompt_llm_messages(
                    stage_name,
                    system_message=(
                        system_message if isinstance(system_message, str) else None
                    ),
                    user_message=(
                        user_message if isinstance(user_message, str) else None
                    ),
                    tool_definitions=(
                        definitions if isinstance(definitions, list) else None
                    ),
                )
                return
            if event_type in {
                RunEventType.TOOL_LOOP_STOPPED,
                RunEventType.TOOL_LOOP_FAILED,
            }:
                return
            if event_type is RunEventType.TOOL_CALL_REQUESTED:
                await recorder.start_tool_call_step(
                    stage_name,
                    tool_call_id=str(payload.get("tool_call_id") or ""),
                    tool_name=str(payload.get("tool_name") or "未命名工具"),
                    arguments=_coerce_dict_arg(payload, "arguments"),
                )
                return
            if event_type in {
                RunEventType.TOOL_CALL_COMPLETED,
                RunEventType.TOOL_CALL_BLOCKED,
                RunEventType.TOOL_CALL_FAILED,
            }:
                status = {
                    RunEventType.TOOL_CALL_COMPLETED: "completed",
                    RunEventType.TOOL_CALL_BLOCKED: "blocked",
                    RunEventType.TOOL_CALL_FAILED: "failed",
                }[event_type]
                await recorder.finish_tool_call_step(
                    stage_name,
                    tool_call_id=str(payload.get("tool_call_id") or ""),
                    tool_name=str(payload.get("tool_name") or "未命名工具"),
                    status=status,
                    arguments=_coerce_dict_arg(payload, "arguments"),
                    result=payload.get("result") or payload.get("content"),
                    error=(
                        None
                        if payload.get("error") is None
                        else str(payload.get("error"))
                    ),
                    model_content_characters=(
                        _coerce_model_content_characters(payload)
                    ),
                    stock_api_calls=_coerce_stock_api_calls(payload),
                )
                if event_type is RunEventType.TOOL_CALL_COMPLETED:
                    self._runtime.stage_tool_calls += 1
                    await recorder.set_stage_summary(
                        stage_name,
                        f"调用工具 {self._runtime.stage_tool_calls} 次",
                    )
                return
        if event_type is RunEventType.SUMMARY_GENERATION_STARTED:
            await recorder.set_step(
                stage_name,
                step_id="result",
                type="result",
                title="生成 HTML 总结",
                status="running",
                summary="正在生成 HTML 总结",
                data=dict(payload),
            )

    async def on_llm_stream_delta(
        self,
        run_id: int,
        stage_name: str,
        delta: str,
        channel: str = "text",
    ) -> None:
        self._require_active_run(run_id)
        if not delta:
            return
        recorder = self._require_trace_recorder(run_id)
        if channel == "thinking" and is_tool_capable_stage(stage_name):
            segment_ref = await recorder.append_segmented_thinking(
                stage_name,
                delta,
                title="深度思考",
                step_id_prefix="thinking",
                publish_stream=False,
            )
        elif channel == "thinking":
            segment_ref = await recorder.append_thinking(
                stage_name, delta, publish_stream=False
            )
        else:
            if is_tool_capable_stage(stage_name):
                await recorder.close_running_segmented_thinking(stage_name)
            segment_ref = await recorder.append_step_content(
                stage_name,
                step_id="result",
                type="result",
                title=result_title_for_stage(stage_name),
                delta=delta,
                summary=streaming_summary_for_stage(stage_name),
                update_summary_from_content=True,
                publish_stream=False,
            )
        if segment_ref is None:
            return
        stage_id, step_id = segment_ref
        await self._emit_step_delta(
            run_id, stage_id, step_id, delta, channel=channel
        )
        await recorder.publish_stream_if_due()

    async def on_stage_prompt_prepared(
        self,
        run_id: int,
        stage_name: str,
        payload: dict[str, Any],
    ) -> None:
        data = {key: value for key, value in payload.items() if key != "payload"}
        await self._require_trace_recorder(run_id).set_prompt_step(
            stage_name,
            step_id=str(payload.get("phase") or "prompt"),
            title=str(payload.get("title") or "运行提示词"),
            summary=str(payload.get("summary") or "提示词已发送给大模型"),
            prompt=(
                None
                if payload.get("prompt") is None
                else str(payload.get("prompt"))
            ),
            data=data,
        )

    async def _emit_step_delta(
        self,
        run_id: int,
        stage_id: str,
        step_id: str,
        delta: str,
        *,
        channel: str,
    ) -> None:
        if self._publish_trace_step_delta is None:
            return
        await self._publish_trace_step_delta(
            run_id,
            stage_id=stage_id,
            step_id=step_id,
            channel=channel,
            delta=delta,
        )
