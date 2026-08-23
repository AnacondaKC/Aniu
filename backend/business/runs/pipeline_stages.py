"""Canonical metadata for the two-stage strategy run."""

from __future__ import annotations

from dataclasses import dataclass

from backend.business.shared.enums import RunState


@dataclass(frozen=True, slots=True)
class PipelineStage:
    run_state: RunState
    stage_id: str
    trace_key: str
    title: str
    description: str

    @property
    def state_name(self) -> str:
        return self.run_state.value


RUN = PipelineStage(
    run_state=RunState.RUN,
    stage_id="Run",
    trace_key="run",
    title="任务执行",
    description="完成研究、判断、交易与 Markdown 运行报告",
)
SUMMARY = PipelineStage(
    run_state=RunState.SUMMARY,
    stage_id="Summary",
    trace_key="summary",
    title="展示总结",
    description="根据运行报告与执行证据生成 HTML 总结",
)

PIPELINE: tuple[PipelineStage, ...] = (RUN, SUMMARY)
_BY_TRACE_KEY = {stage.trace_key: stage for stage in PIPELINE}
TRACE_STAGE_META = {
    stage.trace_key: (stage.title, stage.description) for stage in PIPELINE
}


def pipeline_stage_for_state_name(stage_name: str) -> PipelineStage | None:
    if not stage_name:
        return None
    base = stage_name.split(":", 1)[0]
    for stage in PIPELINE:
        if base in {stage.state_name, stage.stage_id}:
            return stage
    return None


def pipeline_stage_for_trace_key(trace_key: str) -> PipelineStage | None:
    return _BY_TRACE_KEY.get(trace_key)


def trace_key_for_stage_name(stage_name: str) -> str:
    stage = pipeline_stage_for_state_name(stage_name)
    if stage is None:
        raise ValueError(f"unknown pipeline stage: {stage_name}")
    return stage.trace_key


def is_tool_capable_stage(stage_name: str) -> bool:
    return pipeline_stage_for_state_name(stage_name) is RUN


def is_write_stage(stage_name: str) -> bool:
    return pipeline_stage_for_state_name(stage_name) is RUN


def result_title_for_stage(stage_name: str) -> str:
    if pipeline_stage_for_state_name(stage_name) is RUN:
        return "生成运行报告"
    return "生成 HTML 总结"


def streaming_summary_for_stage(stage_name: str) -> str:
    if pipeline_stage_for_state_name(stage_name) is RUN:
        return "正在生成运行报告"
    return "正在生成 HTML 总结"
