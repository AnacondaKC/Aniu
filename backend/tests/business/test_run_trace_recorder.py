"""Tests for runtime trace recording."""

from __future__ import annotations

import pytest

import backend.business.runs.traces.recorder as recorder_module
from backend.business.runs import StrategyRun, StrategySnapshot
from backend.business.runs.traces.recorder import RunTraceRecorder
from backend.business.shared.enums import TriggerSource


def make_run() -> StrategyRun:
    return StrategyRun(
        run_id=1,
        trigger_source=TriggerSource.MANUAL,
        schedule_id=None,
        snapshot=StrategySnapshot(
            prompt_version="v1",
            risk_rules_version="risk-v1",
        ),
    )


@pytest.mark.asyncio
async def test_report_result_and_summary_are_separate_steps() -> None:
    run = make_run()
    snapshots: list[object] = []

    async def persist_run(stored_run: StrategyRun) -> StrategyRun:
        return stored_run

    async def publish_snapshot(_run_id: int, snapshot: object) -> None:
        snapshots.append(snapshot)

    recorder = RunTraceRecorder(
        run=run,
        persist_run=persist_run,
        publish_snapshot=publish_snapshot,
    )

    await recorder.enter_stage("Run")
    await recorder.append_segmented_thinking("Run", "运行思考")
    await recorder.append_step_content(
        "Run",
        step_id="result",
        type="result",
        title="生成运行报告",
        delta="运行报告正文",
        summary="正在生成运行报告",
    )
    await recorder.close_running_segmented_thinking("Run")
    await recorder.set_result_step(
        "Run",
        title="生成运行报告",
        summary="已生成运行报告",
        content="运行报告正文",
        data=None,
    )
    await recorder.set_step(
        "Run",
        step_id="summary",
        type="result",
        title="生成摘要",
        status="running",
        summary="已生成运行报告，开始生成摘要",
    )

    stage = run.trace.stages[0]
    step_ids = [step.step_id for step in stage.steps]
    report_step = next(step for step in stage.steps if step.step_id == "result")
    summary_step = next(step for step in stage.steps if step.step_id == "summary")

    assert snapshots
    # set_step 会调用 _move_generated_steps_to_end，把 result 移到末尾。
    # summary 是被切除保护后的通用 stand-in，仅作为新增的后进步骤存在，
    # 但 result 在本次 set_step 后被重新搄到末尾，与 summary 仍保持独立、未被合并。
    assert step_ids == ["thinking:1", "summary", "result"]
    assert report_step.title == "生成运行报告"
    assert report_step.status == "completed"
    assert summary_step.title == "生成摘要"
    assert summary_step.status == "running"


@pytest.mark.asyncio
async def test_generated_result_moves_after_late_segmented_thinking() -> None:
    run = make_run()

    async def persist_run(stored_run: StrategyRun) -> StrategyRun:
        return stored_run

    async def publish_snapshot(_run_id: int, _snapshot: object) -> None:
        return None

    recorder = RunTraceRecorder(
        run=run,
        persist_run=persist_run,
        publish_snapshot=publish_snapshot,
    )

    await recorder.enter_stage("Run")
    await recorder.append_step_content(
        "Run",
        step_id="result",
        type="result",
        title="生成运行报告",
        delta="运行报告正文",
        summary="正在生成运行报告",
    )
    # 通用 stand-in 步骤：不参与 move-to-end 算法，仅作背景噪声。
    # 添加后应保留在原位置，验证只有 result 会被移动到末尾。
    await recorder.set_step(
        "Run",
        step_id="summary",
        type="result",
        title="生成摘要",
        status="running",
        summary="开始生成摘要",
    )
    await recorder.append_segmented_thinking("Run", "补充运行思考")

    stage = run.trace.stages[0]

    # result 是唯一被 move-to-end 算法保护的 generated step；
    # summary 不在保护列表中，遵循后进先得的自然顺序保留在末尾位置。
    assert [step.step_id for step in stage.steps] == [
        "summary",
        "thinking:1",
        "result",
    ]


@pytest.mark.asyncio
async def test_tool_request_discards_intermediate_result_draft() -> None:
    run = make_run()

    async def persist_run(stored_run: StrategyRun) -> StrategyRun:
        return stored_run

    async def publish_snapshot(_run_id: int, _snapshot: object) -> None:
        return None

    recorder = RunTraceRecorder(
        run=run,
        persist_run=persist_run,
        publish_snapshot=publish_snapshot,
    )

    await recorder.enter_stage("Run")
    await recorder.append_segmented_thinking("Run", "先看一下指数")
    await recorder.append_step_content(
        "Run",
        step_id="result",
        type="result",
        title="生成运行报告",
        delta="中间草稿，不是最终报告",
        summary="正在生成运行报告",
    )
    await recorder.start_tool_call_step(
        "Run",
        tool_call_id="c1",
        tool_name="query_market_data",
        arguments={"query": "A股行情"},
    )

    stage = run.trace.stages[0]
    result_step = next(step for step in stage.steps if step.step_id == "result")
    assert result_step.content is None
    assert result_step.status == "pending"
    assert any(step.type == "tool" for step in stage.steps)


@pytest.mark.asyncio
async def test_publish_skips_snapshot_when_no_subscribers() -> None:
    """When the SSE hub reports no subscribers, publish() must still persist the
    run but must NOT build the DTO / invoke the publish_snapshot sink."""

    run = make_run()
    persisted: list[StrategyRun] = []
    snapshots: list[object] = []

    async def persist_run(stored_run: StrategyRun) -> StrategyRun:
        persisted.append(stored_run)
        return stored_run

    async def publish_snapshot(_run_id: int, _snapshot: object) -> None:
        snapshots.append(_snapshot)

    recorder = RunTraceRecorder(
        run=run,
        persist_run=persist_run,
        publish_snapshot=publish_snapshot,
        has_snapshot_subscribers=lambda _run_id: False,
    )

    await recorder.publish()

    assert len(persisted) == 1
    assert snapshots == []


@pytest.mark.asyncio
async def test_terminal_publish_releases_stream_without_subscribers() -> None:
    run = make_run()
    snapshots: list[object] = []

    async def persist_run(stored_run: StrategyRun) -> StrategyRun:
        return stored_run

    async def publish_snapshot(_run_id: int, snapshot: object) -> None:
        snapshots.append(snapshot)

    recorder = RunTraceRecorder(
        run=run,
        persist_run=persist_run,
        publish_snapshot=publish_snapshot,
        has_snapshot_subscribers=lambda _run_id: False,
    )
    run.fail("failed")

    await recorder.publish()

    assert len(snapshots) == 1


@pytest.mark.asyncio
async def test_publish_emits_snapshot_when_subscribers_present() -> None:
    run = make_run()
    snapshots: list[object] = []

    async def persist_run(stored_run: StrategyRun) -> StrategyRun:
        return stored_run

    async def publish_snapshot(_run_id: int, snapshot: object) -> None:
        snapshots.append(snapshot)

    recorder = RunTraceRecorder(
        run=run,
        persist_run=persist_run,
        publish_snapshot=publish_snapshot,
        has_snapshot_subscribers=lambda _run_id: True,
    )

    await recorder.publish()

    assert len(snapshots) == 1


@pytest.mark.asyncio
async def test_publish_throttles_commits_for_mid_stage_updates() -> None:
    """Stage transitions force-commit; mid-stage publish(force_commit=False)
    commits at most once per COMMIT_THROTTLE_SECONDS window."""

    from backend.business.runs.traces.recorder import COMMIT_THROTTLE_SECONDS

    run = make_run()
    commits: list[int] = []

    async def persist_run(stored_run: StrategyRun) -> StrategyRun:
        return stored_run

    async def commit() -> None:
        commits.append(len(commits) + 1)

    recorder = RunTraceRecorder(
        run=run,
        persist_run=persist_run,
        publish_snapshot=lambda *_a, **_k: None,
        commit=commit,
        has_snapshot_subscribers=lambda _run_id: False,
    )
    assert COMMIT_THROTTLE_SECONDS >= 1.0

    # Stage transition forces a commit.
    await recorder.enter_stage("Run")
    forced_commits = len(commits)

    # Many mid-stage stream ticks within the throttle window: at most one
    # additional commit (the first one, since _last_commit_at starts at 0).
    for _ in range(20):
        await recorder.publish(force_commit=False)
    mid_stage_commits = len(commits) - forced_commits
    assert mid_stage_commits <= 1

    # A terminal event always commits regardless of throttle.
    before_terminal = len(commits)
    await recorder.complete_stage("Run", summary="完成")
    assert len(commits) > before_terminal


@pytest.mark.asyncio
async def test_set_stage_summary_always_commits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """set_stage_summary must commit even inside the commit throttle window.

    It may follow a just-persisted event and precede a long LLM call with no
    further DB writes; a throttled commit would hold the SQLite write lock
    across that whole call and starve every other writer.
    """

    monkeypatch.setattr(recorder_module, "perf_counter", lambda: 0.0)

    run = make_run()
    events: list[str] = []

    async def persist_run(stored_run: StrategyRun) -> StrategyRun:
        events.append("persist")
        return stored_run

    async def commit() -> None:
        events.append("commit")

    recorder = RunTraceRecorder(
        run=run,
        persist_run=persist_run,
        publish_snapshot=lambda *_a, **_k: None,
        commit=commit,
        has_snapshot_subscribers=lambda _run_id: False,
    )
    await recorder.enter_stage("Run")
    events.clear()

    await recorder.set_stage_summary("Run", "调用工具 1 次")

    assert events == ["persist", "commit"]


@pytest.mark.asyncio
async def test_prompt_trace_records_tool_definitions_for_debugging() -> None:
    run = make_run()

    async def persist_run(stored_run: StrategyRun) -> StrategyRun:
        return stored_run

    async def publish_snapshot(_run_id: int, _snapshot: object) -> None:
        return None

    recorder = RunTraceRecorder(
        run=run,
        persist_run=persist_run,
        publish_snapshot=publish_snapshot,
    )
    await recorder.enter_stage("Run")
    await recorder.set_prompt_step(
        "Run",
        step_id="run_input",
        title="发给大模型的输入",
        summary="已发送",
        prompt="请决策",
        data={"user_message": "请决策"},
    )
    definitions: list[object] = [
        {
            "name": "query_market_data",
            "description": "查询市场数据",
            "parameters": {"type": "object"},
        }
    ]

    await recorder.patch_prompt_llm_messages(
        "Run",
        system_message="Tool protocol",
        user_message="请决策",
        tool_definitions=definitions,
    )

    prompt_step = run.trace.stages[0].steps[0]
    assert prompt_step.data is not None
    assert prompt_step.data["system_message"] == "Tool protocol"
    assert prompt_step.data["user_message"] == "请决策"
    assert prompt_step.data["tool_definitions"] == definitions


@pytest.mark.asyncio
async def test_prompt_patch_keeps_latest_messages_in_canonical_trace() -> None:
    run = make_run()

    async def persist_run(stored_run: StrategyRun) -> StrategyRun:
        return stored_run

    async def publish_snapshot(_run_id: int, _snapshot: object) -> None:
        return None

    recorder = RunTraceRecorder(
        run=run,
        persist_run=persist_run,
        publish_snapshot=publish_snapshot,
    )
    first_message = "第一版完整模型输入" * 1_000

    await recorder.enter_stage("Run")
    await recorder.set_prompt_step(
        "Run",
        step_id="run_input",
        title="发给大模型的输入",
        summary="已发送",
        prompt="研究提示词",
        data={"user_message": first_message},
    )
    second_message = "第二版完整模型输入" * 1_000
    await recorder.patch_prompt_llm_messages(
        "Run",
        system_message="Tool protocol",
        user_message=second_message,
    )

    prompt_step = run.trace.stages[0].steps[0]
    assert prompt_step.data == {
        "user_message": second_message,
        "system_message": "Tool protocol",
    }


@pytest.mark.asyncio
async def test_completed_reasoning_remains_in_canonical_trace() -> None:
    run = make_run()

    async def persist_run(stored_run: StrategyRun) -> StrategyRun:
        return stored_run

    async def publish_snapshot(_run_id: int, _snapshot: object) -> None:
        return None

    recorder = RunTraceRecorder(
        run=run,
        persist_run=persist_run,
        publish_snapshot=publish_snapshot,
    )
    reasoning = "完整深度思考" * 1_000

    await recorder.enter_stage("Run")
    await recorder.append_segmented_thinking("Run", reasoning)
    await recorder.close_running_segmented_thinking("Run")

    step = run.trace.stages[0].steps[0]
    assert step.content == reasoning
    assert step.data is None
