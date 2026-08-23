"""Tests for two-stage run domain entities and state rules."""

from __future__ import annotations

import pytest

from backend.business.runs import StrategyRun, StrategySnapshot
from backend.business.shared.enums import RunState, RunStatus, TriggerSource


def make_strategy_snapshot() -> StrategySnapshot:
    return StrategySnapshot(
        prompt_version="v2",
        risk_rules_version="risk-v1",
    )


def make_strategy_run() -> StrategyRun:
    return StrategyRun(
        run_id=1,
        trigger_source=TriggerSource.MANUAL,
        schedule_id=None,
        snapshot=make_strategy_snapshot(),
    )


def test_strategy_run_advances_valid_sequence_to_completion() -> None:
    run = make_strategy_run()

    run.advance_to(RunState.SUMMARY)
    run.advance_to(RunState.COMPLETED)

    assert run.status is RunStatus.COMPLETED
    assert run.current_state is RunState.COMPLETED
    assert run.completed_at is not None


def test_strategy_run_rejects_skipping_summary_stage() -> None:
    run = make_strategy_run()

    with pytest.raises(ValueError, match="invalid state transition"):
        run.advance_to(RunState.COMPLETED)


def test_strategy_run_starts_with_empty_trace() -> None:
    run = make_strategy_run()

    assert run.trace.current_stage_id is None
    assert run.trace.stages == []


def test_scheduled_run_requires_schedule_id() -> None:
    with pytest.raises(ValueError, match="schedule_id"):
        StrategyRun(
            run_id=2,
            trigger_source=TriggerSource.SCHEDULED,
            schedule_id=None,
            snapshot=make_strategy_snapshot(),
        )


def test_strategy_run_can_fail_immediately() -> None:
    run = make_strategy_run()

    run.fail("研究模型连接超时")

    assert run.status is RunStatus.FAILED
    assert run.current_state is RunState.FAILED
    assert run.failure_reason == "研究模型连接超时"
    assert run.completed_at is not None


def test_strategy_run_can_abort_immediately() -> None:
    run = make_strategy_run()

    run.abort()

    assert run.status is RunStatus.ABORTED
    assert run.current_state is RunState.FAILED
    assert run.completed_at is not None
