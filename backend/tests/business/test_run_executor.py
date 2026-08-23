"""Tests for worker execution fencing in the run executor."""

from __future__ import annotations

import pytest

from backend.business.runs import StrategyRun, StrategySnapshot
from backend.business.runs.abort_registry import ActiveRunAbortRegistry
from backend.business.runs.executor import RunExecutor
from backend.business.shared import RunAbortError
from backend.business.shared.enums import RunStatus, TriggerSource


class InMemoryRunRepository:
    def __init__(self, run: StrategyRun) -> None:
        self.run = run

    async def get_by_id(self, run_id: int) -> StrategyRun | None:
        return self.run if run_id == self.run.run_id else None

    async def save(self, run: StrategyRun) -> StrategyRun:
        self.run = run
        return run


class FailingAgentRunnerFactory:
    def __init__(self) -> None:
        self.prepared = False

    async def prepare(self, _snapshot: StrategySnapshot) -> object:
        self.prepared = True
        raise AssertionError("execution guard must run before agent preparation")


@pytest.mark.asyncio
async def test_execution_guard_runs_after_abort_signal_activation() -> None:
    run = StrategyRun(
        run_id=20260823101,
        trigger_source=TriggerSource.MANUAL,
        schedule_id=None,
        snapshot=StrategySnapshot(
            prompt_version="v1",
            risk_rules_version="risk-v1",
        ),
    )
    repository = InMemoryRunRepository(run)
    registry = ActiveRunAbortRegistry()
    agent_factory = FailingAgentRunnerFactory()
    executor = RunExecutor(
        repository,  # type: ignore[arg-type]
        agent_runner_factory=agent_factory,  # type: ignore[arg-type]
        abort_registry=registry,
    )

    async def reject_expired_claim() -> None:
        assert registry.active_signal is not None
        raise RunAbortError(run.run_id)

    executor.set_execution_guard(reject_expired_claim)

    with pytest.raises(RunAbortError):
        await executor.execute(run.run_id)

    assert agent_factory.prepared is False
    assert repository.run.status is RunStatus.ABORTED
    assert registry.active_signal is None
