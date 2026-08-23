"""Run feature persistence ports."""

from __future__ import annotations

from datetime import date
from typing import Protocol

from backend.business.runs.job import RunJob
from backend.business.runs.reports import RunReportRecord
from backend.business.runs.run_entity import StrategyRun


class RunRepositoryPort(Protocol):
    async def next_run_id(
        self,
        reference_date: date | None = None,
        task_type: int = 1,
    ) -> int: ...

    async def get_running_run(self) -> StrategyRun | None: ...

    async def add(self, run: StrategyRun) -> StrategyRun: ...

    async def save(self, run: StrategyRun) -> StrategyRun: ...

    async def get_by_id(self, run_id: int) -> StrategyRun | None: ...

    async def list_runs(
        self,
        limit: int = 100,
        offset: int = 0,
        started_date: date | None = None,
    ) -> list[StrategyRun]: ...

    async def list_run_summaries(
        self,
        limit: int = 100,
        offset: int = 0,
        started_date: date | None = None,
    ) -> list[dict[str, object]]: ...

    async def list_completed_reports(
        self,
        target_date: date,
        limit: int = 100,
        offset: int = 0,
    ) -> list[RunReportRecord]: ...

    async def delete(self, run_id: int) -> None: ...


class RunJobRepositoryPort(Protocol):
    async def get_by_run_id(self, run_id: int) -> RunJob | None: ...

    async def get_active_job(self) -> RunJob | None: ...

    async def create_pending(self, run_id: int) -> RunJob: ...

    async def request_cancel(
        self,
        run_id: int,
        *,
        reason: str,
    ) -> RunJob | None: ...
