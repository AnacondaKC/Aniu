"""Runtime state container for one strategy run execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from backend.business.runs import StrategyRun

if TYPE_CHECKING:
    from backend.business.runs.traces import RunTraceRecorder


@dataclass(slots=True)
class RunRuntimeState:
    active_run: StrategyRun | None = None
    trace_recorder: RunTraceRecorder | None = None
    stage_tool_calls: int = 0
