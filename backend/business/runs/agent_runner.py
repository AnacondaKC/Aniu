"""Application-owned boundary for creating and invoking generic agents."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from backend.llm import AbortSignal


@dataclass(frozen=True, slots=True)
class AgentStageResult:
    content: str
    tool_activity: tuple[dict[str, object], ...] = ()
    transcript: tuple[dict[str, object], ...] = ()


class AgentRunnerPort(Protocol):
    async def prepare_prompt(
        self,
        message: str,
        *,
        preserve_prefix: str = "",
        abort_signal: AbortSignal | None = None,
    ) -> str: ...

    async def prompt(
        self,
        message: str,
        *,
        abort_signal: AbortSignal | None = None,
    ) -> AgentStageResult: ...


@dataclass(frozen=True, slots=True)
class AgentRuntimeBundle:
    tool_registry: object | None
    stage_runtimes: dict[str, object]


class AgentRunnerFactoryPort(Protocol):
    async def prepare(self, snapshot: object) -> AgentRuntimeBundle: ...

    def create(
        self,
        context: object,
        *,
        label: str,
        runtime: object,
    ) -> AgentRunnerPort: ...


__all__ = [
    "AgentRunnerFactoryPort",
    "AgentRunnerPort",
    "AgentRuntimeBundle",
    "AgentStageResult",
]
