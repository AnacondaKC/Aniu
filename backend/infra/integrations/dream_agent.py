"""Dream Agent assembled on the business-neutral backend/agent harness."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date
from typing import cast

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.agent.harness import AgentHarness
from backend.business.dreams import DreamAgentPort, MemoryDream
from backend.business.settings import AppSettings
from backend.business.settings.ports import SettingsRepositoryPort
from backend.infra.integrations.agent_runtime import AgentRuntimeFactory
from backend.infra.integrations.dream_agent_tools import (
    DreamReportReadTool,
    MemoryListTool,
)
from backend.infra.integrations.memory_agent_tools import (
    MemoryReadTool,
    MemoryWriteTool,
)
from backend.llm import AbortSignal, LLMClientPort


async def _run_tool(tool: object, **kwargs: object) -> object:
    runner = getattr(tool, "run", None)
    if not callable(runner):
        raise TypeError("dream tool does not implement run")
    return await cast(Callable[..., Awaitable[object]], runner)(**kwargs)


class DreamToolRegistry:
    """Allowlist registry that forwards memory writes with the dream task id."""

    def __init__(self, task_id: int, tools: list[object]) -> None:
        self._task_id = task_id
        self._tools = {
            str(getattr(tool, "name", "")): tool
            for tool in tools
            if str(getattr(tool, "name", ""))
        }

    def get(self, name: str) -> object:
        try:
            return self._tools[name]
        except KeyError as exc:
            raise KeyError(f"dream tool not available: {name}") from exc

    def list_tools(self) -> list[object]:
        return [self._tools[name] for name in sorted(self._tools)]

    async def call(self, name: str, **kwargs: object) -> object:
        tool = self.get(name)
        return await _run_tool(tool, **kwargs)

    async def call_with_abort(
        self,
        name: str,
        *,
        abort_signal: AbortSignal | None,
        **kwargs: object,
    ) -> object:
        tool = self.get(name)
        run_for_call = getattr(tool, "run_for_call", None)
        if callable(run_for_call):
            return await run_for_call(
                run_id=self._task_id,
                tool_call_id="",
                abort_signal=abort_signal,
                **kwargs,
            )
        return await _run_tool(tool, **kwargs)

    async def call_idempotently(
        self,
        name: str,
        *,
        tool_call_id: str,
        abort_signal: AbortSignal | None,
        **kwargs: object,
    ) -> object:
        tool = self.get(name)
        run_for_call = getattr(tool, "run_for_call", None)
        if callable(run_for_call):
            return await run_for_call(
                run_id=self._task_id,
                tool_call_id=tool_call_id,
                abort_signal=abort_signal,
                **kwargs,
            )
        return await _run_tool(tool, **kwargs)


@dataclass(slots=True)
class DreamAgentRunner(DreamAgentPort):
    llm_client: LLMClientPort
    runtime_factory: AgentRuntimeFactory
    settings_repo: SettingsRepositoryPort
    session_factory: async_sessionmaker[AsyncSession]

    async def run(self, dream: MemoryDream) -> str:
        settings = await self.settings_repo.get() or AppSettings()
        dream_settings = settings.stage_settings["Dream"]
        runtime = await self.runtime_factory.build_stage_runtime(dream_settings)
        registry = DreamToolRegistry(
            dream.task_id,
            [
                DreamReportReadTool(self.session_factory, dream.target_date),
                MemoryListTool(self.session_factory),
                MemoryReadTool(self.session_factory),
                MemoryWriteTool(self.session_factory),
            ],
        )
        harness = AgentHarness(
            runtime=runtime,
            llm_client=self.llm_client,
            system_prompt=(
                f"{settings.prompt_profile.global_prompt}\n\n"
                f"{dream_settings.prompt}"
            ),
            tool_registry=registry,
            label="Dream",
        )
        result = await harness.prompt(_dream_request(dream.target_date))
        return result.content.strip()


def _dream_request(target_date: date) -> str:
    return (
        f"请整理 {target_date.isoformat()} 这一天的运行报告和长期记忆。"
        "请先分页阅读当日报告和全部当前记忆，再自主决定需要创建、更新、"
        "或软删除的记忆。更新和删除必须带上最近读取到的 expected_version。"
        "不要调用任何交易或股票工具；完成后汇报本次整理结果。"
    )


__all__ = ["DreamAgentRunner", "DreamToolRegistry"]
