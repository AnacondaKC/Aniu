"""Generic registry for tools exposed to AgentHarness."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

from backend.llm import ToolDefinition

if TYPE_CHECKING:
    from backend.llm import AbortSignal


class RegisteredTool(Protocol):
    name: str

    def to_tool_definition(self) -> ToolDefinition: ...

    async def run(self, *args: Any, **kwargs: Any) -> object: ...


class ToolRegistry:
    def __init__(self, host_context: object | None = None) -> None:
        self._tools: dict[str, RegisteredTool] = {}
        self.host_context = host_context

    def register(self, tool: RegisteredTool) -> None:
        if tool.name in self._tools:
            raise ValueError(f"tool already registered: {tool.name}")
        self._tools[tool.name] = tool

    def get(self, name: str) -> RegisteredTool:
        try:
            return self._tools[name]
        except KeyError as exc:
            raise KeyError(f"tool not registered: {name}") from exc

    def list_tool_names(self) -> list[str]:
        return sorted(self._tools)

    def list_tools(self) -> list[RegisteredTool]:
        return [self._tools[name] for name in self.list_tool_names()]

    async def call(self, name: str, *args: Any, **kwargs: Any) -> object:
        return await self.get(name).run(*args, **kwargs)

    async def call_with_abort(
        self,
        name: str,
        *args: Any,
        abort_signal: AbortSignal | None,
        **kwargs: Any,
    ) -> object:
        tool = self.get(name)
        run_with_abort = getattr(tool, "run_with_abort", None)
        if callable(run_with_abort):
            return await run_with_abort(*args, abort_signal=abort_signal, **kwargs)
        return await tool.run(*args, **kwargs)


__all__ = ["RegisteredTool", "ToolRegistry"]
