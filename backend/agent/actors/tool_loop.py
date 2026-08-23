"""Tool loop result types emitted by agent tools."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from backend.agent.json import json_safe, serialize_context
from backend.llm import ProviderJsonObject

ToolLoopStatus = Literal["ok", "blocked", "error"]


@dataclass(frozen=True, slots=True)
class ToolLoopResult:
    """Structured result for one model-requested tool call."""

    tool_call_id: str
    tool_name: str
    arguments: ProviderJsonObject
    status: ToolLoopStatus
    content: object | None = None
    details: dict[str, object] = field(default_factory=dict)
    error: str | None = None
    iteration: int = 0
    sequence: int = 0

    @property
    def is_error(self) -> bool:
        return self.status != "ok"

    def as_payload(self) -> dict[str, object]:
        safe_content = json_safe(self.content)
        payload: dict[str, object] = {
            "tool_call_id": self.tool_call_id,
            "tool_name": self.tool_name,
            "arguments": json_safe(self.arguments),
            "status": self.status,
            "content": safe_content,
            "details": json_safe(self.details),
            "is_error": self.is_error,
        }
        if self.status == "ok":
            # Same object as content — avoid a second json_safe pass.
            payload["result"] = safe_content
        if self.error:
            payload["error"] = self.error
        if self._is_loop_level_failure():
            payload["record_type"] = "tool_loop_failure"
        return payload

    def _is_loop_level_failure(self) -> bool:
        """True for agent-loop infrastructure failures (not model tool calls)."""

        return self.status == "error" and (
            (self.tool_name, self.tool_call_id)
            in (
                ("tool_loop", "tool-loop"),
                ("context_budget", "context-budget"),
            )
        )

    def as_model_content(self) -> object:
        """Compact payload returned to the model (events keep full as_payload)."""

        if self.status != "ok":
            compact: dict[str, object] = {
                "status": self.status,
                "tool": self.tool_name,
            }
            if self.error:
                compact["error"] = self.error
            if self.content is not None:
                slim = _slim_tool_content(self.content)
                if slim is not None:
                    compact["content"] = slim
            return compact
        return _slim_tool_content(self.content)

    def as_model_text(self) -> str:
        """Serialize tool content written to the Agent transcript before compaction."""

        return serialize_context(self.as_model_content())


_SLIM_DROP_KEYS = frozenset(
    {
        "command",
        "duration_ms",
        "location",
        "base_dir",
        "tool_call_id",
        "iteration",
        "sequence",
        "details",
        "arguments",
        "record_type",
    }
)


def _slim_tool_content(content: object) -> object:
    """Drop verbose fields that do not help the model continue the task."""

    if not isinstance(content, dict):
        return json_safe(content)
    # Single pass: drop noise keys while coercing values.
    return {
        str(key): json_safe(value)
        for key, value in content.items()
        if key not in _SLIM_DROP_KEYS
    }
