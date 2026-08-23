"""Generic lifecycle events emitted by AgentHarness."""

from __future__ import annotations

from enum import StrEnum


class AgentEventType(StrEnum):
    AGENT_STARTED = "AgentStarted"
    TURN_STARTED = "TurnStarted"
    CONTEXT_COMPACTED = "ContextCompacted"
    TOOL_LOOP_STARTED = "ToolLoopStarted"
    TOOL_CALL_REQUESTED = "ToolCallRequested"
    TOOL_CALL_COMPLETED = "ToolCallCompleted"
    TOOL_CALL_BLOCKED = "ToolCallBlocked"
    TOOL_CALL_FAILED = "ToolCallFailed"
    TOOL_LOOP_STOPPED = "ToolLoopStopped"
    TOOL_LOOP_FAILED = "ToolLoopFailed"
    TURN_COMPLETED = "TurnCompleted"
    AGENT_COMPLETED = "AgentCompleted"
    AGENT_ABORTED = "AgentAborted"
    AGENT_FAILED = "AgentFailed"


__all__ = ["AgentEventType"]
