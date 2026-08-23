"""Public data contracts for the independent agent package."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, cast

from backend.llm import ChatMessage

AgentStopReason = Literal["completed", "aborted", "error"]
CompactionCause = Literal["proactive", "provider_overflow"]
AgentMessage = ChatMessage

COMPACTION_SUMMARY_PREFIX = (
    "The conversation history before this point was compacted into the following "
    "summary:\n\n<summary>\n"
)
COMPACTION_SUMMARY_SUFFIX = "\n</summary>"
_COMPACTION_SUMMARY_MARKER = "_agent_compaction_checkpoint"


@dataclass(frozen=True, slots=True)
class MessageAppended:
    """A raw transcript message produced during a completed agent turn."""

    message: AgentMessage


@dataclass(frozen=True, slots=True)
class CompactionCheckpoint:
    """A projected context checkpoint while raw transcript entries remain intact."""

    summary: str
    retained_messages: tuple[AgentMessage, ...]
    original_tokens: int
    compacted_tokens: int
    summarized_message_count: int
    cause: CompactionCause


type SessionMutation = MessageAppended | CompactionCheckpoint


@dataclass(frozen=True, slots=True)
class AgentResult:
    content: str
    messages: tuple[AgentMessage, ...] = ()
    session_mutations: tuple[SessionMutation, ...] = ()
    tool_activity: tuple[dict[str, object], ...] = ()
    iterations: int = 0
    stop_reason: AgentStopReason = "completed"


def compaction_summary_message(summary: str) -> AgentMessage:
    """Encode a checkpoint as a provider-safe message with trusted metadata."""

    return cast(
        AgentMessage,
        {
            "role": "user",
            "content": COMPACTION_SUMMARY_PREFIX + summary + COMPACTION_SUMMARY_SUFFIX,
            _COMPACTION_SUMMARY_MARKER: True,
        },
    )


def is_compaction_summary_message(message: AgentMessage) -> bool:
    """Return whether a message carries the internal checkpoint marker."""

    content = message.get("content")
    return (
        message.get(_COMPACTION_SUMMARY_MARKER) is True
        and message.get("role") == "user"
        and isinstance(content, str)
        and content.startswith(COMPACTION_SUMMARY_PREFIX)
        and content.endswith(COMPACTION_SUMMARY_SUFFIX)
    )


def compaction_summary_text(message: AgentMessage) -> str:
    """Extract the text from a validated compaction-summary envelope."""

    if not is_compaction_summary_message(message):
        raise ValueError("message is not a compaction summary")
    content = str(message["content"])
    return content[len(COMPACTION_SUMMARY_PREFIX) : -len(COMPACTION_SUMMARY_SUFFIX)]


__all__ = [
    "AgentMessage",
    "AgentResult",
    "AgentStopReason",
    "COMPACTION_SUMMARY_PREFIX",
    "COMPACTION_SUMMARY_SUFFIX",
    "CompactionCause",
    "CompactionCheckpoint",
    "MessageAppended",
    "SessionMutation",
    "compaction_summary_message",
    "compaction_summary_text",
    "is_compaction_summary_message",
]
