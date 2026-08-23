"""Append-only in-memory transcript owned and committed by ``AgentHarness``."""

from __future__ import annotations

from dataclasses import dataclass, field

from backend.agent.contracts import (
    AgentMessage,
    CompactionCheckpoint,
    MessageAppended,
    SessionMutation,
    compaction_summary_message,
)


@dataclass(slots=True)
class AgentSession:
    """Keep raw transcript events and project the latest usable context on demand."""

    _journal: list[SessionMutation] = field(default_factory=list)

    @property
    def journal(self) -> tuple[SessionMutation, ...]:
        """Return the append-only source of truth, including old raw messages."""

        return tuple(self._journal)

    @property
    def messages(self) -> tuple[AgentMessage, ...]:
        """Return the current model context without discarding original history."""

        latest_checkpoint_index = next(
            (
                index
                for index in range(len(self._journal) - 1, -1, -1)
                if isinstance(self._journal[index], CompactionCheckpoint)
            ),
            None,
        )
        if latest_checkpoint_index is None:
            return tuple(
                entry.message
                for entry in self._journal
                if isinstance(entry, MessageAppended)
            )

        checkpoint = self._journal[latest_checkpoint_index]
        assert isinstance(checkpoint, CompactionCheckpoint)
        system_message = next(
            (
                entry.message
                for entry in self._journal
                if isinstance(entry, MessageAppended)
                and entry.message.get("role") == "system"
            ),
            None,
        )
        projected: list[AgentMessage] = []
        if system_message is not None:
            projected.append(system_message)
        projected.append(compaction_summary_message(checkpoint.summary))
        projected.extend(checkpoint.retained_messages)
        projected.extend(
            entry.message
            for entry in self._journal[latest_checkpoint_index + 1 :]
            if isinstance(entry, MessageAppended)
        )
        return tuple(projected)

    def commit_turn(self, mutations: tuple[SessionMutation, ...]) -> None:
        """Append one completed turn's mutations without rewriting prior history."""

        for mutation in mutations:
            if (
                isinstance(mutation, CompactionCheckpoint)
                and not mutation.summary.strip()
            ):
                raise ValueError("compaction checkpoint summary must not be empty")
        self._journal.extend(mutations)

    def clear(self) -> None:
        self._journal.clear()


__all__ = ["AgentSession"]
