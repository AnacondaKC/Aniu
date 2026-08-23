"""Provider-neutral cancellation helper for agent runtime work."""

from __future__ import annotations

from backend.llm import AbortSignal


def throw_if_aborted(abort_signal: AbortSignal | None) -> None:
    if abort_signal is not None:
        abort_signal.throw_if_aborted()


__all__ = ["throw_if_aborted"]
