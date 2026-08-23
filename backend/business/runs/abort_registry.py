"""Process-local abort registry for the active run executor."""

from __future__ import annotations

from backend.business.runs.abort import RunAbortSignal


class ActiveRunAbortRegistry:
    """Bridge an API abort request to the executor running in this process."""

    def __init__(self) -> None:
        self._signal: RunAbortSignal | None = None

    @property
    def active_signal(self) -> RunAbortSignal | None:
        return self._signal

    def activate(self, run_id: int) -> RunAbortSignal:
        signal = RunAbortSignal(run_id)
        self._signal = signal
        return signal

    def abort(self, run_id: int, reason: str) -> bool:
        signal = self._signal
        if signal is None or signal.run_id != run_id:
            return False
        signal.abort(reason)
        return True

    def clear(self, signal: RunAbortSignal) -> None:
        if self._signal is signal:
            self._signal = None
