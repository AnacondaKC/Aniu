"""FSM transition rules for strategy runs."""

from __future__ import annotations

from backend.business.shared.enums import RunState
from backend.business.shared.trading.value_objects import coerce_enum

INITIAL_STATE = RunState.RUN
TERMINAL_STATES = frozenset({RunState.COMPLETED, RunState.FAILED})

ALLOWED_TRANSITIONS: dict[RunState, frozenset[RunState]] = {
    RunState.RUN: frozenset({RunState.SUMMARY}),
    RunState.SUMMARY: frozenset({RunState.COMPLETED}),
    RunState.COMPLETED: frozenset(),
    RunState.FAILED: frozenset(),
}


def can_transition(
    current_state: RunState | str,
    next_state: RunState | str,
) -> bool:
    """Return whether the transition is allowed."""

    current = coerce_enum(current_state, RunState, "current_state")
    upcoming = coerce_enum(next_state, RunState, "next_state")
    if current in TERMINAL_STATES:
        return False
    return upcoming in ALLOWED_TRANSITIONS[current]


def assert_transition(
    current_state: RunState | str,
    next_state: RunState | str,
) -> None:
    """Raise ValueError when the transition is not allowed."""

    current = coerce_enum(current_state, RunState, "current_state")
    upcoming = coerce_enum(next_state, RunState, "next_state")
    if not can_transition(current, upcoming):
        raise ValueError(
            f"invalid state transition: {current.value} -> {upcoming.value}"
        )
