"""Authoritative persisted projection for Run Trace v2."""

from __future__ import annotations

from copy import deepcopy


def bound_trace_payload(payload: dict[str, object]) -> dict[str, object]:
    """Return a detached trace without unused raw tool-result payloads."""

    projected = deepcopy(payload)
    stages = projected.get("stages")
    if not isinstance(stages, list):
        return projected
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        steps = stage.get("steps")
        if not isinstance(steps, list):
            continue
        for step in steps:
            if not isinstance(step, dict) or step.get("type") != "tool":
                continue
            data = step.get("data")
            if isinstance(data, dict):
                data.pop("result", None)
    return projected


__all__ = ["bound_trace_payload"]
