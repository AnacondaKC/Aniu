"""Commands for strategy runs."""

from __future__ import annotations

from dataclasses import dataclass

from backend.business.shared.enums import TriggerSource
from backend.business.shared.trading.value_objects import (
    ensure_non_empty_str,
    ensure_positive_int,
)


@dataclass(frozen=True, slots=True)
class StartRunCommand:
    """Application command for creating a new strategy run."""

    trigger_source: TriggerSource = TriggerSource.MANUAL
    schedule_id: int | None = None
    prompt_version: str = "m1-bootstrap"
    risk_rules_version: str = "llm-risk-decision"
    model_name: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "prompt_version",
            ensure_non_empty_str(self.prompt_version, "prompt_version"),
        )
        object.__setattr__(
            self,
            "risk_rules_version",
            ensure_non_empty_str(self.risk_rules_version, "risk_rules_version"),
        )

        if self.trigger_source is TriggerSource.MANUAL and self.schedule_id is not None:
            raise ValueError("manual trigger_source cannot include schedule_id")

        if self.trigger_source is TriggerSource.SCHEDULED:
            if self.schedule_id is None:
                raise ValueError("scheduled trigger_source requires schedule_id")
            ensure_positive_int(self.schedule_id, "schedule_id")

        if self.model_name is not None:
            object.__setattr__(
                self,
                "model_name",
                ensure_non_empty_str(self.model_name, "model_name"),
            )
