"""Run orchestration entities."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime

from backend.business.runs.run_trace import RunTrace, empty_trace
from backend.business.runs.state_machine_rules import INITIAL_STATE, assert_transition
from backend.business.settings import (
    STAGE_IDS,
    STRATEGY_STAGE_IDS,
    AniuAgentPrompt,
    StageSettings,
    default_stage_settings,
    normalize_stage_settings,
)
from backend.business.shared.enums import RunState, RunStatus, TriggerSource
from backend.business.shared.trading.value_objects import (
    coerce_enum,
    ensure_non_empty_str,
    ensure_positive_int,
)


def utc_now() -> datetime:
    """Return the current UTC time."""

    return datetime.now(tz=UTC)


@dataclass(frozen=True, slots=True)
class StageModelSnapshot:
    """Immutable provider endpoint captured for one stage.

    Credentials remain in Aniu Secret Store and are resolved by profile ID at
    execution time; all non-secret routing values are frozen with the run.
    """

    stage_id: str
    selected_model_id: int
    channel_profile_id: int
    channel_revision: int
    credential_owner_created_at: str
    protocol: str
    base_url: str
    model_name: str
    context_window_tokens: int | None = None
    max_output_tokens: int | None = None
    provider_config_json: str = "{}"

    def __post_init__(self) -> None:
        if self.stage_id not in STAGE_IDS:
            raise ValueError(f"unknown stage_id: {self.stage_id}")
        ensure_positive_int(self.selected_model_id, "selected_model_id")
        ensure_positive_int(self.channel_profile_id, "channel_profile_id")
        if self.channel_revision < 0:
            raise ValueError("channel_revision must be >= 0")
        object.__setattr__(
            self,
            "credential_owner_created_at",
            ensure_non_empty_str(
                self.credential_owner_created_at,
                "credential_owner_created_at",
            ),
        )
        object.__setattr__(
            self, "protocol", ensure_non_empty_str(self.protocol, "protocol")
        )
        object.__setattr__(
            self, "base_url", ensure_non_empty_str(self.base_url, "base_url")
        )
        object.__setattr__(
            self, "model_name", ensure_non_empty_str(self.model_name, "model_name")
        )
        if self.context_window_tokens is not None:
            ensure_positive_int(self.context_window_tokens, "context_window_tokens")
        if self.max_output_tokens is not None:
            ensure_positive_int(self.max_output_tokens, "max_output_tokens")
        object.__setattr__(
            self,
            "provider_config_json",
            ensure_non_empty_str(self.provider_config_json, "provider_config_json"),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> StageModelSnapshot:
        return cls(
            stage_id=str(value.get("stage_id", "")),
            selected_model_id=int(str(value.get("selected_model_id", 0))),
            channel_profile_id=int(str(value.get("channel_profile_id", 0))),
            channel_revision=int(str(value.get("channel_revision", 0))),
            credential_owner_created_at=str(
                value.get("credential_owner_created_at", "")
            ),
            protocol=str(value.get("protocol", "")),
            base_url=str(value.get("base_url", "")),
            model_name=str(value.get("model_name", "")),
            context_window_tokens=(
                None
                if value.get("context_window_tokens") is None
                else int(str(value["context_window_tokens"]))
            ),
            max_output_tokens=(
                None
                if value.get("max_output_tokens") is None
                else int(str(value["max_output_tokens"]))
            ),
            provider_config_json=str(value.get("provider_config_json", "{}")),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "stage_id": self.stage_id,
            "selected_model_id": self.selected_model_id,
            "channel_profile_id": self.channel_profile_id,
            "channel_revision": self.channel_revision,
            "credential_owner_created_at": self.credential_owner_created_at,
            "protocol": self.protocol,
            "base_url": self.base_url,
            "model_name": self.model_name,
            "context_window_tokens": self.context_window_tokens,
            "max_output_tokens": self.max_output_tokens,
            "provider_config_json": self.provider_config_json,
        }


def normalize_stage_model_snapshots(
    value: object,
) -> dict[str, StageModelSnapshot]:
    if not isinstance(value, Mapping):
        return {}
    snapshots: dict[str, StageModelSnapshot] = {}
    for raw in value.values():
        try:
            snapshot = (
                raw
                if isinstance(raw, StageModelSnapshot)
                else StageModelSnapshot.from_mapping(raw)
                if isinstance(raw, Mapping)
                else None
            )
        except (TypeError, ValueError):
            continue
        if snapshot is not None:
            snapshots[snapshot.stage_id] = snapshot
    return {
        stage_id: snapshots[stage_id]
        for stage_id in STRATEGY_STAGE_IDS
        if stage_id in snapshots
    }


@dataclass(frozen=True, slots=True)
class StrategySnapshot:
    """Immutable configuration snapshot captured at run start."""

    prompt_version: str
    risk_rules_version: str
    prompt_profile: AniuAgentPrompt = field(default_factory=AniuAgentPrompt)
    stage_settings: dict[str, StageSettings] = field(default_factory=dict)
    stage_models: dict[str, StageModelSnapshot] = field(default_factory=dict)
    captured_at: datetime = field(default_factory=utc_now)

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
        raw_profile = self.prompt_profile
        if isinstance(raw_profile, AniuAgentPrompt):
            prompt_profile = raw_profile
        elif isinstance(raw_profile, dict) or raw_profile is None:
            prompt_profile = AniuAgentPrompt.from_mapping(raw_profile)
        else:
            raise ValueError("prompt_profile must be an object")
        object.__setattr__(self, "prompt_profile", prompt_profile)
        stage_settings = default_stage_settings(prompt_profile)
        stage_settings.update(normalize_stage_settings(self.stage_settings))
        object.__setattr__(self, "stage_settings", stage_settings)
        object.__setattr__(
            self,
            "stage_models",
            normalize_stage_model_snapshots(self.stage_models),
        )

    def settings_for_stage(self, stage_id: str) -> StageSettings:
        configured = self.stage_settings.get(stage_id)
        if configured is None:
            raise ValueError(f"stage settings not found: {stage_id}")
        return replace(
            configured,
            prompt=self._compose_stage_prompt(configured.prompt),
        )

    def _compose_stage_prompt(self, stage_prompt: str) -> str:
        return "\n\n".join(
            part for part in (self.prompt_profile.global_prompt, stage_prompt) if part
        )


@dataclass(slots=True)
class StrategyRun:
    """Aggregate root for one two-stage strategy run."""

    run_id: int
    trigger_source: TriggerSource
    schedule_id: int | None
    snapshot: StrategySnapshot
    status: RunStatus = RunStatus.RUNNING
    summary: str | None = None
    summary_render_mode: str = "markdown"
    failure_reason: str | None = None
    started_at: datetime = field(default_factory=utc_now)
    completed_at: datetime | None = None
    current_state: RunState = INITIAL_STATE
    trace: RunTrace = field(default_factory=empty_trace)

    def __post_init__(self) -> None:
        self.run_id = ensure_positive_int(self.run_id, "run_id")
        self.trigger_source = coerce_enum(
            self.trigger_source,
            TriggerSource,
            "trigger_source",
        )
        self.status = coerce_enum(self.status, RunStatus, "status")
        self.current_state = coerce_enum(self.current_state, RunState, "current_state")

        if not isinstance(self.snapshot, StrategySnapshot):
            raise ValueError("snapshot must be a StrategySnapshot")
        if isinstance(self.trace, RunTrace):
            trace = self.trace
        elif isinstance(self.trace, dict):
            trace = RunTrace.from_mapping(self.trace)
        else:
            raise ValueError("trace must be a RunTrace")
        self.trace = trace

        if self.summary_render_mode not in {"markdown", "html"}:
            raise ValueError("summary_render_mode must be markdown or html")
        if self.failure_reason is not None:
            self.failure_reason = ensure_non_empty_str(
                self.failure_reason,
                "failure_reason",
            )

        if self.trigger_source is TriggerSource.MANUAL and self.schedule_id is not None:
            raise ValueError("manual runs cannot include schedule_id")
        if self.trigger_source is TriggerSource.SCHEDULED and (
            self.schedule_id is None or self.schedule_id <= 0
        ):
            raise ValueError("scheduled runs require a positive schedule_id")

        if self.status is RunStatus.RUNNING and self.completed_at is not None:
            raise ValueError("running runs cannot have completed_at")
        if (
            self.status
            in {
                RunStatus.COMPLETED,
                RunStatus.FAILED,
                RunStatus.ABORTED,
            }
            and self.completed_at is None
        ):
            raise ValueError("terminal runs require completed_at")

    def advance_to(
        self,
        next_state: RunState | str,
        at: datetime | None = None,
    ) -> None:
        """Advance the run to the next FSM state."""

        if self.status is not RunStatus.RUNNING:
            raise ValueError("cannot transition a run that is not running")

        upcoming = coerce_enum(next_state, RunState, "next_state")
        assert_transition(self.current_state, upcoming)
        self.current_state = upcoming

        if upcoming is RunState.COMPLETED:
            self.status = RunStatus.COMPLETED
            self.completed_at = at or utc_now()

    def fail(
        self,
        reason: str | None = None,
        at: datetime | None = None,
    ) -> None:
        """Fail the run immediately and retain its user-visible reason."""

        if self.status is not RunStatus.RUNNING:
            raise ValueError("cannot fail a run that is not running")

        if reason is not None:
            self.failure_reason = ensure_non_empty_str(reason, "failure_reason")
        self.current_state = RunState.FAILED
        self.status = RunStatus.FAILED
        self.completed_at = at or utc_now()

    def abort(self, at: datetime | None = None) -> None:
        """Abort the run immediately after a cooperative stop request."""

        if self.status is not RunStatus.RUNNING:
            raise ValueError("cannot abort a run that is not running")

        self.current_state = RunState.FAILED
        self.status = RunStatus.ABORTED
        self.completed_at = at or utc_now()

    def set_summary(self, summary: str, *, render_mode: str = "markdown") -> None:
        """Set the final display report and its trusted rendering mode."""

        if render_mode not in {"markdown", "html"}:
            raise ValueError("render_mode must be markdown or html")
        self.summary = ensure_non_empty_str(summary, "summary")
        self.summary_render_mode = render_mode
