"""Application settings entity."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime

from backend.business.settings.prompt import (
    AniuAgentPrompt,
    normalize_optional_str,
    utc_now,
)
from backend.business.settings.stages import (
    STAGE_IDS,
    StageSettings,
    default_stage_settings,
    normalize_stage_settings,
)

DEFAULT_DREAM_SCHEDULE_TIME = "00:30"


def normalize_dream_schedule_time(value: str) -> str:
    """Validate and normalize the daily Dream execution time."""

    normalized = value.strip()
    try:
        parsed = datetime.strptime(normalized, "%H:%M")
    except ValueError as exc:
        raise ValueError("dream_schedule_time must use HH:MM format") from exc
    result = parsed.strftime("%H:%M")
    if result != normalized:
        raise ValueError("dream_schedule_time must use HH:MM format")
    return result


@dataclass(slots=True)
class AppSettings:
    """Persisted settings with one authoritative value for each runtime option."""

    mx_api_key: str | None = None
    prompt_profile: AniuAgentPrompt = field(default_factory=AniuAgentPrompt)
    stage_settings: dict[str, StageSettings] = field(default_factory=dict)
    dream_schedule_time: str = DEFAULT_DREAM_SCHEDULE_TIME
    revision: int = 0
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        self.mx_api_key = normalize_optional_str(self.mx_api_key)
        self.dream_schedule_time = normalize_dream_schedule_time(
            self.dream_schedule_time
        )
        raw_profile = self.prompt_profile
        if isinstance(raw_profile, AniuAgentPrompt):
            prompt_profile = raw_profile
        elif isinstance(raw_profile, Mapping) or raw_profile is None:
            prompt_profile = AniuAgentPrompt.from_mapping(raw_profile)
        else:
            raise ValueError("prompt_profile must be an object")
        self.prompt_profile = prompt_profile

        configured = normalize_stage_settings(self.stage_settings)
        defaults = default_stage_settings(prompt_profile)
        if "Dream" not in configured:
            run_settings = configured.get("Run")
            if (
                run_settings is not None
                and run_settings.model_selected_model_id is not None
            ):
                defaults["Dream"] = replace(
                    defaults["Dream"],
                    model_selected_model_id=run_settings.model_selected_model_id,
                )
        self.stage_settings = {
            stage_id: configured.get(stage_id, defaults[stage_id])
            for stage_id in STAGE_IDS
        }
        if self.revision < 0:
            raise ValueError("revision must be >= 0")
