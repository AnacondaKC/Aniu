"""Per-stage runtime configuration for Run and Summary."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from backend.business.settings.prompt import AniuAgentPrompt, normalize_prompt_text
from backend.llm import ThinkingEffort, coerce_thinking_effort

STAGE_IDS: tuple[str, ...] = ("Run", "Summary", "Dream")
STRATEGY_STAGE_IDS: tuple[str, ...] = ("Run", "Summary")

_PROMPT_FIELD_BY_STAGE = {
    "Run": "run_prompt",
    "Summary": "summary_prompt",
    "Dream": "dream_prompt",
}


@dataclass(frozen=True, slots=True)
class StageSettings:
    """Model and prompt configuration for one of the two runtime stages."""

    stage_id: str
    model_selected_model_id: int | None
    temperature: float
    top_p: float
    prompt: str
    thinking_effort: ThinkingEffort | None = None

    def __post_init__(self) -> None:
        if self.stage_id not in STAGE_IDS:
            raise ValueError(f"unknown stage_id: {self.stage_id}")
        if (
            self.model_selected_model_id is not None
            and self.model_selected_model_id <= 0
        ):
            raise ValueError("model_selected_model_id must be positive when provided")
        if not 0 <= self.temperature <= 2:
            raise ValueError("temperature must be between 0 and 2")
        if not 0 <= self.top_p <= 1:
            raise ValueError("top_p must be between 0 and 1")
        prompt = normalize_prompt_text(self.prompt)
        if not prompt:
            raise ValueError("prompt must not be empty")
        object.__setattr__(
            self, "thinking_effort", coerce_thinking_effort(self.thinking_effort)
        )
        object.__setattr__(self, "prompt", prompt)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> StageSettings:
        selected_model_id = value.get("model_selected_model_id")
        return cls(
            stage_id=str(value.get("stage_id", "")),
            model_selected_model_id=(
                None if selected_model_id is None else int(selected_model_id)
            ),
            temperature=float(value.get("temperature", 0)),
            top_p=float(value.get("top_p", 1)),
            thinking_effort=coerce_thinking_effort(value.get("thinking_effort")),
            prompt="" if value.get("prompt") is None else str(value.get("prompt")),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "stage_id": self.stage_id,
            "model_selected_model_id": self.model_selected_model_id,
            "temperature": self.temperature,
            "top_p": self.top_p,
            "thinking_effort": self.thinking_effort,
            "prompt": self.prompt,
        }


def default_stage_settings(
    prompt_profile: AniuAgentPrompt | None = None,
) -> dict[str, StageSettings]:
    profile = prompt_profile or AniuAgentPrompt()
    return {
        stage_id: StageSettings(
            stage_id=stage_id,
            model_selected_model_id=None,
            temperature=0.0,
            top_p=1.0,
            prompt=profile.prompt_text(_PROMPT_FIELD_BY_STAGE[stage_id]),
        )
        for stage_id in STAGE_IDS
    }


def normalize_stage_settings(value: object) -> dict[str, StageSettings]:
    items: Iterable[object]
    if isinstance(value, Mapping):
        items = value.values()
    elif isinstance(value, (list, tuple)):
        items = value
    else:
        return {}
    result: dict[str, StageSettings] = {}
    for item in items:
        try:
            setting = (
                item
                if isinstance(item, StageSettings)
                else StageSettings.from_mapping(item)
                if isinstance(item, Mapping)
                else None
            )
        except (TypeError, ValueError):
            continue
        if setting is not None:
            result[setting.stage_id] = setting
    return {stage_id: result[stage_id] for stage_id in STAGE_IDS if stage_id in result}
