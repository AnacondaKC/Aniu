"""Explicit provider authentication and compatibility overrides."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import cast


class ModelAuthMode(StrEnum):
    """How the configured secret is placed on provider requests."""

    AUTO = "auto"
    BEARER = "bearer"
    API_KEY = "api_key"


class OpenAIMaxTokensField(StrEnum):
    """OpenAI-compatible output-token request field."""

    AUTO = "auto"
    MAX_TOKENS = "max_tokens"
    MAX_COMPLETION_TOKENS = "max_completion_tokens"


@dataclass(frozen=True, slots=True)
class OpenAICompatibilityOverrides:
    """Channel-level overrides for capabilities that compatible gateways vary on."""

    max_tokens_field: OpenAIMaxTokensField = OpenAIMaxTokensField.AUTO
    supports_temperature: bool | None = None
    supports_top_p: bool | None = None
    supports_stream_usage: bool | None = None
    replay_reasoning_content: bool | None = None

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, object] | None,
    ) -> OpenAICompatibilityOverrides:
        if value is None:
            return cls()
        return cls(
            max_tokens_field=OpenAIMaxTokensField(
                cast(
                    str, value.get("max_tokens_field", OpenAIMaxTokensField.AUTO.value)
                )
            ),
            supports_temperature=_optional_bool(
                value.get("supports_temperature"), "supports_temperature"
            ),
            supports_top_p=_optional_bool(
                value.get("supports_top_p"), "supports_top_p"
            ),
            supports_stream_usage=_optional_bool(
                value.get("supports_stream_usage"), "supports_stream_usage"
            ),
            replay_reasoning_content=_optional_bool(
                value.get("replay_reasoning_content"), "replay_reasoning_content"
            ),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "max_tokens_field": self.max_tokens_field.value,
            "supports_temperature": self.supports_temperature,
            "supports_top_p": self.supports_top_p,
            "supports_stream_usage": self.supports_stream_usage,
            "replay_reasoning_content": self.replay_reasoning_content,
        }


@dataclass(frozen=True, slots=True)
class ModelProviderConfig:
    """Provider behavior frozen with a model channel and run snapshot."""

    auth_mode: ModelAuthMode = ModelAuthMode.AUTO
    openai: OpenAICompatibilityOverrides = field(
        default_factory=OpenAICompatibilityOverrides
    )

    @classmethod
    def from_mapping(cls, value: Mapping[str, object] | None) -> ModelProviderConfig:
        if value is None:
            return cls()
        raw_openai = value.get("openai")
        if raw_openai is not None and not isinstance(raw_openai, Mapping):
            raise ValueError("provider config openai must be an object")
        return cls(
            auth_mode=ModelAuthMode(
                cast(str, value.get("auth_mode", ModelAuthMode.AUTO.value))
            ),
            openai=OpenAICompatibilityOverrides.from_mapping(raw_openai),
        )

    @classmethod
    def from_json(cls, value: str) -> ModelProviderConfig:
        try:
            payload = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "provider config snapshot must contain valid JSON"
            ) from exc
        if not isinstance(payload, dict):
            raise ValueError("provider config snapshot must contain an object")
        return cls.from_mapping(payload)

    def as_dict(self) -> dict[str, object]:
        return {
            "auth_mode": self.auth_mode.value,
            "openai": self.openai.as_dict(),
        }

    def as_json(self) -> str:
        return json.dumps(
            self.as_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )


def _optional_bool(value: object, field_name: str) -> bool | None:
    if value is None or isinstance(value, bool):
        return value
    raise ValueError(f"provider config {field_name} must be boolean or null")


__all__ = [
    "ModelAuthMode",
    "ModelProviderConfig",
    "OpenAICompatibilityOverrides",
    "OpenAIMaxTokensField",
]
