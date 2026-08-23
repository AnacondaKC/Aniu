"""Persisted model-channel entities kept outside the inference runtime."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import UTC, datetime

from backend.llm.contracts import ModelProtocol
from backend.llm.provider_config import ModelProviderConfig
from backend.llm.thinking import ThinkingEffort, normalize_thinking_efforts


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _non_empty(value: object, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field_name} must not be empty")
    return text


def _optional_text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _positive(value: int, field_name: str) -> int:
    if value <= 0:
        raise ValueError(f"{field_name} must be > 0")
    return value


def _optional_positive(value: int | None, field_name: str) -> int | None:
    if value is None:
        return None
    return _positive(value, field_name)


def _optional_price(value: float | None, field_name: str) -> float | None:
    if value is None:
        return None
    if value < 0 or not math.isfinite(value):
        raise ValueError(f"{field_name} must be a finite value >= 0")
    return value


@dataclass(slots=True)
class ModelProfile:
    """Saved model connection profile."""

    name: str
    protocol: ModelProtocol = ModelProtocol.OPENAI_CHAT_COMPLETIONS
    model_name: str = "gpt-4o-mini"
    base_url: str | None = None
    api_key: str | None = None
    provider_config: ModelProviderConfig = field(default_factory=ModelProviderConfig)
    enabled: bool = True
    sort_order: int = 0
    profile_id: int = 0
    revision: int = 0
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime = field(default_factory=_utc_now)

    def __post_init__(self) -> None:
        self.name = _non_empty(self.name, "name")
        self.protocol = ModelProtocol(self.protocol)
        self.model_name = _non_empty(self.model_name, "model_name")
        self.base_url = _optional_text(self.base_url)
        self.api_key = _optional_text(self.api_key)
        if not isinstance(self.provider_config, ModelProviderConfig):
            raise ValueError("provider_config must be ModelProviderConfig")
        if self.profile_id < 0:
            raise ValueError("profile_id must be >= 0")
        if self.revision < 0:
            raise ValueError("revision must be >= 0")
        if self.sort_order < 0:
            raise ValueError("sort_order must be >= 0")


@dataclass(slots=True)
class SelectedModel:
    """Persisted model selected from one channel."""

    channel_profile_id: int
    model_name: str
    label: str
    provider_id: str | None = None
    context_window_tokens: int | None = None
    max_output_tokens: int | None = None
    input_price_per_million: float | None = None
    output_price_per_million: float | None = None
    cache_read_price_per_million: float | None = None
    cache_write_price_per_million: float | None = None
    thinking_efforts: tuple[ThinkingEffort, ...] = ()
    sort_order: int = 0
    selected_model_id: int = 0
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime = field(default_factory=_utc_now)

    def __post_init__(self) -> None:
        self.channel_profile_id = _positive(
            self.channel_profile_id,
            "channel_profile_id",
        )
        self.model_name = _non_empty(self.model_name, "model_name")
        self.label = _non_empty(self.label, "label")
        self.provider_id = _optional_text(self.provider_id)
        self.context_window_tokens = _optional_positive(
            self.context_window_tokens, "context_window_tokens"
        )
        self.max_output_tokens = _optional_positive(
            self.max_output_tokens, "max_output_tokens"
        )
        if (
            self.context_window_tokens is not None
            and self.max_output_tokens is not None
            and self.max_output_tokens > self.context_window_tokens
        ):
            raise ValueError("max_output_tokens must not exceed context_window_tokens")
        self.input_price_per_million = _optional_price(
            self.input_price_per_million, "input_price_per_million"
        )
        self.output_price_per_million = _optional_price(
            self.output_price_per_million, "output_price_per_million"
        )
        self.cache_read_price_per_million = _optional_price(
            self.cache_read_price_per_million, "cache_read_price_per_million"
        )
        self.cache_write_price_per_million = _optional_price(
            self.cache_write_price_per_million, "cache_write_price_per_million"
        )
        self.thinking_efforts = normalize_thinking_efforts(self.thinking_efforts)
        if self.selected_model_id < 0:
            raise ValueError("selected_model_id must be >= 0")
        if self.sort_order < 0:
            raise ValueError("sort_order must be >= 0")


@dataclass(frozen=True, slots=True)
class ModelsDevModel:
    """Exact models.dev metadata used to prefill one selected model."""

    model_name: str
    label: str
    provider_id: str
    context_window_tokens: int
    max_output_tokens: int
    thinking_efforts: tuple[ThinkingEffort, ...] = ()
    input_price_per_million: float | None = None
    output_price_per_million: float | None = None
    cache_read_price_per_million: float | None = None
    cache_write_price_per_million: float | None = None


__all__ = ["ModelProfile", "ModelsDevModel", "SelectedModel"]
