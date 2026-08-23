"""Persistence ports for application and model-channel settings."""

from __future__ import annotations

from typing import Protocol

from backend.business.settings.channels_models import (
    ModelProfile,
    ModelsDevModel,
    SelectedModel,
)
from backend.business.settings.models import AppSettings


class ModelProfileRepositoryPort(Protocol):
    async def list_profiles(self) -> list[ModelProfile]: ...

    async def get_by_id(self, profile_id: int) -> ModelProfile | None: ...

    async def get_by_ids(self, profile_ids: set[int]) -> dict[int, ModelProfile]: ...

    async def create(self, profile: ModelProfile) -> ModelProfile: ...

    async def update(self, profile: ModelProfile) -> ModelProfile: ...

    async def delete(self, profile_id: int) -> None: ...


class SelectedModelRepositoryPort(Protocol):
    async def list_all(self) -> list[SelectedModel]: ...

    async def list_by_channel(self, channel_profile_id: int) -> list[SelectedModel]: ...

    async def get_by_id(self, selected_model_id: int) -> SelectedModel | None: ...

    async def get_by_ids(
        self, selected_model_ids: set[int]
    ) -> dict[int, SelectedModel]: ...

    async def replace_for_channel(
        self,
        channel_profile_id: int,
        models: list[SelectedModel],
    ) -> list[SelectedModel]: ...

    async def delete_by_channel(self, channel_profile_id: int) -> None: ...


class ModelsDevCatalogPort(Protocol):
    async def lookup(self, model_name: str) -> ModelsDevModel | None: ...


class SettingsRepositoryPort(Protocol):
    async def get(self) -> AppSettings | None: ...

    async def save(self, settings: AppSettings) -> AppSettings: ...


__all__ = [
    "ModelProfileRepositoryPort",
    "ModelsDevCatalogPort",
    "SelectedModelRepositoryPort",
    "SettingsRepositoryPort",
]
