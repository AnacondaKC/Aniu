"""Application service for global and per-stage settings."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime

from backend.business.settings import (
    AniuAgentPrompt,
    AppSettings,
    ModelProfileRepositoryPort,
    SelectedModel,
    SelectedModelRepositoryPort,
    StageSettings,
)
from backend.business.settings.commands import UpdateSettingsCommand
from backend.business.settings.dto import (
    AppSettingsDTO,
    StockApiSettingsDTO,
    to_settings_dto,
    to_stock_api_settings_dto,
)
from backend.business.settings.ports import SettingsRepositoryPort
from backend.business.settings.resolver import ModelSelectionResolver
from backend.business.shared import CommitterPort, ConfigurationConflictError


def _normalized_mx_api_key(value: object) -> str | None:
    if value is None:
        return None
    api_key = str(value).strip()
    if not api_key:
        raise ValueError("mx_api_key must not be blank")
    return api_key


class SettingsService:
    """Read and update the single application settings aggregate."""

    def __init__(
        self,
        settings_repo: SettingsRepositoryPort,
        model_profile_repo: ModelProfileRepositoryPort,
        selected_model_repo: SelectedModelRepositoryPort,
        committer: CommitterPort | None = None,
    ) -> None:
        self._settings_repo = settings_repo
        self._selected_model_repo = selected_model_repo
        self._committer = committer
        self._model_resolver = ModelSelectionResolver(
            model_profile_repo=model_profile_repo,
            selected_model_repo=selected_model_repo,
        )

    async def get_settings(self) -> AppSettingsDTO:
        return to_settings_dto(await self._load_settings())

    async def get_stock_api_settings(self) -> StockApiSettingsDTO:
        return to_stock_api_settings_dto(await self._load_settings())

    async def update_settings(self, command: UpdateSettingsCommand) -> AppSettingsDTO:
        visible = await self._settings_repo.get()
        if (
            command.expected_revision is not None
            and visible is not None
            and command.expected_revision != visible.revision
        ):
            raise ConfigurationConflictError(
                "app_settings", command.expected_revision, visible.revision
            )
        current = await self._load_settings()
        mx_api_key = (
            _normalized_mx_api_key(command.mx_api_key)
            if command.provided("mx_api_key")
            else current.mx_api_key
        )
        stage_settings = await self._resolve_stage_settings_update(
            current.stage_settings,
            command.stage_settings if command.provided("stage_settings") else None,
        )
        prompt_profile = (
            AniuAgentPrompt.from_mapping(command.prompt_profile)
            if command.provided("prompt_profile")
            else current.prompt_profile
        )
        dream_schedule_time = (
            command.dream_schedule_time
            if command.provided("dream_schedule_time")
            else current.dream_schedule_time
        )
        updated = await self._model_resolver.reconcile(
            AppSettings(
                mx_api_key=mx_api_key,
                prompt_profile=prompt_profile,
                stage_settings=stage_settings,
                dream_schedule_time=dream_schedule_time,
                revision=current.revision,
                created_at=current.created_at,
                updated_at=datetime.now(tz=UTC),
            )
        )
        stored = await self._settings_repo.save(updated)
        await self._commit()
        return to_settings_dto(stored)

    async def _load_settings(self) -> AppSettings:
        settings = await self._settings_repo.get() or AppSettings()
        return await self._model_resolver.reconcile(settings)

    async def _resolve_stage_settings_update(
        self,
        current: dict[str, StageSettings],
        payload: object | None,
    ) -> dict[str, StageSettings]:
        if payload is None:
            return current
        if not isinstance(payload, list):
            raise ValueError("stage_settings must be an array")
        resolved: dict[str, StageSettings] = {}
        for raw in payload:
            if not isinstance(raw, Mapping):
                raise ValueError("stage_settings entries must be objects")
            item = StageSettings.from_mapping(raw)
            if item.stage_id in resolved:
                raise ValueError(f"duplicate stage settings: {item.stage_id}")
            if item.model_selected_model_id is None:
                raise ValueError(f"{item.stage_id} must select a model")
            selected_model = await self._require_selected_model(
                item.model_selected_model_id,
                f"{item.stage_id}.model_selected_model_id",
            )
            if (
                item.thinking_effort is not None
                and item.thinking_effort not in selected_model.thinking_efforts
            ):
                raise ValueError(
                    f"{item.stage_id}.thinking_effort is not enabled "
                    "for the selected model"
                )
            resolved[item.stage_id] = item
        if set(resolved) != set(current):
            raise ValueError("stage_settings must include every pipeline stage")
        return resolved

    async def _require_selected_model(
        self,
        selected_model_id: int | None,
        field_name: str,
    ) -> SelectedModel:
        if selected_model_id is None:
            raise ValueError(f"{field_name} is required")
        model = await self._selected_model_repo.get_by_id(selected_model_id)
        if model is None:
            raise ValueError(f"{field_name} points to a missing selected model")
        return model

    async def _commit(self) -> None:
        if self._committer is not None:
            await self._committer.commit()
