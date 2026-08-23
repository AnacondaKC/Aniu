"""Resolve stage model selections and clear dangling references."""

from __future__ import annotations

from dataclasses import dataclass, replace

from backend.business.settings import (
    AppSettings,
    ModelProfile,
    ModelProfileRepositoryPort,
    SelectedModel,
    SelectedModelRepositoryPort,
)


@dataclass(frozen=True, slots=True)
class ResolvedModel:
    selected: SelectedModel
    profile: ModelProfile


class ModelSelectionResolver:
    def __init__(
        self,
        *,
        model_profile_repo: ModelProfileRepositoryPort,
        selected_model_repo: SelectedModelRepositoryPort,
    ) -> None:
        self._model_profile_repo = model_profile_repo
        self._selected_model_repo = selected_model_repo

    async def reconcile(self, settings: AppSettings) -> AppSettings:
        stages = dict(settings.stage_settings)
        selected_ids = {
            stage.model_selected_model_id
            for stage in stages.values()
            if stage.model_selected_model_id is not None
        }
        resolved = await self.resolve_many(selected_ids)
        changed = False
        for stage_id, stage in stages.items():
            selected_model_id = stage.model_selected_model_id
            if selected_model_id is not None and selected_model_id not in resolved:
                stages[stage_id] = replace(stage, model_selected_model_id=None)
                changed = True
        return replace(settings, stage_settings=stages) if changed else settings

    async def resolve_many(
        self, selected_model_ids: set[int]
    ) -> dict[int, ResolvedModel]:
        selected_models = await self._selected_model_repo.get_by_ids(selected_model_ids)
        profiles = await self._model_profile_repo.get_by_ids(
            {model.channel_profile_id for model in selected_models.values()}
        )
        return {
            selected_id: ResolvedModel(selected=selected, profile=profile)
            for selected_id, selected in selected_models.items()
            if (profile := profiles.get(selected.channel_profile_id)) is not None
        }

    async def resolve_selected(
        self, selected_model_id: int | None
    ) -> ResolvedModel | None:
        if selected_model_id is None:
            return None
        return (await self.resolve_many({selected_model_id})).get(selected_model_id)


__all__ = ["ModelSelectionResolver", "ResolvedModel"]
