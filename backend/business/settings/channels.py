"""Model channel / selected-model use cases."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

from backend.business.settings import (
    AppSettings,
    ModelProfile,
    ModelProfileRepositoryPort,
    SelectedModel,
    SelectedModelRepositoryPort,
    StageSettings,
)
from backend.business.settings.dto import (
    ModelCatalogItemDTO,
    ModelProfileDTO,
    ModelsDevModelDTO,
    SelectedModelDTO,
    to_model_catalog_item_dto,
    to_model_profile_dto,
    to_models_dev_model_dto,
    to_selected_model_dto,
)
from backend.business.settings.ports import ModelsDevCatalogPort, SettingsRepositoryPort
from backend.business.settings.resolver import ModelSelectionResolver
from backend.business.shared import (
    CommitterPort,
    ConfigurationConflictError,
    IntegrationErrorCode,
    ServiceConfigurationError,
    ServiceIntegrationError,
)
from backend.llm import (
    LLMConfigurationError,
    LLMIntegrationError,
    ModelConnectivityTesterPort,
    ModelProtocol,
    ModelProviderConfig,
)


def _resolve_channel_api_key(
    incoming: str | None,
    existing: str | None,
) -> str | None:
    """Preserve the stored key when the client omits or blanks the secret field.

    Clients no longer receive full API keys on GET. An empty/None update means
    "keep current secret"; only a non-empty string replaces it.
    """

    if incoming is None:
        return existing
    stripped = incoming.strip()
    if not stripped:
        return existing
    return stripped


def _as_service_error(exc: LLMIntegrationError) -> ServiceIntegrationError:
    if isinstance(exc, LLMConfigurationError):
        return ServiceConfigurationError(str(exc), status_code=exc.status_code)
    return ServiceIntegrationError(
        str(exc),
        status_code=exc.status_code,
        error_code=IntegrationErrorCode(exc.error_code.value),
    )


class ModelChannelService:
    """Manage model channels, selected models, and connectivity checks."""

    def __init__(
        self,
        *,
        settings_repo: SettingsRepositoryPort,
        model_profile_repo: ModelProfileRepositoryPort,
        selected_model_repo: SelectedModelRepositoryPort,
        model_resolver: ModelSelectionResolver,
        model_connectivity_tester: ModelConnectivityTesterPort | None = None,
        models_dev_catalog: ModelsDevCatalogPort | None = None,
        committer: CommitterPort | None = None,
    ) -> None:
        self._settings_repo = settings_repo
        self._model_profile_repo = model_profile_repo
        self._selected_model_repo = selected_model_repo
        self._model_resolver = model_resolver
        self._model_connectivity_tester = model_connectivity_tester
        self._models_dev_catalog = models_dev_catalog
        self._committer = committer

    async def list_model_channels(self) -> list[ModelProfileDTO]:
        profiles = await self._model_profile_repo.list_profiles()
        selected_by_channel = await self._selected_model_dtos_by_channel()
        return [
            to_model_profile_dto(
                profile,
                tuple(selected_by_channel.get(profile.profile_id, [])),
            )
            for profile in profiles
        ]

    async def save_model_channel(
        self,
        channel_id: int | None,
        channel: ModelProfile,
        selected_models: list[SelectedModel],
    ) -> ModelProfileDTO:
        if channel_id is None:
            stored = await self._model_profile_repo.create(channel)
        else:
            existing = await self._require_channel(channel_id, "channel_id")
            stored = await self._model_profile_repo.update(
                replace(
                    existing,
                    name=channel.name,
                    protocol=channel.protocol,
                    model_name=channel.model_name,
                    base_url=channel.base_url,
                    api_key=_resolve_channel_api_key(channel.api_key, existing.api_key),
                    provider_config=channel.provider_config,
                    enabled=channel.enabled,
                    sort_order=channel.sort_order,
                    revision=channel.revision,
                    updated_at=datetime.now(tz=UTC),
                )
            )

        old_models = (
            []
            if channel_id is None
            else await self._selected_model_repo.list_by_channel(stored.profile_id)
        )
        rebound_models = [
            SelectedModel(
                channel_profile_id=stored.profile_id,
                model_name=model.model_name,
                label=model.label,
                provider_id=model.provider_id,
                context_window_tokens=model.context_window_tokens,
                max_output_tokens=model.max_output_tokens,
                input_price_per_million=model.input_price_per_million,
                output_price_per_million=model.output_price_per_million,
                cache_read_price_per_million=model.cache_read_price_per_million,
                cache_write_price_per_million=model.cache_write_price_per_million,
                thinking_efforts=model.thinking_efforts,
                sort_order=model.sort_order,
                created_at=model.created_at,
                updated_at=model.updated_at,
            )
            for model in selected_models
        ]
        stored_models = await self._selected_model_repo.replace_for_channel(
            stored.profile_id,
            rebound_models,
        )
        await self._sync_settings_after_selected_models_change(
            old_models,
            stored_models,
        )
        if self._committer is not None:
            await self._committer.commit()
        return to_model_profile_dto(
            stored,
            tuple(to_selected_model_dto(model) for model in stored_models),
        )

    async def clear_model_channel_api_key(
        self,
        channel_id: int,
        *,
        expected_revision: int,
    ) -> ModelProfileDTO:
        existing = await self._require_channel(channel_id, "channel_id")
        if expected_revision != existing.revision:
            raise ConfigurationConflictError(
                "model_profile", expected_revision, existing.revision
            )
        stored = await self._model_profile_repo.update(
            replace(
                existing,
                api_key=None,
                updated_at=datetime.now(tz=UTC),
            )
        )
        if self._committer is not None:
            await self._committer.commit()
        selected_models = await self._selected_model_repo.list_by_channel(channel_id)
        return to_model_profile_dto(
            stored,
            tuple(to_selected_model_dto(model) for model in selected_models),
        )

    async def delete_model_channel(
        self,
        channel_id: int,
        *,
        expected_revision: int | None = None,
    ) -> None:
        channel = await self._require_channel(channel_id, "channel_id")
        if expected_revision is not None and expected_revision != channel.revision:
            raise ConfigurationConflictError(
                "model_profile", expected_revision, channel.revision
            )
        settings = await self._settings_repo.get()
        old_models = await self._selected_model_repo.list_by_channel(channel_id)
        await self._selected_model_repo.delete_by_channel(channel_id)
        await self._model_profile_repo.delete(channel_id)
        if settings is not None:
            updated = self._replace_changed_channel_references(
                settings,
                old_models=old_models,
                stored_models=[],
            )
            refreshed = await self._model_resolver.reconcile(updated)
            if refreshed != settings:
                await self._settings_repo.save(refreshed)
        if self._committer is not None:
            await self._committer.commit()

    async def fetch_available_models(
        self,
        *,
        channel_id: int,
        protocol: ModelProtocol,
        base_url: str,
        api_key: str | None,
        provider_config: ModelProviderConfig,
    ) -> list[ModelCatalogItemDTO]:
        if self._model_connectivity_tester is None:
            raise RuntimeError("model connectivity tester is not configured")

        resolved_api_key = api_key.strip() if api_key is not None else ""
        if not resolved_api_key and channel_id > 0:
            channel = await self._require_channel(channel_id, "channel_id")
            resolved_api_key = channel.api_key or ""
        if not resolved_api_key:
            raise ServiceConfigurationError(
                "请填写 API 密钥，或先保存包含 API 密钥的模型渠道"
            )

        try:
            items = await self._model_connectivity_tester.list_models(
                protocol=protocol,
                base_url=base_url,
                api_key=resolved_api_key,
                provider_config=provider_config,
            )
        except LLMIntegrationError as exc:
            raise _as_service_error(exc) from exc
        return [to_model_catalog_item_dto(item) for item in items]

    async def lookup_models_dev_model(self, model_name: str) -> ModelsDevModelDTO:
        if self._models_dev_catalog is None:
            raise RuntimeError("models.dev catalog is not configured")
        try:
            model = await self._models_dev_catalog.lookup(model_name)
        except Exception as exc:
            raise ServiceIntegrationError(
                f"models.dev 获取失败：{exc}",
                error_code=IntegrationErrorCode.NETWORK,
            ) from exc
        if model is None:
            raise ValueError(f"models.dev 未找到唯一精确匹配：{model_name}")
        return to_models_dev_model_dto(model)

    async def _selected_model_dtos_by_channel(
        self,
    ) -> dict[int, list[SelectedModelDTO]]:
        grouped: dict[int, list[SelectedModelDTO]] = {}
        for model in await self._selected_model_repo.list_all():
            grouped.setdefault(model.channel_profile_id, []).append(
                to_selected_model_dto(model)
            )
        return grouped

    async def _sync_settings_after_selected_models_change(
        self,
        old_models: list[SelectedModel],
        stored_models: list[SelectedModel],
    ) -> None:
        settings = await self._settings_repo.get()
        if settings is None:
            return

        updated = self._replace_changed_channel_references(
            settings,
            old_models=old_models,
            stored_models=stored_models,
        )
        refreshed = await self._model_resolver.reconcile(updated)
        if refreshed != settings:
            await self._settings_repo.save(refreshed)

    @staticmethod
    def _replace_changed_channel_references(
        settings: AppSettings,
        *,
        old_models: list[SelectedModel],
        stored_models: list[SelectedModel],
    ) -> AppSettings:
        """Reconcile stage references and unavailable thinking presets.

        Stable models retain their IDs. References to deleted catalogue entries
        fall back to the first remaining entry, including all four stage models.
        A model edit may also remove a configured effort preset, in which case
        affected stages safely return to the provider default.
        """

        old_ids = {model.selected_model_id for model in old_models}
        stored_ids = {model.selected_model_id for model in stored_models}
        removed_ids = old_ids - stored_ids
        fallback_id = stored_models[0].selected_model_id if stored_models else None
        stored_by_id = {
            model.selected_model_id: model
            for model in stored_models
            if model.selected_model_id > 0
        }

        def replacement(stage: StageSettings) -> StageSettings:
            selected_model_id = (
                fallback_id
                if stage.model_selected_model_id in removed_ids
                else stage.model_selected_model_id
            )
            model = (
                stored_by_id.get(selected_model_id)
                if selected_model_id is not None
                else None
            )
            thinking_effort = stage.thinking_effort
            if selected_model_id is None:
                thinking_effort = None
            elif model is not None and thinking_effort not in model.thinking_efforts:
                thinking_effort = None
            if selected_model_id == stage.model_selected_model_id and (
                thinking_effort == stage.thinking_effort
            ):
                return stage
            return replace(
                stage,
                model_selected_model_id=selected_model_id,
                thinking_effort=thinking_effort,
            )

        stage_settings = {
            stage_id: replacement(stage)
            for stage_id, stage in settings.stage_settings.items()
        }
        return (
            settings
            if stage_settings == settings.stage_settings
            else replace(settings, stage_settings=stage_settings)
        )

    async def _require_channel(
        self,
        channel_id: int | None,
        field_name: str,
    ) -> ModelProfile:
        if channel_id is None:
            raise ValueError(f"{field_name} is required")
        channel = await self._model_profile_repo.get_by_id(channel_id)
        if channel is None:
            raise ValueError(f"{field_name} points to a missing channel")
        return channel
