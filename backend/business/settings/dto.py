"""Business read models for settings, schedules, and model channels."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from backend.business.settings.channels_models import (
    ModelProfile,
    ModelsDevModel,
    SelectedModel,
)
from backend.business.settings.models import AppSettings
from backend.business.settings.mx_interfaces import MX_INTERFACE_CATALOG
from backend.business.settings.prompt import AniuAgentPrompt
from backend.business.settings.public_stock_interfaces import (
    PUBLIC_STOCK_DATA_FEATURES,
    PUBLIC_STOCK_DATA_NAME,
    PUBLIC_STOCK_DATA_PROVIDERS,
    PUBLIC_STOCK_DATA_SUMMARY,
    PUBLIC_STOCK_TOOL_CATALOG,
    PublicStockProvider,
)
from backend.business.settings.stages import StageSettings
from backend.llm import (
    ModelCatalogItem,
    ModelProtocol,
    ModelProviderConfig,
    ThinkingEffort,
)


@dataclass(frozen=True, slots=True)
class AniuAgentPromptDTO:
    schema: str
    name: str
    description: str
    global_prompt: str
    run_prompt: str
    summary_prompt: str
    dream_prompt: str


def _secret_configured(value: str | None) -> bool:
    return bool(value and str(value).strip())


def _secret_last_four(value: str | None) -> str | None:
    if not _secret_configured(value):
        return None
    text = str(value).strip()
    return text[-4:] if len(text) >= 4 else text


@dataclass(frozen=True, slots=True)
class StageSettingsDTO:
    stage_id: str
    model_selected_model_id: int | None
    temperature: float
    top_p: float
    thinking_effort: ThinkingEffort | None
    prompt: str


@dataclass(frozen=True, slots=True)
class AppSettingsDTO:
    mx: MxSettingsDTO
    prompt_profile: AniuAgentPromptDTO
    stage_settings: tuple[StageSettingsDTO, ...]
    dream_schedule_time: str
    revision: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class MxInterfaceDTO:
    id: str
    name: str
    summary: str
    features: tuple[str, ...]
    examples: tuple[str, ...]
    access_modes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MxSettingsDTO:
    api_key_configured: bool
    api_key_last_four: str | None


@dataclass(frozen=True, slots=True)
class StockApiMxCatalogDTO:
    interfaces: tuple[MxInterfaceDTO, ...]


@dataclass(frozen=True, slots=True)
class PublicStockToolDTO:
    tool_name: str
    name: str
    summary: str
    actions: tuple[str, ...]
    providers: tuple[PublicStockProvider, ...]


@dataclass(frozen=True, slots=True)
class StockApiPublicSettingsDTO:
    name: str
    summary: str
    providers: tuple[PublicStockProvider, ...]
    features: tuple[str, ...]
    tools: tuple[PublicStockToolDTO, ...]


@dataclass(frozen=True, slots=True)
class StockApiSettingsDTO:
    mx: StockApiMxCatalogDTO
    public_stock: StockApiPublicSettingsDTO


@dataclass(frozen=True, slots=True)
class ModelProfileDTO:
    selected_models: tuple[SelectedModelDTO, ...]
    profile_id: int
    name: str
    protocol: ModelProtocol
    model_name: str
    base_url: str | None
    provider_config: ModelProviderConfig
    api_key_configured: bool
    api_key_last_four: str | None
    enabled: bool
    sort_order: int
    revision: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ModelCatalogItemDTO:
    model: str
    label: str
    provider_id: str | None


@dataclass(frozen=True, slots=True)
class SelectedModelDTO:
    selected_model_id: int
    channel_profile_id: int
    model_name: str
    label: str
    provider_id: str | None
    context_window_tokens: int | None
    max_output_tokens: int | None
    input_price_per_million: float | None
    output_price_per_million: float | None
    cache_read_price_per_million: float | None
    cache_write_price_per_million: float | None
    thinking_efforts: tuple[ThinkingEffort, ...]
    sort_order: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ModelsDevModelDTO:
    model_name: str
    label: str
    provider_id: str
    context_window_tokens: int
    max_output_tokens: int
    thinking_efforts: tuple[ThinkingEffort, ...]
    input_price_per_million: float | None
    output_price_per_million: float | None
    cache_read_price_per_million: float | None
    cache_write_price_per_million: float | None


def to_settings_dto(settings: AppSettings) -> AppSettingsDTO:
    return AppSettingsDTO(
        mx=MxSettingsDTO(
            api_key_configured=_secret_configured(settings.mx_api_key),
            api_key_last_four=_secret_last_four(settings.mx_api_key),
        ),
        prompt_profile=to_prompt_profile_dto(settings.prompt_profile),
        stage_settings=tuple(
            to_stage_settings_dto(item) for item in settings.stage_settings.values()
        ),
        dream_schedule_time=settings.dream_schedule_time,
        revision=settings.revision,
        created_at=settings.created_at,
        updated_at=settings.updated_at,
    )


def _mx_interfaces() -> tuple[MxInterfaceDTO, ...]:
    return tuple(
        MxInterfaceDTO(
            id=item.interface_id,
            name=item.name,
            summary=item.summary,
            features=item.features,
            examples=item.examples,
            access_modes=item.access_modes,
        )
        for item in MX_INTERFACE_CATALOG
    )


def _public_stock_settings() -> StockApiPublicSettingsDTO:
    return StockApiPublicSettingsDTO(
        name=PUBLIC_STOCK_DATA_NAME,
        summary=PUBLIC_STOCK_DATA_SUMMARY,
        providers=PUBLIC_STOCK_DATA_PROVIDERS,
        features=PUBLIC_STOCK_DATA_FEATURES,
        tools=tuple(
            PublicStockToolDTO(
                tool_name=item.tool_name,
                name=item.name,
                summary=item.summary,
                actions=item.actions,
                providers=item.providers,
            )
            for item in PUBLIC_STOCK_TOOL_CATALOG
        ),
    )


def to_stock_api_settings_dto(settings: AppSettings) -> StockApiSettingsDTO:
    return StockApiSettingsDTO(
        mx=StockApiMxCatalogDTO(interfaces=_mx_interfaces()),
        public_stock=_public_stock_settings(),
    )


def to_stage_settings_dto(settings: StageSettings) -> StageSettingsDTO:
    return StageSettingsDTO(
        stage_id=settings.stage_id,
        model_selected_model_id=settings.model_selected_model_id,
        temperature=settings.temperature,
        top_p=settings.top_p,
        thinking_effort=settings.thinking_effort,
        prompt=settings.prompt,
    )


def to_prompt_profile_dto(profile: AniuAgentPrompt) -> AniuAgentPromptDTO:
    return AniuAgentPromptDTO(
        schema=profile.schema,
        name=profile.name,
        description=profile.description,
        global_prompt=profile.global_prompt,
        run_prompt=profile.run_prompt,
        summary_prompt=profile.summary_prompt,
        dream_prompt=profile.dream_prompt,
    )


def to_selected_model_dto(model: SelectedModel) -> SelectedModelDTO:
    return SelectedModelDTO(
        selected_model_id=model.selected_model_id,
        channel_profile_id=model.channel_profile_id,
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


def to_model_profile_dto(
    profile: ModelProfile,
    selected_models: tuple[SelectedModelDTO, ...] = (),
) -> ModelProfileDTO:
    return ModelProfileDTO(
        selected_models=selected_models,
        profile_id=profile.profile_id,
        name=profile.name,
        protocol=profile.protocol,
        model_name=profile.model_name,
        base_url=profile.base_url,
        provider_config=profile.provider_config,
        api_key_configured=_secret_configured(profile.api_key),
        api_key_last_four=_secret_last_four(profile.api_key),
        enabled=profile.enabled,
        sort_order=profile.sort_order,
        revision=profile.revision,
        created_at=profile.created_at,
        updated_at=profile.updated_at,
    )


def to_model_catalog_item_dto(item: ModelCatalogItem) -> ModelCatalogItemDTO:
    return ModelCatalogItemDTO(
        model=item.model,
        label=item.label,
        provider_id=item.provider_id,
    )


def to_models_dev_model_dto(model: ModelsDevModel) -> ModelsDevModelDTO:
    return ModelsDevModelDTO(
        model_name=model.model_name,
        label=model.label,
        provider_id=model.provider_id,
        context_window_tokens=model.context_window_tokens,
        max_output_tokens=model.max_output_tokens,
        thinking_efforts=model.thinking_efforts,
        input_price_per_million=model.input_price_per_million,
        output_price_per_million=model.output_price_per_million,
        cache_read_price_per_million=model.cache_read_price_per_million,
        cache_write_price_per_million=model.cache_write_price_per_million,
    )
