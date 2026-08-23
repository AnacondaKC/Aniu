"""Settings and model-channel routes."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Depends, Query, status

from backend.api.deps import (
    ApiRuntimePort,
    get_model_channel_service,
    get_runtime,
    get_settings_service,
    get_stock_api_log_service,
)
from backend.api.schemas.error import error_responses
from backend.api.schemas.settings import (
    AniuAgentPromptRequest,
    AppSettingsResponse,
    CreateModelChannelWithModelsRequest,
    FetchModelCatalogRequest,
    ModelCatalogItemResponse,
    ModelProfileResponse,
    ModelsDevLookupRequest,
    ModelsDevModelResponse,
    SaveModelChannelFields,
    SelectedModelRequest,
    StockApiCallLogPageResponse,
    StockApiSettingsResponse,
    UpdateModelChannelWithModelsRequest,
    UpdateSettingsRequest,
)
from backend.api.security import require_authenticated
from backend.business.settings import ModelProfile, ModelProviderConfig, SelectedModel
from backend.business.settings.channels import ModelChannelService
from backend.business.settings.commands import UpdateSettingsCommand
from backend.business.settings.dto import (
    AppSettingsDTO,
    ModelCatalogItemDTO,
    ModelProfileDTO,
    ModelsDevModelDTO,
    StockApiSettingsDTO,
)
from backend.business.settings.service import SettingsService
from backend.business.stock_api_logs import (
    ListStockApiLogsQuery,
    StockApiLogService,
)
from backend.business.stock_api_logs.catalog import StockApiToolSource
from backend.business.stock_api_logs.dto import StockApiCallLogPageDTO

router = APIRouter(
    prefix="/api/aniu/settings",
    tags=["Settings"],
    dependencies=[Depends(require_authenticated)],
    responses=error_responses(401, 403, 404, 409, 422, 502, 503),
)


def prompt_profile_payload(
    payload: AniuAgentPromptRequest | None,
) -> dict[str, object] | None:
    if payload is None:
        return None
    return payload.model_dump(by_alias=True, exclude_none=True)


@router.get("", response_model=AppSettingsResponse)
async def get_settings(
    service: Annotated[SettingsService, Depends(get_settings_service)],
) -> AppSettingsDTO:
    return await service.get_settings()


@router.put("", response_model=AppSettingsResponse)
async def update_settings(
    service: Annotated[SettingsService, Depends(get_settings_service)],
    runtime: Annotated[ApiRuntimePort, Depends(get_runtime)],
    payload: UpdateSettingsRequest = Body(...),
) -> AppSettingsDTO:
    fields = payload.model_dump(exclude_unset=True)
    if "prompt_profile" in fields:
        fields["prompt_profile"] = prompt_profile_payload(payload.prompt_profile)
    updated = await service.update_settings(UpdateSettingsCommand(**fields))
    if "dream_schedule_time" in fields:
        job_runner = getattr(runtime, "job_runner", None)
        sync_dream_job = getattr(job_runner, "sync_memory_dream_job", None)
        if callable(sync_dream_job):
            await sync_dream_job(updated.dream_schedule_time)
    return updated


@router.get("/stock-api", response_model=StockApiSettingsResponse)
async def get_stock_api_settings(
    service: Annotated[SettingsService, Depends(get_settings_service)],
) -> StockApiSettingsDTO:
    return await service.get_stock_api_settings()


@router.get("/stock-api/logs", response_model=StockApiCallLogPageResponse)
async def list_stock_api_logs(
    service: Annotated[StockApiLogService, Depends(get_stock_api_log_service)],
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    tool_source: Annotated[StockApiToolSource | None, Query()] = None,
    tool_id: Annotated[str | None, Query(min_length=1)] = None,
    status: Annotated[str | None, Query(min_length=1)] = None,
) -> StockApiCallLogPageDTO:
    return await service.list_logs(
        ListStockApiLogsQuery(
            limit=limit,
            offset=offset,
            tool_source=tool_source,
            tool_id=tool_id,
            status=status,
        )
    )


@router.get("/channels", response_model=list[ModelProfileResponse])
async def list_model_channels(
    service: Annotated[ModelChannelService, Depends(get_model_channel_service)],
) -> list[ModelProfileDTO]:
    return await service.list_model_channels()


def _model_profile_entity(
    payload: SaveModelChannelFields,
    *,
    revision: int,
) -> ModelProfile:
    return ModelProfile(
        name=payload.name,
        protocol=payload.protocol,
        model_name=payload.model_name,
        base_url=payload.base_url,
        api_key=payload.api_key,
        provider_config=ModelProviderConfig.from_mapping(
            payload.provider_config.model_dump()
        ),
        enabled=payload.enabled,
        sort_order=payload.sort_order,
        revision=revision,
    )


def _selected_models(
    channel_id: int,
    items: list[SelectedModelRequest],
) -> list[SelectedModel]:
    return [
        SelectedModel(
            channel_profile_id=channel_id,
            model_name=item.model_name,
            label=item.label,
            provider_id=item.provider_id,
            context_window_tokens=item.context_window_tokens,
            max_output_tokens=item.max_output_tokens,
            input_price_per_million=item.input_price_per_million,
            output_price_per_million=item.output_price_per_million,
            cache_read_price_per_million=item.cache_read_price_per_million,
            cache_write_price_per_million=item.cache_write_price_per_million,
            thinking_efforts=tuple(item.thinking_efforts),
            sort_order=item.sort_order,
        )
        for item in items
    ]


@router.delete(
    "/channels/{channel_id}/api-key",
    response_model=ModelProfileResponse,
)
async def clear_model_channel_api_key(
    channel_id: int,
    service: Annotated[ModelChannelService, Depends(get_model_channel_service)],
    expected_revision: int,
) -> ModelProfileDTO:
    return await service.clear_model_channel_api_key(
        channel_id,
        expected_revision=expected_revision,
    )


@router.delete(
    "/channels/{channel_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_model_channel(
    channel_id: int,
    service: Annotated[ModelChannelService, Depends(get_model_channel_service)],
    expected_revision: int,
) -> None:
    await service.delete_model_channel(channel_id, expected_revision=expected_revision)


@router.post(
    "/channels/{channel_id}/models/fetch",
    response_model=list[ModelCatalogItemResponse],
)
async def fetch_model_catalog(
    channel_id: int,
    service: Annotated[ModelChannelService, Depends(get_model_channel_service)],
    payload: FetchModelCatalogRequest = Body(...),
) -> list[ModelCatalogItemDTO]:
    return await service.fetch_available_models(
        channel_id=channel_id,
        protocol=payload.llm_protocol,
        base_url=payload.llm_base_url,
        api_key=payload.llm_api_key,
        provider_config=ModelProviderConfig.from_mapping(
            payload.provider_config.model_dump()
        ),
    )


@router.post(
    "/models/models-dev/lookup",
    response_model=ModelsDevModelResponse,
)
async def lookup_models_dev_model(
    service: Annotated[ModelChannelService, Depends(get_model_channel_service)],
    payload: ModelsDevLookupRequest = Body(...),
) -> ModelsDevModelDTO:
    return await service.lookup_models_dev_model(payload.model_name)


@router.post(
    "/channels/with-models",
    response_model=ModelProfileResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_model_channel_with_models(
    service: Annotated[ModelChannelService, Depends(get_model_channel_service)],
    payload: CreateModelChannelWithModelsRequest = Body(...),
) -> ModelProfileDTO:
    return await service.save_model_channel(
        None,
        _model_profile_entity(payload, revision=0),
        _selected_models(1, payload.selected_models),
    )


@router.put(
    "/channels/{channel_id}/with-models",
    response_model=ModelProfileResponse,
)
async def update_model_channel_with_models(
    channel_id: int,
    service: Annotated[ModelChannelService, Depends(get_model_channel_service)],
    payload: UpdateModelChannelWithModelsRequest = Body(...),
) -> ModelProfileDTO:
    return await service.save_model_channel(
        channel_id,
        _model_profile_entity(payload, revision=payload.expected_revision),
        _selected_models(channel_id, payload.selected_models),
    )
