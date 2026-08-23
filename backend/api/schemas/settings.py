"""Settings and model-channel API schemas."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)

from backend.api.schemas.common import ApiModel
from backend.business.settings import (
    PROMPT_PROFILE_SCHEMA,
    ModelAuthMode,
    ModelProtocol,
    OpenAIMaxTokensField,
    ThinkingEffort,
)
from backend.business.settings.public_stock_interfaces import PublicStockProvider
from backend.business.stock_api_logs.catalog import StockApiToolSource

type StockApiErrorCategory = Literal[
    "timeout",
    "network",
    "rate_limited",
    "upstream_http",
    "invalid_response",
    "business_failure",
    "cancelled",
    "unknown",
]


class AniuAgentPromptResponse(ApiModel):
    schema_value: str = Field(validation_alias="schema", serialization_alias="schema")
    name: str
    description: str
    global_prompt: str
    run_prompt: str
    summary_prompt: str
    dream_prompt: str

    model_config = {**ApiModel.model_config, "populate_by_name": True}


class StageSettingsResponse(ApiModel):
    stage_id: str
    model_selected_model_id: int | None = None
    temperature: float
    top_p: float
    thinking_effort: ThinkingEffort | None = None
    prompt: str


class MxSettingsResponse(ApiModel):
    api_key_configured: bool
    api_key_last_four: str | None = None


class AppSettingsResponse(ApiModel):
    mx: MxSettingsResponse
    prompt_profile: AniuAgentPromptResponse
    stage_settings: list[StageSettingsResponse]
    dream_schedule_time: str = Field(pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$")
    revision: int
    created_at: datetime
    updated_at: datetime


class MxInterfaceResponse(ApiModel):
    id: str
    name: str
    summary: str
    features: list[str]
    examples: list[str]
    access_modes: list[str]


class StockApiMxCatalogResponse(ApiModel):
    interfaces: list[MxInterfaceResponse]


class PublicStockToolResponse(ApiModel):
    tool_name: str
    name: str
    summary: str
    actions: list[str]
    providers: list[PublicStockProvider]


class StockApiPublicSettingsResponse(ApiModel):
    name: str
    summary: str
    providers: list[PublicStockProvider]
    features: list[str]
    tools: list[PublicStockToolResponse]


class StockApiSettingsResponse(ApiModel):
    mx: StockApiMxCatalogResponse
    public_stock: StockApiPublicSettingsResponse


class StockApiCallLogResponse(ApiModel):
    id: int
    tool_source: StockApiToolSource
    tool_id: str
    tool_name: str
    parameters: object
    status: str
    duration_ms: int
    response_characters: int | None
    error_category: StockApiErrorCategory | None = None
    error_message: str | None = None
    created_at: datetime


class StockApiCallLogSummaryResponse(ApiModel):
    total_calls: int
    success_calls: int
    failed_calls: int
    average_duration_ms: int


class StockApiCallLogPageResponse(ApiModel):
    items: list[StockApiCallLogResponse]
    total: int
    summary: StockApiCallLogSummaryResponse


class SelectedModelResponse(ApiModel):
    selected_model_id: int
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
    thinking_efforts: list[ThinkingEffort] = Field(default_factory=list)
    sort_order: int
    created_at: datetime
    updated_at: datetime


class OpenAICompatibilityFields(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    max_tokens_field: OpenAIMaxTokensField = OpenAIMaxTokensField.AUTO
    supports_temperature: bool | None = None
    supports_top_p: bool | None = None
    supports_stream_usage: bool | None = None
    replay_reasoning_content: bool | None = None


class ModelProviderConfigFields(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    auth_mode: ModelAuthMode = ModelAuthMode.AUTO
    openai: OpenAICompatibilityFields = Field(default_factory=OpenAICompatibilityFields)


class ModelProfileResponse(ApiModel):
    selected_models: list[SelectedModelResponse]
    profile_id: int
    name: str
    protocol: ModelProtocol
    model_name: str
    base_url: str | None = None
    provider_config: ModelProviderConfigFields
    api_key_configured: bool
    api_key_last_four: str | None = None
    enabled: bool
    sort_order: int
    revision: int
    created_at: datetime
    updated_at: datetime


class ModelCatalogItemResponse(ApiModel):
    model: str
    label: str
    provider_id: str | None = None


class ModelsDevModelResponse(ApiModel):
    model_name: str
    label: str
    provider_id: str
    context_window_tokens: int
    max_output_tokens: int
    thinking_efforts: list[ThinkingEffort] = Field(default_factory=list)
    input_price_per_million: float | None = None
    output_price_per_million: float | None = None
    cache_read_price_per_million: float | None = None
    cache_write_price_per_million: float | None = None


class AniuAgentPromptRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_: str = Field(default=PROMPT_PROFILE_SCHEMA, alias="schema")
    name: str | None = None
    description: str | None = None
    global_prompt: str | None = None
    run_prompt: str | None = None
    summary_prompt: str | None = None
    dream_prompt: str | None = None


class StageSettingsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    stage_id: str = Field(min_length=1)
    model_selected_model_id: int | None = Field(default=None, gt=0)
    temperature: float = Field(default=0, ge=0, le=2)
    top_p: float = Field(default=1, ge=0, le=1)
    thinking_effort: ThinkingEffort | None = None
    prompt: str = Field(min_length=1)


class UpdateSettingsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_revision: int = Field(ge=0)
    mx_api_key: str | None = Field(default=None, min_length=1)
    prompt_profile: AniuAgentPromptRequest | None = None
    stage_settings: list[StageSettingsRequest] | None = None
    dream_schedule_time: str | None = Field(
        default=None, pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$"
    )


class FetchModelCatalogRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    llm_protocol: ModelProtocol
    llm_base_url: str = Field(min_length=1)
    llm_api_key: str | None = None
    provider_config: ModelProviderConfigFields = Field(
        default_factory=ModelProviderConfigFields
    )


class SelectedModelRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    model_name: str = Field(min_length=1)
    label: str = Field(min_length=1)
    provider_id: str | None = None
    context_window_tokens: int | None = Field(default=None, ge=1000, le=10_000_000)
    max_output_tokens: int | None = Field(default=None, ge=1, le=2_000_000)
    input_price_per_million: float | None = Field(default=None, ge=0)
    output_price_per_million: float | None = Field(default=None, ge=0)
    cache_read_price_per_million: float | None = Field(default=None, ge=0)
    cache_write_price_per_million: float | None = Field(default=None, ge=0)
    thinking_efforts: list[ThinkingEffort] = Field(default_factory=list)
    sort_order: int = Field(default=0, ge=0)


class ModelsDevLookupRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    model_name: str = Field(min_length=1)


class SaveModelChannelFields(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: str = Field(min_length=1)
    protocol: ModelProtocol
    model_name: str = Field(min_length=1)
    base_url: str | None = None
    api_key: str | None = None
    provider_config: ModelProviderConfigFields = Field(
        default_factory=ModelProviderConfigFields
    )
    enabled: bool = True
    sort_order: int = Field(default=0, ge=0)
    selected_models: list[SelectedModelRequest] = Field(default_factory=list)


class CreateModelChannelWithModelsRequest(SaveModelChannelFields):
    """Atomic create of channel and selected models."""


class UpdateModelChannelWithModelsRequest(SaveModelChannelFields):
    """Atomic update of channel and selected models; revision required."""

    expected_revision: int = Field(gt=0)
