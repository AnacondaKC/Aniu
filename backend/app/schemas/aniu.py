from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


def _mask_key(value: str | None) -> str | None:
    if not value:
        return value
    if len(value) <= 8:
        return "****" + value[-2:] if len(value) > 2 else "****"
    return value[:3] + "****" + value[-4:]


class AppSettingsBase(BaseModel):
    provider_name: str = "openai-compatible"
    mx_api_key: str | None = Field(default=None, max_length=512)
    llm_base_url: str | None = Field(default=None, max_length=512)
    llm_api_key: str | None = Field(default=None, max_length=512)
    llm_model: str = Field(default="gpt-4o-mini", max_length=128)
    system_prompt: str = Field(max_length=20000)


class AppSettingsRead(AppSettingsBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def mask_sensitive_fields(self) -> "AppSettingsRead":
        self.mx_api_key = _mask_key(self.mx_api_key)
        self.llm_api_key = _mask_key(self.llm_api_key)
        return self


class AppSettingsUpdate(AppSettingsBase):
    pass


class ScheduleBase(BaseModel):
    name: str = Field(default="默认任务", max_length=64)
    run_type: Literal["analysis", "trade"] = "analysis"
    cron_expression: str = Field(default="*/30 * * * *", min_length=5, max_length=64)
    task_prompt: str = Field(
        default="请根据当前市场和持仓情况生成交易决策。", max_length=20000
    )
    timeout_seconds: int = Field(default=1800, ge=5, le=3600)
    enabled: bool = False


class ScheduleRead(ScheduleBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    retry_count: int = 0
    last_run_at: datetime | None = None
    next_run_at: datetime | None = None
    retry_after_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class ScheduleUpdate(ScheduleBase):
    id: int | None = None


class TradeOrderRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    symbol: str
    action: str
    quantity: int
    price_type: str
    price: float | None = None
    status: str
    response_payload: dict[str, Any] | None = None
    created_at: datetime


class ApiDetailRead(BaseModel):
    name: str
    summary: str


class TradeDetailRead(BaseModel):
    action: Literal["buy", "sell"]
    action_text: str
    symbol: str
    name: str
    volume: int
    price: float | None = None
    amount: float | None = None
    summary: str


class RunSummaryRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    trigger_source: str
    run_type: str
    schedule_name: str | None = None
    status: str
    analysis_summary: str | None = None
    error_message: str | None = None
    api_call_count: int = 0
    executed_trade_count: int = 0
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    started_at: datetime
    finished_at: datetime | None = None


class RunDetailRead(RunSummaryRead):
    model_config = ConfigDict(from_attributes=True)

    final_answer: str | None = None
    output_markdown: str | None = None
    api_details: list[ApiDetailRead] = Field(default_factory=list)
    trade_details: list[TradeDetailRead] = Field(default_factory=list)
    decision_payload: dict[str, Any] | None = None
    executed_actions: list[dict[str, Any]] | None = None
    llm_request_payload: dict[str, Any] | None = None
    llm_response_payload: dict[str, Any] | None = None
    skill_payloads: dict[str, Any] | None = None
    trade_orders: list[TradeOrderRead] = Field(default_factory=list)


class RunSummaryPageRead(BaseModel):
    items: list[RunSummaryRead] = Field(default_factory=list)
    next_before_id: int | None = None
    has_more: bool = False


class RuntimeSummarySectionRead(BaseModel):
    analysis_count: int = 0
    api_calls: int = 0
    trades: int = 0
    success_rate: float = 0.0
    input_tokens: str = "--"
    output_tokens: str = "--"
    total_tokens: str = "--"


class RuntimeLastRunRead(BaseModel):
    start_time: str = "--"
    end_time: str = "--"
    status: str = "idle"
    status_text: str = "暂无记录"
    duration: str = "--"
    input_tokens: str = "--"
    output_tokens: str = "--"
    total_tokens: str = "--"


class RuntimeOverviewRead(BaseModel):
    last_run: RuntimeLastRunRead = Field(default_factory=RuntimeLastRunRead)
    today: RuntimeSummarySectionRead = Field(default_factory=RuntimeSummarySectionRead)
    recent_3_days: RuntimeSummarySectionRead = Field(default_factory=RuntimeSummarySectionRead)
    recent_7_days: RuntimeSummarySectionRead = Field(default_factory=RuntimeSummarySectionRead)


class PositionOverviewRead(BaseModel):
    name: str
    symbol: str
    amount: float
    volume: int | None = None
    available_volume: int | None = None
    day_profit: float | None = None
    day_profit_ratio: float | None = None
    profit: float | None = None
    profit_ratio: float | None = None
    profit_text: str
    current_price: float | None = None
    cost_price: float | None = None
    position_ratio: float | None = None


class OrderOverviewRead(BaseModel):
    order_id: str
    order_time: str | None = None
    name: str
    symbol: str
    side: str
    side_text: str
    status: str
    status_text: str
    order_price: float | None = None
    order_quantity: int | None = None
    filled_price: float | None = None
    filled_quantity: int | None = None


class TradeSummaryRead(BaseModel):
    name: str
    symbol: str
    volume: int
    buy_amount: float
    sell_amount: float
    buy_price: float | None = None
    sell_price: float | None = None
    profit: float
    profit_ratio: float | None = None
    opened_at: str | None = None
    closed_at: str | None = None


class AccountOverviewRead(BaseModel):
    open_date: str | None = None
    daily_profit_trade_date: str | None = None
    operating_days: int | None = None
    initial_capital: float | None = None
    total_assets: float | None = None
    total_market_value: float | None = None
    cash_balance: float | None = None
    total_position_ratio: float | None = None
    holding_profit: float | None = None
    total_return_ratio: float | None = None
    nav: float | None = None
    daily_profit: float | None = None
    daily_return_ratio: float | None = None
    positions: list[PositionOverviewRead] = Field(default_factory=list)
    orders: list[OrderOverviewRead] = Field(default_factory=list)
    trade_summaries: list[TradeSummaryRead] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class AccountOverviewDebugRead(AccountOverviewRead):
    raw_balance: dict[str, Any] | None = None
    raw_positions: dict[str, Any] | None = None
    raw_orders: dict[str, Any] | None = None


class ChatMessage(BaseModel):
    role: str = Field(pattern="^(system|user|assistant)$")
    content: str = Field(min_length=1, max_length=50000)


class ChatRequest(BaseModel):
    messages: list[ChatMessage] = Field(default_factory=list, min_length=1)
    include_system_prompt: bool = True
    include_account_summary: bool = True
    include_positions_orders: bool = False
    include_latest_run_summary: bool = False


class ChatResponse(BaseModel):
    message: ChatMessage
    context: dict[str, bool]


class LoginRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


class LoginResponse(BaseModel):
    authenticated: bool
    username: str
    token: str | None = None
