"""SQLAlchemy ORM models for local persistence (SQLite by default)."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utc_now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""

    return datetime.now(tz=UTC).isoformat()


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class AuditLogModel(Base):
    """Append-only security and administrative action record."""

    __tablename__ = "audit_logs"
    __table_args__ = (
        Index("idx_audit_logs_created", "created_at"),
        Index("idx_audit_logs_actor", "actor_name", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    method: Mapped[str] = mapped_column(String(16), nullable=False)
    path: Mapped[str] = mapped_column(Text, nullable=False)
    resource_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    actor_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    request_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_ip: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status_code: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)


class SecretStoreModel(Base):
    """Unified encrypted secret value store."""

    __tablename__ = "secret_store"
    __table_args__ = (
        UniqueConstraint(
            "namespace",
            "owner_id",
            "secret_name",
            name="uq_secret_store_ref",
        ),
        Index("idx_secret_store_owner", "namespace", "owner_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    namespace: Mapped[str] = mapped_column(String(64), nullable=False)
    owner_id: Mapped[str] = mapped_column(String(128), nullable=False)
    secret_name: Mapped[str] = mapped_column(String(128), nullable=False)
    encrypted_value: Mapped[str] = mapped_column(Text, nullable=False)
    key_version: Mapped[str] = mapped_column(
        String(32), nullable=False, default="local-v1", server_default="local-v1"
    )
    created_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)
    updated_at: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default=utc_now_iso,
        onupdate=utc_now_iso,
    )


class AppSettingsModel(Base):
    """Single-row application settings."""

    __tablename__ = "app_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    prompt_profile_json: Mapped[dict[str, object]] = mapped_column(
        JSON,
        nullable=False,
        default=dict,
        server_default="{}",
    )
    # One JSON object keyed by stable pipeline stage name. Model endpoints and
    # secrets stay in their normalized channel / secret tables.
    stage_settings_json: Mapped[dict[str, object]] = mapped_column(
        JSON,
        nullable=False,
        default=dict,
        server_default="{}",
    )
    dream_schedule_time: Mapped[str] = mapped_column(
        String(5), nullable=False, default="00:30", server_default="00:30"
    )
    revision: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    created_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)
    updated_at: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default=utc_now_iso,
        onupdate=utc_now_iso,
    )

    __mapper_args__ = {"version_id_col": revision}


class ModelProfileModel(Base):
    """Saved model connection profile."""

    __tablename__ = "model_profiles"
    __table_args__ = (Index("idx_model_profiles_sort_order", "sort_order"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    protocol: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="openai_chat_completions",
    )
    model_name: Mapped[str] = mapped_column(Text, nullable=False)
    base_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    provider_config_json: Mapped[dict[str, object]] = mapped_column(
        JSON, nullable=False, default=dict
    )
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    revision: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    created_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)
    updated_at: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default=utc_now_iso,
        onupdate=utc_now_iso,
    )

    __mapper_args__ = {"version_id_col": revision}


class SelectedModelModel(Base):
    """Persisted selected model from one channel."""

    __tablename__ = "channel_selected_models"
    __table_args__ = (
        Index("idx_channel_selected_models_channel", "channel_profile_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel_profile_id: Mapped[int] = mapped_column(Integer, nullable=False)
    model_name: Mapped[str] = mapped_column(Text, nullable=False)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    provider_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    context_window_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    input_price_per_million: Mapped[float | None] = mapped_column(Float, nullable=True)
    output_price_per_million: Mapped[float | None] = mapped_column(Float, nullable=True)
    cache_read_price_per_million: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )
    cache_write_price_per_million: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )
    thinking_efforts_json: Mapped[list[str]] = mapped_column(
        JSON,
        nullable=False,
        default=list,
        server_default="[]",
    )
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)
    updated_at: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default=utc_now_iso,
        onupdate=utc_now_iso,
    )


class AccountCacheStateModel(Base):
    """Single-row persisted account cache state and latest overview."""

    __tablename__ = "account_cache_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    total_asset: Mapped[float | None] = mapped_column(Float, nullable=True)
    available_cash: Mapped[float | None] = mapped_column(Float, nullable=True)
    frozen_cash: Mapped[float | None] = mapped_column(Float, nullable=True)
    market_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_profit: Mapped[float | None] = mapped_column(Float, nullable=True)
    daily_profit: Mapped[float | None] = mapped_column(Float, nullable=True)
    captured_at: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_refresh_attempt_at: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_refresh_succeeded_at: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_refresh_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class AccountProfileCacheModel(Base):
    """Single-row persisted account profile metrics from MX."""

    __tablename__ = "account_profile_cache"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    operation_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    open_date: Mapped[str | None] = mapped_column(Text, nullable=True)
    initial_capital: Mapped[float | None] = mapped_column(Float, nullable=True)
    net_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    position_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)


class AccountPositionCacheModel(Base):
    """Current cached positions snapshot."""

    __tablename__ = "account_positions_cache"
    __table_args__ = (
        UniqueConstraint("symbol", name="uq_account_positions_cache_symbol"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(6), nullable=False)
    stock_name: Mapped[str] = mapped_column(Text, nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    avg_cost: Mapped[float] = mapped_column(Float, nullable=False)
    current_price: Mapped[float] = mapped_column(Float, nullable=False)
    market_value: Mapped[float] = mapped_column(Float, nullable=False)
    profit_ratio: Mapped[float] = mapped_column(Float, nullable=False)
    day_profit: Mapped[float | None] = mapped_column(Float, nullable=True)
    available_quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    captured_at: Mapped[str] = mapped_column(Text, nullable=False)


class AccountOrderCacheModel(Base):
    """Current cached entrust/order snapshot."""

    __tablename__ = "account_orders_cache"
    __table_args__ = (
        UniqueConstraint("order_id", name="uq_account_orders_cache_order_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(6), nullable=False)
    stock_name: Mapped[str] = mapped_column(Text, nullable=False)
    direction: Mapped[str] = mapped_column(String(16), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    order_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    filled_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    filled_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    submitted_at: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[str | None] = mapped_column(Text, nullable=True)


class StrategyRunModel(Base):
    """Strategy run header row."""

    __tablename__ = "strategy_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trigger_source: Mapped[str] = mapped_column(String(32), nullable=False)
    schedule_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="RUNNING")
    current_state: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="Run",
    )
    snapshot_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    trace_json: Mapped[dict[str, object]] = mapped_column(
        JSON, nullable=False, default=dict
    )
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary_render_mode: Mapped[str] = mapped_column(
        String(16), nullable=False, default="markdown", server_default="markdown"
    )
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Denormalized list projection — kept in sync on every run save so list
    # endpoints never parse the full trace_json payload.
    tool_calls_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    thinking_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    total_tokens: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    trade_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    started_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)
    completed_at: Mapped[str | None] = mapped_column(Text, nullable=True)


class MemoryItemModel(Base):
    """Current projection of one task-sourced memory."""

    __tablename__ = "memory_items"
    __table_args__ = (Index("idx_memory_items_updated", "updated_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_task_id: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_task_id: Mapped[int] = mapped_column(Integer, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)
    updated_at: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default=utc_now_iso,
        onupdate=utc_now_iso,
    )
    deleted_at: Mapped[str | None] = mapped_column(Text, nullable=True)


class MemoryActivityModel(Base):
    """One read or write event for the memory workbench."""

    __tablename__ = "memory_activities"
    __table_args__ = (Index("idx_memory_activities_created", "created_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    operation: Mapped[str] = mapped_column(String(16), nullable=False)
    memory_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    content: Mapped[str] = mapped_column(Text, nullable=False, default="")
    result_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    task_id: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)


class MemoryDreamModel(Base):
    """One idempotent nightly memory-maintenance task."""

    __tablename__ = "memory_dreams"
    __table_args__ = (
        UniqueConstraint("target_date", name="uq_memory_dreams_target_date"),
        Index("idx_memory_dreams_status", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    target_date: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    result: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)
    started_at: Mapped[str | None] = mapped_column(Text, nullable=True)
    completed_at: Mapped[str | None] = mapped_column(Text, nullable=True)


class StrategyScheduleModel(Base):
    """Persisted schedule row."""

    __tablename__ = "strategy_schedules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    interval_minutes: Mapped[int] = mapped_column(
        Integer, nullable=False, default=15, server_default="15"
    )
    custom_schedule_times_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    revision: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    runtime_synced_revision: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    sync_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default=utc_now_iso,
        onupdate=utc_now_iso,
    )


class TaskLeaseModel(Base):
    """Short-lived ownership lease shared by schedulers and workers."""

    __tablename__ = "task_leases"

    lease_key: Mapped[str] = mapped_column(String(160), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(128), nullable=False)
    lease_expires_at: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)


class RunJobModel(Base):
    """Durable execution job for one strategy run (SQLite lease worker)."""

    __tablename__ = "run_jobs"
    __table_args__ = (
        UniqueConstraint("active_guard", name="uq_run_jobs_active_guard"),
        Index("idx_run_jobs_status_available", "status", "available_at"),
        Index("idx_run_jobs_lease", "status", "lease_expires_at"),
    )

    run_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("strategy_runs.id", ondelete="CASCADE"),
        primary_key=True,
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="PENDING")
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    worker_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    claim_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lease_expires_at: Mapped[str | None] = mapped_column(Text, nullable=True)
    heartbeat_at: Mapped[str | None] = mapped_column(Text, nullable=True)
    available_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)
    last_error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_error_message_preview: Mapped[str | None] = mapped_column(Text, nullable=True)
    cancel_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    # 1 while job is active (PENDING/LEASED/CANCEL_REQUESTED); NULL when terminal.
    active_guard: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)
    updated_at: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default=utc_now_iso,
        onupdate=utc_now_iso,
    )


class ToolInvocationModel(Base):
    """At-most-once execution record for external write tools."""

    __tablename__ = "tool_invocations"

    run_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("strategy_runs.id", ondelete="CASCADE"),
        primary_key=True,
    )
    tool_call_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tool_name: Mapped[str] = mapped_column(String(128), nullable=False)
    arguments_json: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    result_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)
    updated_at: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default=utc_now_iso,
        onupdate=utc_now_iso,
    )


class StockApiCallLogModel(Base):
    """One persisted user-visible data-interface tool invocation."""

    __tablename__ = "stock_api_call_logs"
    __table_args__ = (
        Index("idx_stock_api_call_logs_created", "created_at"),
        Index(
            "idx_stock_api_call_logs_provider_operation",
            "provider",
            "operation_id",
            "created_at",
        ),
        Index("idx_stock_api_call_logs_status", "status", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="unknown",
        server_default="unknown",
    )
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    operation_id: Mapped[str] = mapped_column(String(128), nullable=False)
    parameters_json: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    duration_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    response_characters: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_category: Mapped[str | None] = mapped_column(String(32), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)


class LocalIdentityModel(Base):
    """Singleton local identity (always primary key 1)."""

    __tablename__ = "local_identity"
    __table_args__ = (CheckConstraint("id = 1", name="ck_local_identity_singleton"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)
    updated_at: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default=utc_now_iso,
        onupdate=utc_now_iso,
    )


class AuthSessionModel(Base):
    """Server-side session row for the singleton local account."""

    __tablename__ = "auth_sessions"
    __table_args__ = (
        UniqueConstraint("token_hash", name="uq_auth_sessions_token_hash"),
        Index("idx_auth_sessions_expires", "expires_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    token_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    csrf_token_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    credential_fingerprint: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        default="",
        server_default="",
    )
    expires_at: Mapped[str] = mapped_column(Text, nullable=False)
    last_seen_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)
    revoked_at: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)
