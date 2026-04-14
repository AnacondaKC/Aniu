from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import JSON

from app.core.constants import DEFAULT_SYSTEM_PROMPT


class Base(DeclarativeBase):
    pass


class AppSettings(Base):
    __tablename__ = "app_settings"

    id: Mapped[int] = mapped_column(primary_key=True)
    provider_name: Mapped[str] = mapped_column(String(32), default="openai-compatible")
    mx_api_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    llm_base_url: Mapped[str | None] = mapped_column(String(255), nullable=True)
    llm_api_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    llm_model: Mapped[str] = mapped_column(String(128), default="gpt-4o-mini")
    system_prompt: Mapped[str] = mapped_column(
        Text,
        default=DEFAULT_SYSTEM_PROMPT,
    )
    analyst_prompt: Mapped[str] = mapped_column(
        Text,
        default=(
            "请结合市场数据、资讯、候选股票、持仓和资金情况做判断。"
            "当信号不明确时返回HOLD。"
        ),
    )
    market_query: Mapped[str] = mapped_column(
        String(255), default="上证指数今天走势和市场概况"
    )
    news_query: Mapped[str] = mapped_column(String(255), default="今天A股市场热点新闻")
    screener_query: Mapped[str] = mapped_column(
        String(255), default="A股今天值得关注的强势股"
    )
    max_actions: Mapped[int] = mapped_column(Integer, default=2)
    trade_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )


class StrategySchedule(Base):
    __tablename__ = "strategy_schedules"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(64), default="默认调度任务")
    run_type: Mapped[str] = mapped_column(String(32), default="analysis")
    interval_minutes: Mapped[int] = mapped_column(Integer, default=30)
    cron_expression: Mapped[str | None] = mapped_column(String(64), nullable=True)
    task_prompt: Mapped[str | None] = mapped_column(Text, nullable=True)
    timeout_seconds: Mapped[int] = mapped_column(Integer, default=1800)
    enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    next_run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    retry_after_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )


class StrategyRun(Base):
    __tablename__ = "strategy_runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    trigger_source: Mapped[str] = mapped_column(String(32), default="manual")
    run_type: Mapped[str] = mapped_column(String(32), default="analysis")
    schedule_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="pending", index=True)
    analysis_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    final_answer: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    llm_request_payload: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, nullable=True
    )
    llm_response_payload: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, nullable=True
    )
    skill_payloads: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    decision_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    executed_actions: Mapped[list[dict[str, Any]] | None] = mapped_column(
        JSON, nullable=True
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), index=True
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    trade_orders: Mapped[list["TradeOrder"]] = relationship(
        back_populates="run",
        cascade="all, delete-orphan",
    )


class TradeOrder(Base):
    __tablename__ = "trade_orders"

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("strategy_runs.id"), index=True)
    symbol: Mapped[str] = mapped_column(String(16), index=True)
    action: Mapped[str] = mapped_column(String(16))
    quantity: Mapped[int] = mapped_column(Integer)
    price_type: Mapped[str] = mapped_column(String(16), default="MARKET")
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="submitted")
    response_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    run: Mapped[StrategyRun] = relationship(back_populates="trade_orders")
