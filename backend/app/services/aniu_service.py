from __future__ import annotations

import inspect
import json
import logging
import queue
import secrets
import time
import traceback
from datetime import date, datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock, Thread
from types import SimpleNamespace
from typing import Any, Iterator
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.core.auth import create_access_token
from app.core.config import get_settings
from app.core.constants import DEFAULT_SYSTEM_PROMPT
from app.db.database import session_scope
from app.db.models import (
    AppSettings,
    ChatMessageRecord,
    ChatSession,
    StrategyRun,
    StrategySchedule,
    TradeOrder,
)
from app.schemas.aniu import AppSettingsUpdate, ChatRequest, ScheduleUpdate
from app.services.event_bus import event_bus, make_emitter
from app.services.llm_service import LLMStreamCancelled, llm_service
from app.services.mx_skill_service import mx_skill_service
from app.services.mx_service import MXClient
from app.services.token_estimator import estimate_messages_tokens, estimate_text_tokens
from app.services.trading_calendar_service import trading_calendar_service


logger = logging.getLogger(__name__)

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
ANALYSIS_TASK_NAMES = {"盘前分析", "午间复盘", "收盘分析"}
SCHEDULE_RETRY_DELAY = timedelta(minutes=5)
SCHEDULE_MAX_RETRIES = 3
ACCOUNT_PREFETCH_TOOL_NAMES = (
    "mx_get_balance",
    "mx_get_positions",
    "mx_get_orders",
)
ACCOUNT_OVERVIEW_CACHE_MAX_WORKERS = 3
AUTOMATION_SESSION_SLUG = "automation-default"
AUTOMATION_SESSION_TITLE = "自动化交易会话"
AUTOMATION_DEFAULT_CONTEXT_WINDOW_TOKENS = 65536
AUTOMATION_DEFAULT_TARGET_PROMPT_TOKENS = 24000
AUTOMATION_DEFAULT_RECENT_MESSAGE_LIMIT = 24
AUTOMATION_DEFAULT_IDLE_SUMMARY_HOURS = 12
AUTOMATION_RESERVED_OUTPUT_TOKENS = 4000
AUTOMATION_SAFETY_BUFFER_TOKENS = 2000


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_shanghai() -> datetime:
    return now_utc().astimezone(SHANGHAI_TZ)


def _assume_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_percent(value: float | None) -> float | None:
    if value is None:
        return None
    return value / 100.0


def _scaled_decimal(value: Any, decimal_places: Any) -> float | None:
    numeric = _parse_float(value)
    if numeric is None:
        return None

    decimals = _parse_float(decimal_places)
    scale = int(decimals) if decimals is not None else 0
    if scale <= 0:
        return numeric
    return numeric / (10**scale)


def _market_suffix(value: Any) -> str:
    mapping = {
        0: "SZ",
        1: "SH",
    }
    numeric = _parse_float(value)
    if numeric is None:
        return ""
    return mapping.get(int(numeric), "")


def _format_open_date(value: Any) -> str | None:
    text = str(value or "").strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text or None


def _format_timestamp(value: Any) -> str | None:
    numeric = _parse_float(value)
    if numeric is None:
        return None
    if numeric > 10_000_000_000:
        numeric = numeric / 1000
    try:
        return datetime.fromtimestamp(numeric, tz=SHANGHAI_TZ).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except (OverflowError, OSError, ValueError):
        return None


def _coalesce(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _order_status_text(
    value: Any,
    *,
    filled_quantity: Any = None,
    order_quantity: Any = None,
    db_status: Any = None,
) -> str:
    filled = int(_parse_float(filled_quantity) or 0)
    total = int(_parse_float(order_quantity) or 0)
    if total > 0:
        if filled >= total and filled > 0:
            return "已成交"
        if 0 < filled < total:
            return "部分成交"

    mapping = {
        "0": "未知",
        "1": "已报",
        "2": "已报",
        "3": "已撤单",
        "4": "已成交",
        "8": "未成交",
        "9": "已撤单",
        "100": "处理中",
        "200": "已完成",
        "206": "已撤单",
    }
    text = str(value or "").strip()
    if text == "" and db_status is not None:
        text = str(db_status).strip()
    return mapping.get(text, text or "未知")


class AniuService:
    def __init__(self) -> None:
        self._run_lock = Lock()
        self._account_cache_lock = Lock()
        self._account_overview_cache: dict[str, Any] | None = None
        self._account_overview_cache_expires_at: datetime | None = None

    def _resolve_run_type(self, schedule: StrategySchedule | None) -> str:
        if schedule is None:
            return "analysis"

        run_type = str(schedule.run_type or "").strip()
        if run_type in {"analysis", "trade"}:
            return run_type

        name = str(schedule.name or "").strip()
        if name.startswith("上午运行") or name.startswith("下午运行"):
            return "trade"
        return "analysis"

    def _run_agent_supports_emit(self, run_agent: Any) -> bool:
        try:
            signature = inspect.signature(run_agent)
        except (TypeError, ValueError):
            return True

        for parameter in signature.parameters.values():
            if parameter.kind is inspect.Parameter.VAR_KEYWORD:
                return True
            if parameter.name == "emit":
                return True
        return False

    def _infer_run_type(self, run: StrategyRun) -> str:
        schedule_name = str(run.schedule_name or "").strip()
        if schedule_name in ANALYSIS_TASK_NAMES:
            return "analysis"
        if schedule_name.startswith("上午运行") or schedule_name.startswith("下午运行"):
            return "trade"

        if run.trade_orders:
            return "trade"

        executed_actions = run.executed_actions if isinstance(run.executed_actions, list) else []
        trade_actions = {"BUY", "SELL", "CANCEL"}
        if any(str(item.get("action") or "").upper() in trade_actions for item in executed_actions if isinstance(item, dict)):
            return "trade"

        tool_calls = self._get_run_tool_calls(run)
        trade_tool_names = {"mx_moni_trade", "mx_moni_cancel"}
        if any(str(item.get("name") or "") in trade_tool_names for item in tool_calls):
            return "trade"

        stored_run_type = str(run.run_type or "").strip()
        if stored_run_type in {"trade", "analysis"}:
            return stored_run_type

        return "analysis"

    def authenticate_login(self, password: str) -> dict[str, Any]:
        settings = get_settings()
        expected_password = settings.app_login_password

        if not expected_password:
            raise RuntimeError("未配置登录密码，请先设置 APP_LOGIN_PASSWORD。")

        if not secrets.compare_digest(password, expected_password):
            raise RuntimeError("密码错误。")

        token = create_access_token("single-user")
        return {
            "authenticated": True,
            "token": token,
        }

    def get_or_create_settings(self, db: Session) -> AppSettings:
        instance = db.scalar(select(AppSettings).limit(1))
        if instance is None:
            env = get_settings()
            instance = AppSettings(
                provider_name="openai-compatible",
                mx_api_key=env.mx_apikey,
                llm_base_url=env.openai_base_url,
                llm_api_key=env.openai_api_key,
                llm_model=env.openai_model,
                system_prompt=DEFAULT_SYSTEM_PROMPT,
            )
            db.add(instance)
            db.commit()
            db.refresh(instance)
        return instance

    def list_schedules(self, db: Session) -> list[StrategySchedule]:
        stmt = select(StrategySchedule).order_by(StrategySchedule.id.asc())
        schedules = list(db.scalars(stmt).all())
        mutated = False
        for schedule in schedules:
            if not schedule.name:
                schedule.name = "默认任务"
                mutated = True
            if str(schedule.run_type or "").strip() not in {"analysis", "trade"}:
                schedule.run_type = self._resolve_run_type(schedule)
                mutated = True
            if not schedule.cron_expression:
                schedule.cron_expression = "*/30 * * * *"
                mutated = True
            if not schedule.task_prompt:
                schedule.task_prompt = "请根据当前市场和持仓情况生成交易决策。"
                mutated = True
            if not schedule.timeout_seconds or schedule.timeout_seconds <= 0:
                schedule.timeout_seconds = 1800
                mutated = True
            if schedule.retry_count < 0:
                schedule.retry_count = 0
                mutated = True
            if schedule.enabled and schedule.next_run_at is None:
                schedule.next_run_at = self._compute_next_run_at(
                    schedule.cron_expression
                )
                mutated = True
        if mutated:
            db.commit()
            for schedule in schedules:
                db.refresh(schedule)
        if not schedules:
            instance = StrategySchedule(
                name="默认任务",
                run_type="analysis",
                cron_expression="*/30 * * * *",
                task_prompt="请根据当前市场和持仓情况生成交易决策。",
                timeout_seconds=1800,
                enabled=False,
            )
            db.add(instance)
            db.commit()
            db.refresh(instance)
            schedules = [instance]
        for schedule in schedules:
            schedule.retry_count = max(int(schedule.retry_count or 0), 0)
            schedule.last_run_at = _assume_utc(schedule.last_run_at)
            schedule.next_run_at = _assume_utc(schedule.next_run_at)
            schedule.retry_after_at = _assume_utc(schedule.retry_after_at)
            schedule.created_at = _assume_utc(schedule.created_at)
            schedule.updated_at = _assume_utc(schedule.updated_at)
        return schedules

    def update_settings(self, db: Session, payload: AppSettingsUpdate) -> AppSettings:
        instance = self.get_or_create_settings(db)
        sensitive_fields = {"mx_api_key", "llm_api_key"}
        changed_fields: list[str] = []
        for field, value in payload.model_dump().items():
            if field in sensitive_fields:
                if isinstance(value, str) and "****" in value:
                    continue
            old_value = getattr(instance, field, None)
            if old_value != value:
                changed_fields.append(field)
            setattr(instance, field, value)
        db.add(instance)
        db.commit()
        db.refresh(instance)
        instance.created_at = _assume_utc(instance.created_at)
        instance.updated_at = _assume_utc(instance.updated_at)
        logger.info("settings updated: changed_fields=%s", changed_fields)
        return instance

    def replace_schedules(
        self, db: Session, payloads: list[ScheduleUpdate]
    ) -> list[StrategySchedule]:
        existing = {item.id: item for item in self.list_schedules(db)}
        keep_ids: set[int] = set()

        for payload in payloads:
            data = payload.model_dump()
            schedule_id = data.pop("id", None)
            if schedule_id is not None and schedule_id in existing:
                instance = existing[schedule_id]
            else:
                instance = StrategySchedule()
                db.add(instance)
                db.flush()

            for field, value in data.items():
                setattr(instance, field, value)

            instance.next_run_at = self._compute_next_run_at(instance.cron_expression)
            db.add(instance)
            db.flush()
            keep_ids.add(instance.id)

        for schedule_id, instance in existing.items():
            if schedule_id not in keep_ids:
                db.delete(instance)

        db.commit()
        logger.info(
            "schedules replaced: kept=%s, deleted=%s",
            keep_ids,
            set(existing.keys()) - keep_ids,
        )
        return self.list_schedules(db)

    def list_runs(
        self,
        db: Session,
        limit: int = 20,
        run_date: date | None = None,
        status: str | None = None,
        before_id: int | None = None,
    ) -> list[StrategyRun]:
        stmt = select(StrategyRun)

        if run_date is not None:
            start_of_day = datetime.combine(run_date, datetime.min.time())
            end_of_day = start_of_day + timedelta(days=1)
            stmt = stmt.where(
                StrategyRun.started_at >= start_of_day,
                StrategyRun.started_at < end_of_day,
            )

        normalized_status = str(status or "").strip().lower()
        if normalized_status:
            stmt = stmt.where(StrategyRun.status == normalized_status)

        if before_id is not None:
            stmt = stmt.where(StrategyRun.id < before_id)

        stmt = stmt.order_by(StrategyRun.started_at.desc(), StrategyRun.id.desc()).limit(
            limit
        )
        runs = list(db.scalars(stmt).all())
        for run in runs:
            self._hydrate_run_datetimes(run, include_display_fields=False)
        return runs

    def list_runs_page(
        self,
        db: Session,
        limit: int = 20,
        run_date: date | None = None,
        status: str | None = None,
        before_id: int | None = None,
    ) -> dict[str, Any]:
        page_size = max(1, limit)
        runs = self.list_runs(
            db,
            limit=page_size + 1,
            run_date=run_date,
            status=status,
            before_id=before_id,
        )
        has_more = len(runs) > page_size
        items = runs[:page_size]
        next_before_id = items[-1].id if has_more and items else None
        return {
            "items": items,
            "next_before_id": next_before_id,
            "has_more": has_more,
        }

    def get_runtime_overview(self, db: Session) -> dict[str, Any]:
        runs = self.list_runs(db, limit=100)
        latest_run = runs[0] if runs else None
        return {
            "last_run": self._build_runtime_last_run(latest_run),
            "today": self._build_runtime_summary_section(
                [run for run in runs if self._is_within_days(run.started_at, 1, same_day_only=True)]
            ),
            "recent_3_days": self._build_runtime_summary_section(
                [run for run in runs if self._is_within_days(run.started_at, 3)]
            ),
            "recent_7_days": self._build_runtime_summary_section(
                [run for run in runs if self._is_within_days(run.started_at, 7)]
            ),
        }

    def get_run(self, db: Session, run_id: int) -> StrategyRun | None:
        stmt = (
            select(StrategyRun)
            .where(StrategyRun.id == run_id)
            .options(selectinload(StrategyRun.trade_orders))
        )
        run = db.scalar(stmt)
        if run is not None:
            self._hydrate_run_datetimes(run, include_display_fields=True)
        return run

    def _hydrate_run_datetimes(
        self, run: StrategyRun, *, include_display_fields: bool
    ) -> None:
        run.started_at = _assume_utc(run.started_at)
        run.finished_at = _assume_utc(run.finished_at)
        run.run_type = self._infer_run_type(run)
        self._hydrate_run_summary_metrics(run)
        if include_display_fields:
            self._hydrate_run_display_fields(run)
            for order in run.trade_orders:
                order.created_at = _assume_utc(order.created_at)

    def _hydrate_run_summary_metrics(self, run: StrategyRun) -> None:
        token_usage = self._get_run_token_usage(run)
        run.api_call_count = self._count_run_api_calls(run)
        run.executed_trade_count = self._count_executed_actions(run)
        run.input_tokens = token_usage["input"]
        run.output_tokens = token_usage["output"]
        run.total_tokens = token_usage["total"]

    def _hydrate_run_display_fields(self, run: StrategyRun) -> None:
        run.output_markdown = (
            str(run.final_answer or run.analysis_summary or run.error_message or "").strip()
            or None
        )
        run.api_details = self._build_run_api_details(run)
        run.raw_tool_previews = self._build_raw_tool_previews(run)
        run.trade_details = self._build_run_trade_details(run)

    def _format_token_count(self, value: int) -> str:
        if not isinstance(value, int) or value <= 0:
            return "--"
        if value >= 1000:
            return f"{value / 1000:.1f}k"
        return str(value)

    def _get_duration_text(
        self, started_at: datetime | None, finished_at: datetime | None
    ) -> str:
        if started_at is None or finished_at is None:
            return "进行中" if started_at is not None and finished_at is None else "--"
        start = _assume_utc(started_at)
        end = _assume_utc(finished_at)
        if start is None or end is None or end <= start:
            return "--"
        total_seconds = int((end - start).total_seconds())
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        return f"{minutes}分{seconds:02d}秒"

    def _get_runtime_status_text(self, status: str | None) -> str:
        if status == "completed":
            return "正常"
        if status == "failed":
            return "失败"
        if status == "running":
            return "进行中"
        return "暂无记录"

    def _is_within_days(
        self,
        started_at: datetime | None,
        days: int,
        *,
        same_day_only: bool = False,
    ) -> bool:
        timestamp = _assume_utc(started_at)
        if timestamp is None:
            return False
        now = now_utc()
        if same_day_only:
            local_started = timestamp.astimezone(SHANGHAI_TZ)
            local_now = now.astimezone(SHANGHAI_TZ)
            return local_started.date() == local_now.date()
        return now - timestamp <= timedelta(days=days)

    def _build_runtime_last_run(self, run: StrategyRun | None) -> dict[str, Any]:
        if run is None:
            return {
                "start_time": "--",
                "end_time": "--",
                "status": "idle",
                "status_text": "暂无记录",
                "duration": "--",
                "input_tokens": "--",
                "output_tokens": "--",
                "total_tokens": "--",
            }

        return {
            "start_time": run.started_at.isoformat() if run.started_at else "--",
            "end_time": run.finished_at.isoformat() if run.finished_at else "--",
            "status": run.status,
            "status_text": self._get_runtime_status_text(run.status),
            "duration": self._get_duration_text(run.started_at, run.finished_at),
            "input_tokens": self._format_token_count(int(run.input_tokens or 0)),
            "output_tokens": self._format_token_count(int(run.output_tokens or 0)),
            "total_tokens": self._format_token_count(int(run.total_tokens or 0)),
        }

    def _build_runtime_summary_section(
        self, runs: list[StrategyRun]
    ) -> dict[str, Any]:
        analysis_count = len(runs)
        success_count = sum(1 for run in runs if run.status == "completed")
        api_calls = sum(int(run.api_call_count or 0) for run in runs)
        trades = sum(int(run.executed_trade_count or 0) for run in runs)
        input_tokens = sum(int(run.input_tokens or 0) for run in runs)
        output_tokens = sum(int(run.output_tokens or 0) for run in runs)
        total_tokens = sum(int(run.total_tokens or 0) for run in runs)
        return {
            "analysis_count": analysis_count,
            "api_calls": api_calls,
            "trades": trades,
            "success_rate": round((success_count / analysis_count) * 100, 1)
            if analysis_count > 0
            else 0.0,
            "input_tokens": self._format_token_count(input_tokens),
            "output_tokens": self._format_token_count(output_tokens),
            "total_tokens": self._format_token_count(total_tokens),
        }

    def _get_api_tool_text(self, name: str) -> dict[str, str]:
        mapping = {
            "mx_get_positions": {"name": "获取持仓", "summary": "读取当前账户持仓与仓位分布。"},
            "mx_get_balance": {"name": "获取资产", "summary": "读取账户总资产、现金和收益情况。"},
            "mx_get_orders": {"name": "获取委托", "summary": "读取近期委托和成交记录，用于判断交易状态。"},
            "mx_get_self_selects": {"name": "获取自选", "summary": "读取当前自选股列表，辅助观察候选标的。"},
            "mx_query_market": {"name": "查询行情", "summary": "获取目标股票的实时行情和基础市场数据。"},
            "mx_search_news": {"name": "搜索资讯", "summary": "查询相关新闻或公告，辅助判断市场事件影响。"},
            "mx_screen_stocks": {"name": "筛选股票", "summary": "按条件筛选候选标的，缩小分析范围。"},
            "mx_manage_self_select": {"name": "管理自选", "summary": "增删自选股，维护后续关注列表。"},
            "mx_moni_trade": {"name": "提交模拟交易", "summary": "向模拟交易系统提交买入或卖出指令。"},
            "mx_moni_cancel": {"name": "撤销委托", "summary": "撤销尚未完成的模拟委托单。"},
        }
        return mapping.get(name, {"name": name or "未命名调用", "summary": "执行一次系统或妙想工具调用。"})

    def _build_run_api_details(self, run: StrategyRun) -> list[dict[str, Any]]:
        trade_tool_names = {"mx_moni_trade", "mx_moni_cancel"}
        results: list[dict[str, Any]] = []
        for idx, item in enumerate(self._get_detail_tool_calls(run)):
            tool_name = str(item.get("name") or "")
            if tool_name in trade_tool_names:
                continue
            tool_text = self._get_api_tool_text(tool_name)
            result = item.get("result")
            ok: bool | None = None
            status = "done"
            if isinstance(result, dict) and "ok" in result:
                ok = bool(result.get("ok"))
                status = "done" if ok else "failed"
            results.append(
                {
                    "tool_name": tool_name,
                    "name": tool_text["name"],
                    "summary": tool_text["summary"],
                    "preview_index": idx,
                    "tool_call_id": str(
                        item.get("id") or item.get("tool_call_id") or ""
                    )
                    or None,
                    "status": status,
                    "ok": ok,
                }
            )
        return results

    def _build_raw_tool_previews(self, run: StrategyRun) -> list[dict[str, str]]:
        previews: list[dict[str, str]] = []
        for idx, item in enumerate(self._get_detail_tool_calls(run)):
            tool_name = str(item.get("name") or "")
            tool_text = self._get_api_tool_text(tool_name)
            result = item.get("result")
            if not isinstance(result, dict):
                continue
            raw_payload = result.get("result")
            preview_source = raw_payload if raw_payload is not None else result
            previews.append(
                {
                    "preview_index": idx,
                    "tool_name": tool_name,
                    "display_name": tool_text["name"],
                    "summary": str(result.get("summary") or tool_text["summary"]),
                    "preview": self._format_tool_preview(preview_source),
                }
            )
        return previews

    def _format_tool_preview(self, payload: Any, max_chars: int = 6000) -> str:
        try:
            text = json.dumps(payload, ensure_ascii=False, indent=2)
        except (TypeError, ValueError):
            text = str(payload)
        text = text.strip()
        if len(text) <= max_chars:
            return text
        return text[: max_chars - 16].rstrip() + "\n...\n<已截断>"

    def _extract_trade_name(self, payload: Any) -> str:
        if not isinstance(payload, dict):
            return ""

        candidates = [
            payload.get("name"),
            payload.get("stock_name"),
            payload.get("stockName"),
            payload.get("security_name"),
            payload.get("securityName"),
        ]
        for candidate in candidates:
            value = str(candidate or "").strip()
            if value:
                return value

        result = payload.get("result")
        if result is not payload:
            return self._extract_trade_name(result)
        return ""

    def _get_trade_summary(
        self,
        action: str,
        symbol: str,
        volume: int,
    ) -> str:
        action_text = "卖出" if action == "sell" else "买入"
        display_symbol = symbol or "--"
        return f"挂单{action_text}{display_symbol}共计{volume}股。"

    def _resolve_trade_detail_status(self, raw_status: Any) -> tuple[str, bool | None]:
        text = str(raw_status or "").strip().lower()
        if text and any(flag in text for flag in ("fail", "error", "reject")):
            return "failed", False
        return "done", True

    def _build_run_trade_details(self, run: StrategyRun) -> list[dict[str, Any]]:
        tool_calls = self._get_detail_tool_calls(run)
        if run.trade_orders:
            details: list[dict[str, Any]] = []
            for order in run.trade_orders:
                action_name = str(order.action).upper()
                trade_action = "sell" if action_name == "SELL" else "buy"
                tool_name = self._match_trade_tool_name(tool_calls, order.symbol, action_name)
                detail_status, detail_ok = self._resolve_trade_detail_status(order.status)
                details.append(
                    {
                        "action": trade_action,
                        "action_text": "模拟卖出" if action_name == "SELL" else "模拟买入",
                        "symbol": order.symbol,
                        "name": self._extract_trade_name(order.response_payload) or order.symbol,
                        "volume": int(order.quantity),
                        "price": order.price,
                        "amount": round(float(order.price or 0) * int(order.quantity), 2)
                        if order.price is not None
                        else None,
                        "summary": self._get_trade_summary(trade_action, order.symbol, int(order.quantity)),
                        "tool_name": tool_name,
                        "preview_index": self._find_tool_call_index(tool_calls, tool_name, order.symbol),
                        "status": detail_status,
                        "ok": detail_ok,
                    }
                )
            return details

        executed_actions = run.executed_actions if isinstance(run.executed_actions, list) else []
        details: list[dict[str, Any]] = []
        for action in executed_actions:
            if not isinstance(action, dict):
                continue
            action_name = str(action.get("action") or "").upper()
            if action_name not in {"BUY", "SELL"}:
                continue
            trade_action = "sell" if action_name == "SELL" else "buy"
            price = _parse_float(action.get("price"))
            volume = int(action.get("quantity") or 0)
            symbol = str(action.get("symbol") or "--")
            tool_name = self._match_trade_tool_name(tool_calls, symbol, action_name)
            detail_status, detail_ok = self._resolve_trade_detail_status(
                action.get("status")
            )
            details.append(
                {
                    "action": trade_action,
                    "action_text": "模拟卖出" if action_name == "SELL" else "模拟买入",
                    "symbol": symbol,
                    "name": str(action.get("name") or "").strip() or symbol,
                    "volume": volume,
                    "price": price,
                    "amount": round((price or 0) * volume, 2) if price is not None else None,
                    "summary": self._get_trade_summary(trade_action, symbol, volume),
                    "tool_name": tool_name,
                    "preview_index": self._find_tool_call_index(tool_calls, tool_name, symbol),
                    "status": detail_status,
                    "ok": detail_ok,
                }
            )
        return details

    def _match_trade_tool_name(
        self, tool_calls: list[dict[str, Any]], symbol: str, action_name: str
    ) -> str | None:
        desired_name = "mx_moni_trade"
        target_symbol = str(symbol or "").strip()
        target_action = str(action_name or "").upper()
        for item in reversed(tool_calls):
            tool_name = str(item.get("name") or "")
            if tool_name != desired_name:
                continue
            result = item.get("result")
            if not isinstance(result, dict):
                continue
            executed_action = result.get("executed_action")
            if not isinstance(executed_action, dict):
                continue
            if str(executed_action.get("symbol") or "").strip() != target_symbol:
                continue
            if str(executed_action.get("action") or "").upper() != target_action:
                continue
            return tool_name
        return None

    def _find_tool_call_index(
        self,
        tool_calls: list[dict[str, Any]],
        tool_name: str | None,
        symbol: str | None = None,
    ) -> int | None:
        if not tool_name:
            return None
        target_symbol = str(symbol or "").strip()
        for idx in range(len(tool_calls) - 1, -1, -1):
            item = tool_calls[idx]
            if str(item.get("name") or "") != tool_name:
                continue
            if not target_symbol:
                return idx
            result = item.get("result")
            if not isinstance(result, dict):
                continue
            executed_action = result.get("executed_action")
            if isinstance(executed_action, dict) and str(executed_action.get("symbol") or "").strip() == target_symbol:
                return idx
        return None

    def _get_run_token_usage(self, run: StrategyRun) -> dict[str, int | None]:
        response_usage = self._extract_usage(run.llm_response_payload)
        request_usage = self._extract_usage(run.llm_request_payload)

        prompt_tokens = self._coerce_token_value(
            _coalesce(
                response_usage.get("prompt_tokens") if response_usage is not None else None,
                request_usage.get("prompt_tokens") if request_usage is not None else None,
            )
        )
        completion_tokens = self._coerce_token_value(
            _coalesce(
                response_usage.get("completion_tokens")
                if response_usage is not None
                else None,
                request_usage.get("completion_tokens")
                if request_usage is not None
                else None,
            )
        )
        total_tokens = self._coerce_token_value(
            _coalesce(
                response_usage.get("total_tokens") if response_usage is not None else None,
                request_usage.get("total_tokens") if request_usage is not None else None,
            )
        )

        if total_tokens is None and (
            prompt_tokens is not None or completion_tokens is not None
        ):
            total_tokens = (prompt_tokens or 0) + (completion_tokens or 0)

        return {
            "input": prompt_tokens,
            "output": completion_tokens,
            "total": total_tokens,
        }

    def _extract_usage(self, payload: Any) -> dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None

        direct_usage = payload.get("usage")
        if isinstance(direct_usage, dict):
            return direct_usage

        responses = payload.get("responses")
        if not isinstance(responses, list):
            return None

        for item in reversed(responses):
            if not isinstance(item, dict):
                continue
            usage = item.get("usage")
            if isinstance(usage, dict):
                return usage
        return None

    def _coerce_token_value(self, value: Any) -> int | None:
        numeric = _parse_float(value)
        if numeric is None or numeric <= 0:
            return None
        return int(numeric)

    def _count_run_api_calls(self, run: StrategyRun) -> int:
        trade_tool_names = {"mx_moni_trade", "mx_moni_cancel"}
        return sum(
            1
            for item in self._get_detail_tool_calls(run)
            if str(item.get("name") or "") not in trade_tool_names
        )

    def _count_executed_actions(self, run: StrategyRun) -> int:
        executed_actions = (
            run.executed_actions if isinstance(run.executed_actions, list) else []
        )
        trade_actions = {"BUY", "SELL"}
        return sum(
            1
            for item in executed_actions
            if isinstance(item, dict)
            and str(item.get("action") or "").upper() in trade_actions
        )

    def _get_detail_tool_calls(self, run: StrategyRun) -> list[dict[str, Any]]:
        skill_payloads = (
            run.skill_payloads if isinstance(run.skill_payloads, dict) else {}
        )
        decision_payload = (
            run.decision_payload if isinstance(run.decision_payload, dict) else {}
        )

        tool_calls = skill_payloads.get("tool_calls")
        if not isinstance(tool_calls, list):
            tool_calls = decision_payload.get("tool_calls")
        if not isinstance(tool_calls, list):
            return []
        return [item for item in tool_calls if isinstance(item, dict)]

    def _empty_account_overview(self, errors: list[str] | None = None) -> dict[str, Any]:
        return {
            "open_date": None,
            "operating_days": None,
            "initial_capital": None,
            "total_assets": None,
            "total_market_value": None,
            "cash_balance": None,
            "total_position_ratio": None,
            "holding_profit": None,
            "total_return_ratio": None,
            "nav": None,
            "daily_profit": None,
            "daily_return_ratio": None,
            "positions": [],
            "orders": [],
            "trade_summaries": [],
            "errors": errors or [],
        }

    def _with_account_raw(
        self,
        overview: dict[str, Any],
        *,
        include_raw: bool,
        balance_result: dict[str, Any] | None,
        positions_result: dict[str, Any] | None,
        orders_result: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if include_raw:
            overview["raw_balance"] = balance_result
            overview["raw_positions"] = positions_result
            overview["raw_orders"] = orders_result
        return overview

    def _build_account_response(
        self,
        *,
        balance_result: dict[str, Any] | None,
        positions_result: dict[str, Any] | None,
        orders_result: dict[str, Any] | None,
        errors: list[str],
        include_raw: bool,
    ) -> dict[str, Any]:
        if (
            balance_result is None
            and positions_result is None
            and orders_result is None
        ):
            return self._with_account_raw(
                self._empty_account_overview(errors),
                include_raw=include_raw,
                balance_result=balance_result,
                positions_result=positions_result,
                orders_result=orders_result,
            )

        overview = self._build_account_overview(balance_result, positions_result)
        normalized_orders = self._build_orders_overview(orders_result)
        overview["orders"] = normalized_orders
        overview["trade_summaries"] = self._build_trade_summaries(
            normalized_orders,
            overview.get("positions") or [],
        )
        overview["errors"] = errors
        return self._with_account_raw(
            overview,
            include_raw=include_raw,
            balance_result=balance_result,
            positions_result=positions_result,
            orders_result=orders_result,
        )

    def _get_cached_account_overview(self) -> dict[str, Any] | None:
        with self._account_cache_lock:
            if (
                self._account_overview_cache is None
                or self._account_overview_cache_expires_at is None
                or self._account_overview_cache_expires_at <= now_utc()
            ):
                self._account_overview_cache = None
                self._account_overview_cache_expires_at = None
                return None

            return dict(self._account_overview_cache)

    def _set_cached_account_overview(self, overview: dict[str, Any]) -> None:
        ttl_seconds = max(0, int(get_settings().account_overview_cache_ttl_seconds))
        if ttl_seconds <= 0:
            with self._account_cache_lock:
                self._account_overview_cache = None
                self._account_overview_cache_expires_at = None
            return

        # Keep debug-only upstream payloads out of the process cache to reduce
        # steady-state RSS while preserving the normalized account summary.
        cached_overview = dict(overview)
        cached_overview.pop("raw_balance", None)
        cached_overview.pop("raw_positions", None)
        cached_overview.pop("raw_orders", None)

        with self._account_cache_lock:
            self._account_overview_cache = cached_overview
            self._account_overview_cache_expires_at = now_utc() + timedelta(
                seconds=ttl_seconds
            )

    def _fetch_live_account_payloads(
        self, client: MXClient
    ) -> dict[str, dict[str, Any]]:
        return {
            "balance": self._safe_call(client.get_balance),
            "positions": self._safe_call(client.get_positions),
            "orders": self._safe_call(client.get_orders),
        }

    def get_account_overview(
        self,
        *,
        include_raw: bool = False,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        if not force_refresh and not include_raw:
            cached_overview = self._get_cached_account_overview()
            if cached_overview is not None:
                return cached_overview

        with session_scope() as db:
            settings = self.get_or_create_settings(db)
            cached_balance_result, cached_positions_result, cached_orders_result = (
                self._get_recent_account_snapshot(db)
            )

        errors: list[str] = []
        balance_result = cached_balance_result
        positions_result = cached_positions_result
        orders_result = cached_orders_result
        client: MXClient | None = None

        if settings.mx_api_key:
            try:
                client = MXClient(api_key=settings.mx_api_key)
            except Exception as exc:
                if (
                    balance_result is None
                    and positions_result is None
                    and orders_result is None
                ):
                    return self._build_account_response(
                        balance_result=None,
                        positions_result=None,
                        orders_result=None,
                        errors=[str(exc)],
                        include_raw=include_raw,
                    )

                errors.append(f"{str(exc)}，当前展示最近一次任务缓存的账户数据。")
                overview = self._build_account_response(
                    balance_result=balance_result,
                    positions_result=positions_result,
                    orders_result=orders_result,
                    errors=errors,
                    include_raw=include_raw,
                )
                return overview

        try:
            if client is not None:
                live_payloads = self._fetch_live_account_payloads(client)

                balance_payload = live_payloads["balance"]
                if not balance_payload.get("ok"):
                    if cached_balance_result is not None:
                        balance_result = cached_balance_result
                        errors.append(
                            f"{str(balance_payload.get('error') or '资金接口失败')}，当前展示最近一次任务缓存的账户资金。"
                        )
                    else:
                        balance_result = None
                        errors.append(
                            str(balance_payload.get("error") or "资金接口失败")
                        )
                else:
                    balance_result = balance_payload.get("result")

                positions_payload = live_payloads["positions"]
                if not positions_payload.get("ok"):
                    if cached_positions_result is not None:
                        positions_result = cached_positions_result
                        errors.append(
                            f"{str(positions_payload.get('error') or '持仓接口失败')}，当前展示最近一次任务缓存的持仓数据。"
                        )
                    else:
                        positions_result = None
                        errors.append(
                            str(positions_payload.get("error") or "持仓接口失败")
                        )
                else:
                    positions_result = positions_payload.get("result")

                orders_payload = live_payloads["orders"]
                if not orders_payload.get("ok"):
                    if cached_orders_result is not None:
                        orders_result = cached_orders_result
                        errors.append(
                            f"{str(orders_payload.get('error') or '委托接口失败')}，当前展示最近一次任务缓存的委托数据。"
                        )
                    else:
                        orders_result = None
                        errors.append(
                            str(orders_payload.get("error") or "委托接口失败")
                        )
                else:
                    orders_result = orders_payload.get("result")
            elif (
                balance_result is None
                and positions_result is None
                and orders_result is None
            ):
                return self._build_account_response(
                    balance_result=None,
                    positions_result=None,
                    orders_result=None,
                    errors=errors
                    or ["未配置 MX API Key，且没有可用缓存账户数据。"],
                    include_raw=include_raw,
                )
        finally:
            if client is not None:
                client.close()

        overview = self._build_account_response(
            balance_result=balance_result,
            positions_result=positions_result,
            orders_result=orders_result,
            errors=errors,
            include_raw=include_raw,
        )
        self._set_cached_account_overview(
            self._build_account_response(
                balance_result=balance_result,
                positions_result=positions_result,
                orders_result=orders_result,
                errors=errors,
                include_raw=True,
            )
        )
        return overview

    def chat(self, payload: ChatRequest) -> dict[str, Any]:
        with session_scope() as db:
            settings = self.get_or_create_settings(db)

        if not settings.llm_base_url or not settings.llm_api_key:
            raise RuntimeError("未配置大模型接口，无法执行 AI 聊天。")

        messages = [
            {"role": item.role, "content": item.content} for item in payload.messages
        ]

        content = llm_service.chat(
            model=settings.llm_model,
            base_url=str(settings.llm_base_url),
            api_key=str(settings.llm_api_key),
            system_prompt=settings.system_prompt,
            messages=messages,
            timeout_seconds=1800,
            tool_context={"app_settings": settings},
        )

        return {
            "message": {
                "role": "assistant",
                "content": content,
            },
            "context": {
                "system_prompt_included": True,
                "tool_access_account_summary": True,
                "tool_access_positions": True,
                "tool_access_orders": True,
                "tool_access_runs": True,
            },
        }

    def chat_stream(self, payload: ChatRequest) -> Iterator[dict[str, Any]]:
        """Yield SSE-style events from the chat agent loop in real time.

        Runs the LLM agent loop on a worker thread and forwards emitted events
        to subscribers via an in-process queue. No StrategyRun is created."""
        with session_scope() as db:
            settings = self.get_or_create_settings(db)

        if not settings.llm_base_url or not settings.llm_api_key:
            raise RuntimeError("未配置大模型接口，无法执行 AI 聊天。")

        messages = [
            {"role": item.role, "content": item.content} for item in payload.messages
        ]
        settings_snapshot = SimpleNamespace(
            mx_api_key=settings.mx_api_key,
            system_prompt=settings.system_prompt,
            llm_model=settings.llm_model,
            llm_base_url=str(settings.llm_base_url),
            llm_api_key=str(settings.llm_api_key),
        )

        event_queue: queue.Queue[dict[str, Any] | None] = queue.Queue()
        cancel_event = Event()

        def _emit(event_type: str, **data: Any) -> None:
            event_queue.put(
                {"type": event_type, "ts": time.time(), **data}
            )

        def _worker() -> None:
            try:
                content = llm_service.chat(
                    model=settings_snapshot.llm_model,
                    base_url=settings_snapshot.llm_base_url,
                    api_key=settings_snapshot.llm_api_key,
                    system_prompt=settings_snapshot.system_prompt,
                    messages=messages,
                    timeout_seconds=180,
                    tool_context={"app_settings": settings_snapshot},
                    emit=_emit,
                    cancel_event=cancel_event,
                )
                _emit("completed", message=content)
            except LLMStreamCancelled:
                logger.info("chat_stream worker cancelled")
            except Exception as exc:  # noqa: BLE001
                logger.exception("chat_stream worker failed")
                _emit(
                    "failed",
                    message=str(exc),
                    traceback=traceback.format_exc(limit=4),
                )
            finally:
                event_queue.put(None)

        worker = Thread(target=_worker, daemon=True, name="aniu-chat-stream")
        worker.start()

        terminal_event_seen = False
        try:
            while True:
                try:
                    event = event_queue.get(timeout=15.0)
                except queue.Empty:
                    yield {"type": "heartbeat", "ts": time.time()}
                    continue
                if event is None:
                    return
                if event.get("type") in {"completed", "failed"}:
                    terminal_event_seen = True
                yield event
                if terminal_event_seen:
                    # Drain any trailing events until sentinel so the thread exits cleanly.
                    while True:
                        trailing = event_queue.get()
                        if trailing is None:
                            return
        finally:
            cancel_event.set()
            worker.join(timeout=1.0)

    def _build_orders_overview(
        self, orders_payload: dict[str, Any] | None
    ) -> list[dict[str, Any]]:
        orders_source = (
            orders_payload.get("data") if isinstance(orders_payload, dict) else {}
        )
        if isinstance(orders_source, dict):
            rows = (
                orders_source.get("rows")
                or orders_source.get("list")
                or orders_source.get("orderList")
                or orders_source.get("orders")
                or []
            )
        else:
            rows = orders_source or []

        normalized_orders: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue

            side_value = str(
                row.get("orderDrt")
                or row.get("drt")
                or row.get("bsFlag")
                or row.get("side")
                or row.get("tradeType")
                or ""
            ).strip()
            side = "sell" if side_value in {"2", "SELL", "sell"} else "buy"

            status_raw = str(
                row.get("orderStatus")
                or row.get("status")
                or row.get("dbStatus")
                or "unknown"
            ).strip()

            raw_symbol = str(
                row.get("stockCode") or row.get("secCode") or row.get("code") or ""
            ).strip()
            market_code = row.get("secMkt")
            if market_code is None:
                market_code = row.get("market")
            suffix = _market_suffix(market_code)
            symbol = f"{raw_symbol}.{suffix}" if raw_symbol and suffix else raw_symbol

            order_quantity = int(
                _parse_float(
                    row.get("orderCount")
                    or row.get("count")
                    or row.get("quantity")
                    or row.get("orderQty")
                )
                or 0
            )
            filled_quantity = int(
                _parse_float(
                    row.get("dealCount")
                    or row.get("tradeCount")
                    or row.get("filledQuantity")
                    or row.get("filledQty")
                )
                or 0
            )

            normalized_orders.append(
                {
                    "order_id": str(
                        row.get("orderId")
                        or row.get("entrustNo")
                        or row.get("id")
                        or "--"
                    ),
                    "order_time": _format_timestamp(
                        row.get("orderTime")
                        or row.get("entrustTime")
                        or row.get("time")
                    ),
                    "name": str(
                        row.get("stockName")
                        or row.get("secName")
                        or row.get("name")
                        or "--"
                    ).strip(),
                    "symbol": symbol,
                    "side": side,
                    "side_text": "卖出" if side == "sell" else "买入",
                    "status": status_raw.lower(),
                    "status_text": _order_status_text(
                        status_raw,
                        filled_quantity=filled_quantity,
                        order_quantity=order_quantity,
                        db_status=row.get("dbStatus"),
                    ),
                    "order_price": _scaled_decimal(
                        row.get("orderPrice") or row.get("price"),
                        row.get("priceDec") or row.get("orderPriceDec"),
                    ),
                    "order_quantity": order_quantity,
                    "filled_price": _scaled_decimal(
                        row.get("dealPrice")
                        or row.get("tradePrice")
                        or row.get("filledPrice"),
                        row.get("priceDec") or row.get("dealPriceDec"),
                    ),
                    "filled_quantity": filled_quantity,
                }
            )

        return normalized_orders

    def _build_trade_summaries(
        self,
        orders: list[dict[str, Any]],
        positions: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        active_symbols = {
            str(position.get("symbol") or "").strip()
            for position in positions
            if isinstance(position, dict)
            and str(position.get("symbol") or "").strip()
            and int(_parse_float(position.get("volume")) or 0) > 0
        }

        grouped_orders: dict[str, list[dict[str, Any]]] = {}
        for order in orders:
            if not isinstance(order, dict):
                continue
            symbol = str(order.get("symbol") or "").strip()
            if not symbol:
                continue
            grouped_orders.setdefault(symbol, []).append(order)

        summaries: list[dict[str, Any]] = []
        for symbol, symbol_orders in grouped_orders.items():
            buy_lots: list[dict[str, Any]] = []
            matched_quantity = 0
            matched_buy_amount = 0.0
            matched_sell_amount = 0.0
            first_buy_time: str | None = None
            last_exit_time: str | None = None
            name = "--"

            sorted_orders = sorted(
                symbol_orders,
                key=lambda item: (
                    str(item.get("order_time") or ""),
                    str(item.get("order_id") or ""),
                ),
            )

            for order in sorted_orders:
                filled_quantity = int(_parse_float(order.get("filled_quantity")) or 0)
                if filled_quantity <= 0:
                    continue

                filled_price = _parse_float(order.get("filled_price"))
                if filled_price is None or filled_price <= 0:
                    filled_price = _parse_float(order.get("order_price"))
                if filled_price is None or filled_price <= 0:
                    continue

                order_name = str(order.get("name") or "").strip()
                if order_name:
                    name = order_name

                if str(order.get("side") or "") == "buy":
                    order_time = str(order.get("order_time") or "").strip() or None
                    if first_buy_time is None and order_time:
                        first_buy_time = order_time
                    buy_lots.append(
                        {
                            "quantity": filled_quantity,
                            "price": filled_price,
                            "order_time": order_time,
                        }
                    )
                    continue

                remaining_sell = filled_quantity
                while remaining_sell > 0 and buy_lots:
                    lot = buy_lots[0]
                    lot_quantity = int(lot.get("quantity") or 0)
                    lot_price = _parse_float(lot.get("price")) or 0.0
                    if lot_quantity <= 0 or lot_price <= 0:
                        buy_lots.pop(0)
                        continue

                    matched = min(remaining_sell, lot_quantity)
                    matched_quantity += matched
                    matched_buy_amount += lot_price * matched
                    matched_sell_amount += filled_price * matched
                    remaining_sell -= matched
                    lot["quantity"] = lot_quantity - matched
                    last_exit_time = (
                        str(order.get("order_time") or "").strip() or last_exit_time
                    )

                    if int(lot.get("quantity") or 0) <= 0:
                        buy_lots.pop(0)

            if matched_quantity <= 0:
                continue
            if symbol in active_symbols:
                continue
            if any(int(lot.get("quantity") or 0) > 0 for lot in buy_lots):
                continue
            if matched_buy_amount <= 0:
                continue

            profit = matched_sell_amount - matched_buy_amount
            summaries.append(
                {
                    "name": name or symbol,
                    "symbol": symbol,
                    "volume": matched_quantity,
                    "buy_amount": matched_buy_amount,
                    "sell_amount": matched_sell_amount,
                    "buy_price": matched_buy_amount / matched_quantity,
                    "sell_price": matched_sell_amount / matched_quantity,
                    "profit": profit,
                    "profit_ratio": profit / matched_buy_amount,
                    "opened_at": first_buy_time,
                    "closed_at": last_exit_time,
                }
            )

        summaries.sort(
            key=lambda item: str(item.get("closed_at") or ""),
            reverse=True,
        )
        return summaries

    def _prepare_run(
        self,
        trigger_source: str,
        schedule_id: int | None,
    ) -> tuple[int, dict[str, Any]]:
        with session_scope() as db:
            settings = self.get_or_create_settings(db)
            schedule = (
                db.get(StrategySchedule, schedule_id) if schedule_id else None
            )
            if schedule_id is not None and schedule is None:
                raise RuntimeError("指定的定时任务不存在。")
            run = StrategyRun(
                trigger_source=trigger_source,
                run_type=schedule.run_type if schedule else "analysis",
                schedule_id=schedule.id if schedule else None,
                schedule_name=schedule.name if schedule else None,
                status="running",
            )
            db.add(run)
            db.flush()
            run_id = run.id
            manual_task_prompt = str(
                getattr(settings, "task_prompt", "") or ""
            ).strip() or (
                "请先分析当前情况，必要时自行调用妙想工具获取数据，并在需要时执行模拟交易。"
                "最后用自然语言总结本次判断、依据和操作结果。"
            )
            settings_snapshot = {
                "id": settings.id,
                "mx_api_key": settings.mx_api_key,
                "llm_base_url": settings.llm_base_url,
                "llm_api_key": settings.llm_api_key,
                "llm_model": settings.llm_model,
                "run_type": schedule.run_type if schedule else "analysis",
                "schedule_id": schedule.id if schedule else None,
                "system_prompt": settings.system_prompt,
                "task_prompt": schedule.task_prompt if schedule else manual_task_prompt,
                "timeout_seconds": int(
                    schedule.timeout_seconds if schedule else 1800
                ),
                "automation_session_id": getattr(
                    settings, "automation_session_id", None
                ),
                "automation_context_window_tokens": getattr(
                    settings,
                    "automation_context_window_tokens",
                    AUTOMATION_DEFAULT_CONTEXT_WINDOW_TOKENS,
                ),
                "automation_target_prompt_tokens": getattr(
                    settings,
                    "automation_target_prompt_tokens",
                    AUTOMATION_DEFAULT_TARGET_PROMPT_TOKENS,
                ),
                "automation_recent_message_limit": getattr(
                    settings,
                    "automation_recent_message_limit",
                    AUTOMATION_DEFAULT_RECENT_MESSAGE_LIMIT,
                ),
                "automation_enable_auto_compaction": getattr(
                    settings, "automation_enable_auto_compaction", True
                ),
                "automation_idle_summary_hours": getattr(
                    settings,
                    "automation_idle_summary_hours",
                    AUTOMATION_DEFAULT_IDLE_SUMMARY_HOURS,
                ),
                "automation_context_source": getattr(
                    settings, "automation_context_source", "default"
                ),
            }
        return run_id, settings_snapshot

    def _run_body(
        self,
        *,
        run_id: int,
        settings_snapshot: dict[str, Any],
        trigger_source: str,
        schedule_id: int | None,
        emit: Any = None,
        return_full_run: bool = True,
    ) -> StrategyRun | None:
        prefetched_tool_calls: list[dict[str, Any]] = []
        prefetched_context: str | None = None
        automation_session_id: int | None = None
        prompt_message_id: int | None = None
        response_message_id: int | None = None
        context_summary_version: int | None = None
        context_tokens_estimate: int | None = None
        automation_phase = "prefetch"
        _emit = emit if callable(emit) else (lambda *_a, **_kw: None)

        try:
            logger.info(
                "execute_run started: run_id=%s, trigger=%s, schedule_id=%s",
                run_id,
                trigger_source,
                schedule_id,
            )
            _emit(
                "stage",
                stage="started",
                message="任务已启动",
                trigger_source=trigger_source,
                schedule_id=schedule_id,
            )

            settings = SimpleNamespace(**settings_snapshot)
            if not settings.mx_api_key:
                raise RuntimeError("未配置 MX API Key，请先在设置页保存后再运行。")
            client = MXClient(api_key=settings.mx_api_key)
            try:
                _emit("stage", stage="prefetch", message="正在预取账户数据")
                prefetched_tool_calls = self._prefetch_account_tool_calls(
                    client=client,
                    app_settings=settings,
                )
                for call in prefetched_tool_calls:
                    _emit(
                        "tool_call",
                        phase="prefetch",
                        tool_name=call.get("tool_name") or call.get("name"),
                        status="done",
                        ok=bool(call.get("ok", True)),
                        summary=call.get("summary"),
                    )
                prefetched_context = self._build_prefetched_account_context(
                    prefetched_tool_calls
                )
                if prefetched_context:
                    original_task_prompt = str(
                        getattr(settings, "task_prompt", "") or ""
                    ).strip()
                    settings.task_prompt = (
                        f"{original_task_prompt}\n\n{prefetched_context}"
                        if original_task_prompt
                        else prefetched_context
                    )
                _emit("stage", stage="llm", message="正在调用大模型")
                automation_phase = "llm"
                if self._is_automation_trigger(trigger_source):
                    with session_scope() as db:
                        session = self._get_or_create_automation_session(db)
                        previous_last_message_at = _assume_utc(session.last_message_at)
                        archived_summary = session.archived_summary
                        automation_session_id = session.id
                        user_content = self._build_automation_user_content(
                            settings=settings,
                            trigger_source=trigger_source,
                            schedule_id=schedule_id,
                            schedule_name=getattr(settings, "schedule_name", None),
                            run_type=str(getattr(settings, "run_type", "analysis") or "analysis"),
                            task_prompt=str(getattr(settings, "task_prompt", "") or ""),
                            prefetched_context=prefetched_context,
                        )
                        user_message = self._persist_automation_user_message(
                            db=db,
                            session=session,
                            run_id=run_id,
                            content=user_content,
                            schedule_id=schedule_id,
                            schedule_name=getattr(settings, "schedule_name", None),
                            run_type=str(getattr(settings, "run_type", "analysis") or "analysis"),
                            trigger_source=trigger_source,
                        )
                        prompt_message_id = user_message.id
                        history_records = self._list_automation_history_records(
                            db=db,
                            session_id=session.id,
                            recent_limit=int(
                                getattr(settings, "automation_recent_message_limit", 0)
                                or AUTOMATION_DEFAULT_RECENT_MESSAGE_LIMIT
                            ),
                        )
                        history_messages = self._build_automation_history_messages(
                            history_records
                        )
                        resume_block = self._build_resume_summary_block(
                            idle_since=previous_last_message_at,
                            archived_summary=archived_summary,
                            settings=settings,
                        )
                        llm_messages = self._build_automation_prompt_messages(
                            session=session,
                            history_messages=history_messages,
                            resume_block=resume_block,
                        )
                        context_tokens_estimate = self._estimate_automation_context_tokens(
                            session=session,
                            settings=settings,
                            messages=llm_messages,
                            resume_block=resume_block,
                        )
                        context_tokens_estimate = max(
                            context_tokens_estimate,
                            estimate_messages_tokens(history_messages),
                        )
                        with db.no_autoflush:
                            run = db.get(StrategyRun, run_id)
                            if run is not None:
                                run.chat_session_id = session.id
                                run.prompt_message_id = prompt_message_id
                                run.context_tokens_estimate = context_tokens_estimate
                                run.context_summary_version = int(
                                    session.summary_revision or 0
                                )
                                db.add(run)
                        context_summary_version = int(session.summary_revision or 0)
                    decision, llm_request, llm_response, runtime_trace = (
                        llm_service.run_agent_with_messages(
                            app_settings=settings,
                            client=client,
                            messages=llm_messages,
                            emit=_emit,
                        )
                    )
                else:
                    run_agent = llm_service.run_agent
                    if self._run_agent_supports_emit(run_agent):
                        decision, llm_request, llm_response, runtime_trace = run_agent(
                            settings,
                            client,
                            emit=_emit,
                        )
                    else:
                        decision, llm_request, llm_response, runtime_trace = run_agent(
                            settings,
                            client,
                        )
            finally:
                client.close()

            tool_calls = decision.get("tool_calls")
            skill_payloads = {
                "prefetched_tool_calls": prefetched_tool_calls,
                "prefetched_context": prefetched_context,
                "tool_calls": tool_calls,
                "runtime_trace": runtime_trace,
            }
            executed_actions = self._extract_executed_actions(tool_calls)
            persisted_trade_orders = [
                {
                    "symbol": action.get("symbol"),
                    "action": action.get("action"),
                    "quantity": action.get("quantity"),
                    "price": action.get("price"),
                    "status": action.get("status") or "submitted",
                }
                for action in executed_actions
                if str(action.get("action") or "") in {"BUY", "SELL"}
            ]
            completed_at = now_utc()
            completed_at_shanghai = completed_at.astimezone(SHANGHAI_TZ)

            if persisted_trade_orders:
                _emit(
                    "stage",
                    stage="trade",
                    message=f"正在写入交易执行记录（{len(persisted_trade_orders)} 条）",
                )

            with session_scope() as db:
                run = db.get(StrategyRun, run_id)
                if run is None:
                    raise RuntimeError("运行记录不存在。")
                run.chat_session_id = automation_session_id
                run.prompt_message_id = prompt_message_id
                run.context_summary_version = context_summary_version
                run.context_tokens_estimate = context_tokens_estimate
                run.skill_payloads = skill_payloads
                run.llm_request_payload = llm_request
                run.llm_response_payload = llm_response
                run.decision_payload = decision
                run.analysis_summary = self._build_analysis_summary(
                    decision.get("final_answer")
                )
                run.final_answer = (
                    str(decision.get("final_answer") or "").strip() or None
                )
                run.executed_actions = executed_actions
                run.status = "completed"
                run.finished_at = completed_at
                db.add(run)

                for action in executed_actions:
                    if str(action.get("action") or "") not in {"BUY", "SELL"}:
                        continue
                    db.add(
                        TradeOrder(
                            run_id=run_id,
                            symbol=str(action.get("symbol") or ""),
                            action=str(action.get("action") or ""),
                            quantity=int(action.get("quantity") or 0),
                            price_type=str(action.get("price_type") or "MARKET"),
                            price=_parse_float(action.get("price")),
                            status=str(action.get("status") or "submitted"),
                            response_payload=action.get("response"),
                        )
                    )

                if schedule_id:
                    schedule = db.get(StrategySchedule, schedule_id)
                    if schedule is not None:
                        schedule.last_run_at = completed_at
                        schedule.retry_count = 0
                        schedule.retry_after_at = None
                        schedule.next_run_at = self._compute_next_run_at(
                            schedule.cron_expression,
                            from_time=completed_at_shanghai,
                        )
                        db.add(schedule)

                if automation_session_id is not None:
                    session = db.get(ChatSession, automation_session_id)
                    if session is not None:
                        assistant_content = self._build_automation_assistant_content(
                            run_id=run_id,
                            run_type=str(getattr(settings, "run_type", "analysis") or "analysis"),
                            status="completed",
                            final_answer=str(decision.get("final_answer") or "").strip()
                            or None,
                            tool_calls=tool_calls if isinstance(tool_calls, list) else None,
                            executed_actions=executed_actions,
                        )
                        response_message = self._persist_automation_assistant_message(
                            db=db,
                            session=session,
                            run_id=run_id,
                            content=assistant_content,
                            tool_calls=tool_calls if isinstance(tool_calls, list) else None,
                            status="completed",
                            meta_payload={
                                "run_type": str(
                                    getattr(settings, "run_type", "analysis") or "analysis"
                                ),
                                "executed_action_count": len(executed_actions),
                            },
                        )
                        response_message_id = response_message.id
                        run.response_message_id = response_message_id
                        archived_summary, summary_version = (
                            self._maybe_compact_automation_session(
                                db=db,
                                session=session,
                                settings=settings,
                                estimated_tokens=int(context_tokens_estimate or 0),
                            )
                        )
                        del archived_summary
                        run.context_summary_version = (
                            int(summary_version)
                            if summary_version is not None
                            else run.context_summary_version
                        )
                        db.add(run)

            for action in persisted_trade_orders:
                _emit(
                    "trade_order",
                    symbol=action.get("symbol"),
                    action=action.get("action"),
                    quantity=action.get("quantity"),
                    price=action.get("price"),
                    status=action.get("status") or "submitted",
                )

            if not return_full_run:
                logger.info(
                    "execute_run completed: run_id=%s, actions=%d",
                    run_id,
                    len(executed_actions),
                )
                _emit(
                    "completed",
                    message="任务完成",
                    actions=len(executed_actions),
                )
                return None

            with session_scope() as db:
                run = self.get_run(db, run_id)
                if run is None:
                    raise RuntimeError("运行记录不存在。")
                logger.info(
                    "execute_run completed: run_id=%s, actions=%d",
                    run_id,
                    len(executed_actions),
                )
                _emit(
                    "completed",
                    message="任务完成",
                    actions=len(executed_actions),
                )
                return run
        except Exception as exc:
            logger.error(
                "execute_run failed: run_id=%s, error=%s",
                run_id,
                exc,
            )
            with session_scope() as db:
                run = db.get(StrategyRun, run_id)
                if run is not None:
                    run.chat_session_id = automation_session_id
                    run.prompt_message_id = prompt_message_id
                    run.response_message_id = response_message_id
                    run.context_summary_version = context_summary_version
                    run.context_tokens_estimate = context_tokens_estimate
                    if prefetched_tool_calls:
                        existing_skill_payloads = (
                            run.skill_payloads
                            if isinstance(run.skill_payloads, dict)
                            else {}
                        )
                        existing_skill_payloads["prefetched_tool_calls"] = (
                            prefetched_tool_calls
                        )
                        existing_skill_payloads["prefetched_context"] = (
                            prefetched_context
                        )
                        run.skill_payloads = existing_skill_payloads
                    run.status = "failed"
                    run.error_message = str(exc)
                    run.final_answer = None
                    run.finished_at = now_utc()
                    db.add(run)
                    if automation_session_id is not None:
                        session = db.get(ChatSession, automation_session_id)
                        if session is not None:
                            assistant_content = self._build_automation_assistant_content(
                                run_id=run_id,
                                run_type=str(settings_snapshot.get("run_type") or "analysis"),
                                status="failed",
                                final_answer=None,
                                tool_calls=prefetched_tool_calls,
                                executed_actions=None,
                                error_message=str(exc),
                                phase=automation_phase,
                            )
                            response_message = self._persist_automation_assistant_message(
                                db=db,
                                session=session,
                                run_id=run_id,
                                content=assistant_content,
                                tool_calls=prefetched_tool_calls,
                                status="failed",
                                meta_payload={
                                    "phase": automation_phase,
                                    "run_type": str(
                                        settings_snapshot.get("run_type") or "analysis"
                                    ),
                                },
                            )
                            run.response_message_id = response_message.id
                            response_message_id = response_message.id
                            db.add(run)
                if schedule_id:
                    schedule = db.get(StrategySchedule, schedule_id)
                    if schedule is not None:
                        schedule.last_run_at = now_utc()
                        schedule.next_run_at = self._compute_next_run_at(
                            schedule.cron_expression,
                            from_time=now_shanghai(),
                        )
                        if trigger_source == "schedule":
                            retry_count = max(int(schedule.retry_count or 0), 0)
                            if retry_count < SCHEDULE_MAX_RETRIES:
                                schedule.retry_count = retry_count + 1
                                schedule.retry_after_at = now_utc() + SCHEDULE_RETRY_DELAY
                            else:
                                schedule.retry_count = 0
                                schedule.retry_after_at = None
                        else:
                            schedule.retry_count = max(int(schedule.retry_count or 0), 0)
                        db.add(schedule)
            _emit("failed", message=str(exc))
            raise

    def execute_run(
        self,
        trigger_source: str = "manual",
        schedule_id: int | None = None,
    ) -> StrategyRun:
        if not self._run_lock.acquire(blocking=False):
            raise RuntimeError("已有运行中的任务，请稍后再试。")
        try:
            run_id, settings_snapshot = self._prepare_run(trigger_source, schedule_id)
            return self._run_body(
                run_id=run_id,
                settings_snapshot=settings_snapshot,
                trigger_source=trigger_source,
                schedule_id=schedule_id,
            )
        finally:
            self._run_lock.release()

    def start_run_async(
        self,
        trigger_source: str = "manual",
        schedule_id: int | None = None,
    ) -> int:
        """Launch a run on a background thread and return its run_id immediately.

        Event stream: subscribe via ``event_bus`` using the returned run_id.
        """
        if not self._run_lock.acquire(blocking=False):
            raise RuntimeError("已有运行中的任务，请稍后再试。")

        run_id: int | None = None
        try:
            run_id, settings_snapshot = self._prepare_run(trigger_source, schedule_id)
        except Exception:
            self._run_lock.release()
            raise

        emit = make_emitter(run_id)

        def _worker() -> None:
            try:
                self._run_body(
                    run_id=run_id,
                    settings_snapshot=settings_snapshot,
                    trigger_source=trigger_source,
                    schedule_id=schedule_id,
                    emit=emit,
                    return_full_run=False,
                )
            except Exception:
                logger.exception("async run worker failed: run_id=%s", run_id)
            finally:
                self._run_lock.release()

        Thread(
            target=_worker,
            name=f"run-{run_id}",
            daemon=True,
        ).start()
        return run_id

    def process_due_schedule(self) -> None:
        due_schedule_id: int | None = None
        with session_scope() as db:
            schedules = self.list_schedules(db)
            now = now_shanghai()
            earliest_due_at: datetime | None = None
            for schedule in schedules:
                if not schedule.enabled:
                    continue
                if schedule.next_run_at is None:
                    schedule.next_run_at = self._compute_next_run_at(
                        schedule.cron_expression
                    )
                    db.add(schedule)
                    continue
                if not trading_calendar_service.is_trading_day(now.date()):
                    schedule.next_run_at = self._compute_next_run_at(
                        schedule.cron_expression,
                        from_time=now,
                    )
                    db.add(schedule)
                    continue
                retry_after_at = _assume_utc(schedule.retry_after_at)
                if retry_after_at is not None:
                    retry_due = retry_after_at.astimezone(SHANGHAI_TZ)
                    if retry_due <= now:
                        if earliest_due_at is None or retry_due < earliest_due_at:
                            earliest_due_at = retry_due
                            due_schedule_id = schedule.id
                        continue
                if (
                    schedule.next_run_at is not None
                    and schedule.next_run_at.astimezone(SHANGHAI_TZ) <= now
                ):
                    schedule_due = schedule.next_run_at.astimezone(SHANGHAI_TZ)
                    if earliest_due_at is None or schedule_due < earliest_due_at:
                        earliest_due_at = schedule_due
                        due_schedule_id = schedule.id

        if due_schedule_id is not None:
            try:
                self.execute_run(trigger_source="schedule", schedule_id=due_schedule_id)
            except RuntimeError as exc:
                if "已有运行中的任务" in str(exc):
                    logger.info(
                        "process_due_schedule skipped because another run is active: schedule_id=%s",
                        due_schedule_id,
                    )
                    return
                raise

    def _safe_call(self, func: Any) -> dict[str, Any]:
        try:
            return {"ok": True, "result": func()}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def _get_recent_account_snapshot(
        self, db: Session
    ) -> tuple[
        dict[str, Any] | None,
        dict[str, Any] | None,
        dict[str, Any] | None,
    ]:
        stmt = select(StrategyRun).order_by(StrategyRun.started_at.desc()).limit(20)

        balance_result: dict[str, Any] | None = None
        positions_result: dict[str, Any] | None = None
        orders_result: dict[str, Any] | None = None

        for run in db.scalars(stmt).all():
            tool_calls = self._get_run_tool_calls(run)
            if not tool_calls:
                continue

            if balance_result is None:
                balance_result = self._extract_tool_result(tool_calls, "mx_get_balance")
            if positions_result is None:
                positions_result = self._extract_tool_result(
                    tool_calls, "mx_get_positions"
                )
            if orders_result is None:
                orders_result = self._extract_tool_result(tool_calls, "mx_get_orders")

            if (
                balance_result is not None
                and positions_result is not None
                and orders_result is not None
            ):
                break

        return balance_result, positions_result, orders_result

    def _get_run_tool_calls(self, run: StrategyRun) -> list[dict[str, Any]]:
        skill_payloads = (
            run.skill_payloads if isinstance(run.skill_payloads, dict) else {}
        )
        decision_payload = (
            run.decision_payload if isinstance(run.decision_payload, dict) else {}
        )

        combined_tool_calls: list[dict[str, Any]] = []
        prefetched_tool_calls = skill_payloads.get("prefetched_tool_calls")
        if isinstance(prefetched_tool_calls, list):
            combined_tool_calls.extend(
                item for item in prefetched_tool_calls if isinstance(item, dict)
            )

        tool_calls = self._get_detail_tool_calls(run)
        if tool_calls:
            combined_tool_calls.extend(
                tool_calls
            )
        return combined_tool_calls

    def _prefetch_account_tool_calls(
        self,
        *,
        client: MXClient,
        app_settings: Any,
    ) -> list[dict[str, Any]]:
        del app_settings
        live_payloads = self._fetch_live_account_payloads(client)
        payload_map = {
            "mx_get_balance": ("balance", "已查询账户资金。"),
            "mx_get_positions": ("positions", "已查询持仓。"),
            "mx_get_orders": ("orders", "已查询委托记录。"),
        }

        tool_calls: list[dict[str, Any]] = []
        for tool_name in ACCOUNT_PREFETCH_TOOL_NAMES:
            payload_key, summary = payload_map[tool_name]
            raw_result = live_payloads[payload_key]
            result: dict[str, Any] = {
                "ok": bool(raw_result.get("ok")),
                "tool_name": tool_name,
            }
            if raw_result.get("ok"):
                result["summary"] = summary
                result["result"] = raw_result.get("result")
            else:
                result["error"] = str(raw_result.get("error") or "预取失败")

            tool_calls.append(
                {
                    "name": tool_name,
                    "arguments": {},
                    "ok": result["ok"],
                    "summary": result.get("summary"),
                    "result": result,
                }
            )
        return tool_calls

    def _build_prefetched_account_context(
        self, tool_calls: list[dict[str, Any]]
    ) -> str | None:
        if not tool_calls:
            return None

        balance_result = self._extract_tool_result(tool_calls, "mx_get_balance")
        positions_result = self._extract_tool_result(tool_calls, "mx_get_positions")
        orders_result = self._extract_tool_result(tool_calls, "mx_get_orders")

        lines = [
            "系统已在本轮开始前预取账户快照，可直接使用；如需更实时或更细粒度的数据，再调用对应工具。"
        ]

        if balance_result is not None or positions_result is not None:
            overview = self._build_account_overview(balance_result, positions_result)
            lines.append(
                "账户快照："
                f"总资产={self._format_prefetch_number(overview.get('total_assets'))}，"
                f"现金余额={self._format_prefetch_number(overview.get('cash_balance'))}，"
                f"持仓市值={self._format_prefetch_number(overview.get('total_market_value'))}，"
                f"当日盈亏={self._format_prefetch_number(overview.get('daily_profit'))}。"
            )
            positions = overview.get("positions") or []
            if positions:
                position_preview = "；".join(
                    (
                        f"{item.get('name') or item.get('symbol')} "
                        f"{int(_parse_float(item.get('volume')) or 0)}股"
                    )
                    for item in positions[:3]
                    if isinstance(item, dict)
                )
                if position_preview:
                    lines.append(
                        f"持仓快照：共 {len(positions)} 条；前 3 条：{position_preview}。"
                    )

        normalized_orders = self._build_orders_overview(orders_result)
        if normalized_orders:
            order_preview = "；".join(
                (
                    f"{item.get('side_text') or '--'} "
                    f"{item.get('name') or item.get('symbol') or '--'} "
                    f"{item.get('status_text') or '--'}"
                )
                for item in normalized_orders[:3]
                if isinstance(item, dict)
            )
            lines.append(
                f"委托快照：共 {len(normalized_orders)} 条；前 3 条：{order_preview}。"
            )

        failed_prefetches = [
            f"{item.get('name')}: {item.get('result', {}).get('error')}"
            for item in tool_calls
            if isinstance(item, dict)
            and isinstance(item.get("result"), dict)
            and not item.get("result", {}).get("ok")
        ]
        if failed_prefetches:
            lines.append("预取失败：" + "；".join(failed_prefetches) + "。")

        return "\n".join(lines)

    def _format_prefetch_number(self, value: Any) -> str:
        numeric = _parse_float(value)
        if numeric is None:
            return "--"
        return f"{numeric:.2f}"

    def _extract_tool_result(
        self, tool_calls: list[dict[str, Any]], tool_name: str
    ) -> dict[str, Any] | None:
        for item in reversed(tool_calls):
            if item.get("name") != tool_name:
                continue
            result = item.get("result")
            if not isinstance(result, dict) or not result.get("ok"):
                continue
            payload = result.get("result")
            if isinstance(payload, dict):
                return payload
        return None

    def _extract_executed_actions(self, tool_calls: Any) -> list[dict[str, Any]]:
        if not isinstance(tool_calls, list):
            return []

        executed_actions: list[dict[str, Any]] = []
        for item in tool_calls:
            if not isinstance(item, dict):
                continue
            result = item.get("result")
            if not isinstance(result, dict) or not result.get("ok"):
                continue
            executed_action = result.get("executed_action")
            if not isinstance(executed_action, dict):
                continue
            action_name = str(executed_action.get("action") or "").upper()
            entry = {
                "symbol": str(
                    executed_action.get("symbol")
                    or executed_action.get("stock_code")
                    or ""
                ).strip(),
                "name": str(executed_action.get("name") or "").strip() or None,
                "action": action_name,
                "quantity": int(executed_action.get("quantity") or 0),
                "price_type": str(executed_action.get("price_type") or "MARKET"),
                "price": executed_action.get("price"),
                "reason": str(executed_action.get("reason") or "").strip(),
                "status": "submitted",
                "response": result.get("result"),
            }
            if action_name == "CANCEL":
                entry["price_type"] = "CANCEL"
                entry["status"] = "cancel_requested"
            if action_name == "MANAGE_SELF_SELECT":
                entry["price_type"] = "SELF_SELECT"
                entry["status"] = "completed"
                entry["symbol"] = str(executed_action.get("query") or "")
            executed_actions.append(entry)
        return executed_actions

    def _build_analysis_summary(self, final_answer: Any) -> str | None:
        text = str(final_answer or "").strip()
        if not text:
            return None
        compact = " ".join(text.split())
        if len(compact) <= 120:
            return compact
        return compact[:117] + "..."

    def _is_automation_trigger(self, trigger_source: str) -> bool:
        return str(trigger_source or "").strip() == "schedule"

    def _get_or_create_automation_session(self, db: Session) -> ChatSession:
        settings = self.get_or_create_settings(db)
        session_id = int(getattr(settings, "automation_session_id", 0) or 0)
        if session_id > 0:
            existing = db.get(ChatSession, session_id)
            if existing is not None and str(existing.kind or "") == "automation":
                return existing

        session = db.scalar(
            select(ChatSession).where(
                ChatSession.kind == "automation",
                ChatSession.slug == AUTOMATION_SESSION_SLUG,
            )
        )
        if session is None:
            session = ChatSession(
                title=AUTOMATION_SESSION_TITLE,
                kind="automation",
                slug=AUTOMATION_SESSION_SLUG,
            )
            db.add(session)
            db.flush()

        settings.automation_session_id = session.id
        settings.automation_context_window_tokens = int(
            getattr(settings, "automation_context_window_tokens", None)
            or AUTOMATION_DEFAULT_CONTEXT_WINDOW_TOKENS
        )
        settings.automation_target_prompt_tokens = int(
            getattr(settings, "automation_target_prompt_tokens", None)
            or AUTOMATION_DEFAULT_TARGET_PROMPT_TOKENS
        )
        settings.automation_recent_message_limit = int(
            getattr(settings, "automation_recent_message_limit", None)
            or AUTOMATION_DEFAULT_RECENT_MESSAGE_LIMIT
        )
        settings.automation_idle_summary_hours = int(
            getattr(settings, "automation_idle_summary_hours", None)
            or AUTOMATION_DEFAULT_IDLE_SUMMARY_HOURS
        )
        settings.automation_context_source = (
            str(getattr(settings, "automation_context_source", "") or "").strip()
            or "default"
        )
        settings.automation_context_detected_at = now_utc()
        if hasattr(settings, "_sa_instance_state"):
            db.add(settings)
        return session

    def _build_automation_user_content(
        self,
        *,
        settings: Any,
        trigger_source: str,
        schedule_id: int | None,
        schedule_name: str | None,
        run_type: str,
        task_prompt: str,
        prefetched_context: str | None,
    ) -> str:
        run_time = now_shanghai().isoformat(timespec="seconds")
        lines = [
            "[自动任务触发]",
            f"时间: {run_time}",
            f"来源: {trigger_source or 'schedule'}",
            f"调度ID: {schedule_id if schedule_id is not None else '--'}",
            f"任务名称: {str(schedule_name or '--').strip() or '--'}",
            f"运行类型: {run_type}",
            "",
            "本轮任务:",
            str(task_prompt or "").strip() or "--",
            "",
            "本轮账户快照:",
            str(prefetched_context or "无预取快照。"),
            "",
            "执行要求:",
            "请结合本会话历史继续判断，明确说明你是在延续、调整还是推翻之前计划。",
            "如果要执行交易，请说明原因、仓位影响和与上一轮计划的关系。",
        ]
        del settings
        return "\n".join(lines).strip()

    def _summarize_tool_calls(
        self, tool_calls: list[dict[str, Any]] | None, *, limit: int = 8
    ) -> list[str]:
        if not isinstance(tool_calls, list):
            return []
        lines: list[str] = []
        for item in tool_calls[:limit]:
            if not isinstance(item, dict):
                continue
            tool_name = str(item.get("name") or item.get("tool_name") or "").strip()
            if not tool_name:
                continue
            result = item.get("result")
            summary = ""
            if isinstance(result, dict):
                summary = str(result.get("summary") or result.get("error") or "").strip()
            if not summary:
                summary = str(item.get("summary") or "已执行").strip() or "已执行"
            lines.append(f"- {tool_name}: {summary}")
        return lines

    def _build_automation_assistant_content(
        self,
        *,
        run_id: int,
        run_type: str,
        status: str,
        final_answer: str | None,
        tool_calls: list[dict[str, Any]] | None,
        executed_actions: list[dict[str, Any]] | None,
        error_message: str | None = None,
        phase: str | None = None,
    ) -> str:
        content = str(final_answer or "").strip()
        actions = executed_actions if isinstance(executed_actions, list) else []
        tool_lines = self._summarize_tool_calls(tool_calls)

        if status == "completed":
            action_lines = []
            for item in actions:
                if not isinstance(item, dict):
                    continue
                action = str(item.get("action") or "").upper()
                symbol = str(item.get("symbol") or "").strip()
                quantity = int(item.get("quantity") or 0)
                price_type = str(item.get("price_type") or "MARKET")
                price = item.get("price")
                action_text = f"- {action} {symbol} {quantity}股 {price_type}"
                if price not in (None, ""):
                    action_text += f" {price}"
                action_lines.append(action_text)

            summary_lines = [
                "[本轮执行摘要]",
                f"- run_id: {run_id}",
                f"- 状态: {status}",
                f"- 运行类型: {run_type}",
                "- 执行动作:",
            ]
            summary_lines.extend(action_lines or ["- 无交易执行。"])
            summary_lines.append("- 工具摘要:")
            summary_lines.extend(tool_lines or ["- 无工具调用。"])
            summary_lines.append("- 遗留事项:")
            summary_lines.append("- 参考本轮结论，并在下一轮结合最新账户快照继续判断。")
            if content:
                return f"{content}\n\n" + "\n".join(summary_lines)
            return "\n".join(summary_lines)

        failure_lines = [
            f"执行失败：{str(error_message or '未知错误').strip() or '未知错误'}",
            "",
            "[本轮失败摘要]",
            f"- run_id: {run_id}",
            f"- 阶段: {str(phase or 'run').strip() or 'run'}",
            "- 已完成部分:",
        ]
        failure_lines.extend(tool_lines or ["- 无已记录工具调用。"])
        failure_lines.append("- 遗留风险:")
        failure_lines.append("- 下次运行需结合失败原因和最新账户快照重新判断。")
        return "\n".join(failure_lines)

    def _persist_automation_user_message(
        self,
        *,
        db: Session,
        session: ChatSession,
        run_id: int,
        content: str,
        schedule_id: int | None,
        schedule_name: str | None,
        run_type: str,
        trigger_source: str,
    ) -> ChatMessageRecord:
        record = ChatMessageRecord(
            session_id=session.id,
            role="user",
            content=content,
            source="automation_run",
            run_id=run_id,
            message_kind="live_turn",
            meta_payload={
                "trigger_source": trigger_source,
                "schedule_id": schedule_id,
                "schedule_name": schedule_name,
                "run_type": run_type,
            },
        )
        db.add(record)
        db.flush()
        session.last_message_at = now_utc()
        db.add(session)
        return record

    def _slim_automation_tool_calls(
        self, tool_calls: list[dict[str, Any]] | None
    ) -> list[dict[str, Any]] | None:
        if not isinstance(tool_calls, list):
            return None
        slimmed: list[dict[str, Any]] = []
        for item in tool_calls:
            if not isinstance(item, dict):
                continue
            result = item.get("result") if isinstance(item.get("result"), dict) else {}
            entry = {
                "name": item.get("name"),
                "tool_call_id": item.get("id") or item.get("tool_call_id"),
                "arguments": item.get("arguments"),
                "ok": result.get("ok"),
                "summary": result.get("summary") or result.get("error"),
            }
            executed_action = result.get("executed_action")
            if isinstance(executed_action, dict):
                entry["executed_action"] = executed_action
            slimmed.append(entry)
        return slimmed or None

    def _persist_automation_assistant_message(
        self,
        *,
        db: Session,
        session: ChatSession,
        run_id: int,
        content: str,
        tool_calls: list[dict[str, Any]] | None,
        status: str,
        meta_payload: dict[str, Any] | None,
    ) -> ChatMessageRecord:
        record = ChatMessageRecord(
            session_id=session.id,
            role="assistant",
            content=content,
            source="automation_run",
            run_id=run_id,
            message_kind="live_turn",
            tool_calls=self._slim_automation_tool_calls(tool_calls),
            meta_payload={"status": status, **(meta_payload or {})} or None,
        )
        db.add(record)
        db.flush()
        session.last_message_at = now_utc()
        db.add(session)
        return record

    def _list_automation_history_records(
        self,
        *,
        db: Session,
        session_id: int,
        recent_limit: int,
    ) -> list[ChatMessageRecord]:
        limit = max(4, int(recent_limit or AUTOMATION_DEFAULT_RECENT_MESSAGE_LIMIT))
        session = db.get(ChatSession, session_id)
        last_compacted_message_id = int(
            getattr(session, "last_compacted_message_id", 0) or 0
        )
        stmt = (
            select(ChatMessageRecord)
            .where(ChatMessageRecord.session_id == session_id)
            .order_by(ChatMessageRecord.id.desc())
            .limit(limit)
        )
        if last_compacted_message_id > 0:
            stmt = stmt.where(ChatMessageRecord.id > last_compacted_message_id)
        records = (
            db.execute(
                stmt
            )
            .scalars()
            .all()
        )
        records.reverse()
        return records

    def _build_automation_history_messages(
        self, records: list[ChatMessageRecord]
    ) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        for record in records:
            if record.role not in {"user", "assistant", "system"}:
                continue
            content_parts = [str(record.content or "").strip()]
            tool_lines = self._summarize_tool_calls(record.tool_calls)
            if tool_lines:
                content_parts.append("[历史工具摘要]\n" + "\n".join(tool_lines))
            content = "\n\n".join(part for part in content_parts if part).strip()
            if not content:
                continue
            messages.append({"role": record.role, "content": content})
        return messages

    def _estimate_automation_context_tokens(
        self,
        *,
        session: ChatSession,
        settings: Any,
        messages: list[dict[str, Any]],
        resume_block: str | None,
    ) -> int:
        estimate = estimate_messages_tokens(messages)
        estimate += estimate_text_tokens(getattr(settings, "system_prompt", None))
        estimate += estimate_text_tokens(session.archived_summary)
        estimate += estimate_text_tokens(resume_block)
        return estimate

    def _build_resume_summary_block(
        self,
        *,
        idle_since: datetime | None,
        archived_summary: str | None,
        settings: Any,
    ) -> str | None:
        archived_summary = str(archived_summary or "").strip()
        if not archived_summary:
            return None
        last_message_at = _assume_utc(idle_since)
        if last_message_at is None:
            return None
        idle_hours = int(
            getattr(settings, "automation_idle_summary_hours", 0)
            or AUTOMATION_DEFAULT_IDLE_SUMMARY_HOURS
        )
        if idle_hours <= 0:
            return None
        elapsed = now_utc() - last_message_at
        if elapsed < timedelta(hours=idle_hours):
            return None
        return (
            f"[恢复摘要]\n该自动化会话已空闲 {int(elapsed.total_seconds() // 3600)} 小时。\n"
            f"以下是上次压缩后的状态摘要：\n{archived_summary}"
        )

    def _build_automation_context_system_message(
        self,
        *,
        session: ChatSession,
        resume_block: str | None,
    ) -> dict[str, Any] | None:
        parts: list[str] = []
        archived_summary = str(session.archived_summary or "").strip()
        if archived_summary:
            parts.append("[滚动摘要]\n" + archived_summary)
        if resume_block:
            parts.append(resume_block)
        if not parts:
            return None
        return {"role": "system", "content": "\n\n".join(parts)}

    def _build_automation_prompt_messages(
        self,
        *,
        session: ChatSession,
        history_messages: list[dict[str, Any]],
        resume_block: str | None,
    ) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        context_message = self._build_automation_context_system_message(
            session=session,
            resume_block=resume_block,
        )
        if context_message is not None:
            messages.append(context_message)
        messages.extend(history_messages)
        return messages

    def _build_compacted_summary_text(
        self, records: list[ChatMessageRecord]
    ) -> str | None:
        if not records:
            return None
        assistant_records = [record for record in records if record.role == "assistant"]
        if not assistant_records:
            return None
        recent = assistant_records[-6:]
        lines = [
            "## 当前策略",
            "- 结合最近自动化运行的结论、失败记录和账户快照继续决策。",
            "## 已执行动作",
        ]
        for record in recent:
            summary = self._build_analysis_summary(record.content)
            run_id = record.run_id if record.run_id is not None else "--"
            if summary:
                lines.append(f"- run_id {run_id}: {summary}")
        lines.extend(
            [
                "## 当前约束",
                "- 原始运行记录和交易记录以 StrategyRun / TradeOrder 为准。",
                "- 账户实时数字应以本轮最新快照和工具结果为准。",
                "## 后续计划",
                "- 下一轮结合最新账户快照，延续、调整或推翻之前计划。",
            ]
        )
        return "\n".join(lines)

    def _safe_prompt_budget(self, settings: Any) -> int:
        context_window = int(
            getattr(settings, "automation_context_window_tokens", 0)
            or AUTOMATION_DEFAULT_CONTEXT_WINDOW_TOKENS
        )
        safe_budget = max(
            2048,
            context_window
            - AUTOMATION_RESERVED_OUTPUT_TOKENS
            - AUTOMATION_SAFETY_BUFFER_TOKENS,
        )
        configured_target = int(
            getattr(settings, "automation_target_prompt_tokens", 0)
            or AUTOMATION_DEFAULT_TARGET_PROMPT_TOKENS
        )
        return min(configured_target, int(safe_budget * 0.7))

    def _should_compact_automation_session(
        self,
        *,
        session: ChatSession,
        records: list[ChatMessageRecord],
        settings: Any,
        estimated_tokens: int,
    ) -> bool:
        if not bool(getattr(settings, "automation_enable_auto_compaction", True)):
            return False
        recent_limit = int(
            getattr(settings, "automation_recent_message_limit", 0)
            or AUTOMATION_DEFAULT_RECENT_MESSAGE_LIMIT
        )
        if len(records) > recent_limit:
            return True
        if estimated_tokens > self._safe_prompt_budget(settings):
            return True
        last_message_at = _assume_utc(session.last_message_at)
        idle_hours = int(
            getattr(settings, "automation_idle_summary_hours", 0)
            or AUTOMATION_DEFAULT_IDLE_SUMMARY_HOURS
        )
        if last_message_at is not None and idle_hours > 0:
            if now_utc() - last_message_at >= timedelta(hours=idle_hours):
                return True
        return False

    def _maybe_compact_automation_session(
        self,
        *,
        db: Session,
        session: ChatSession,
        settings: Any,
        estimated_tokens: int,
    ) -> tuple[str | None, int | None]:
        records = (
            db.execute(
                select(ChatMessageRecord)
                .where(ChatMessageRecord.session_id == session.id)
                .order_by(ChatMessageRecord.id.asc())
            )
            .scalars()
            .all()
        )
        if not self._should_compact_automation_session(
            session=session,
            records=records,
            settings=settings,
            estimated_tokens=estimated_tokens,
        ):
            return session.archived_summary, session.summary_revision

        recent_limit = max(
            8,
            int(
                getattr(settings, "automation_recent_message_limit", 0)
                or AUTOMATION_DEFAULT_RECENT_MESSAGE_LIMIT
            )
            // 2,
        )
        compact_cutoff = max(0, len(records) - recent_limit)
        compact_candidates = records[:compact_cutoff]
        if len(compact_candidates) < 2:
            return session.archived_summary, session.summary_revision
        if len(compact_candidates) % 2 == 1:
            compact_candidates = compact_candidates[:-1]
        if len(compact_candidates) < 2:
            return session.archived_summary, session.summary_revision

        summary = self._build_compacted_summary_text(compact_candidates)
        if not summary:
            return session.archived_summary, session.summary_revision

        session.archived_summary = summary
        session.summary_updated_at = now_utc()
        session.last_compacted_message_id = compact_candidates[-1].id
        last_run_id = compact_candidates[-1].run_id
        session.last_compacted_run_id = int(last_run_id) if last_run_id else None
        session.summary_revision = int(session.summary_revision or 0) + 1
        db.add(session)
        return session.archived_summary, session.summary_revision

    def _compute_next_run_at(
        self,
        cron_expression: str | None,
        from_time: datetime | None = None,
    ) -> datetime | None:
        if not cron_expression:
            return None

        parts = cron_expression.strip().split()
        if len(parts) != 5:
            return None

        minute_expr, hour_expr, day_of_month_expr, month_expr, day_of_week_expr = parts
        try:
            minute_values = self._parse_cron_values(minute_expr, 0, 59)
            hour_values = self._parse_cron_values(hour_expr, 0, 23)
            day_of_month_values = self._parse_cron_values(day_of_month_expr, 1, 31)
            month_values = self._parse_cron_values(month_expr, 1, 12)
            day_of_week_values = self._parse_cron_values(
                day_of_week_expr,
                0,
                6,
                allow_seven_as_zero=True,
            )
        except ValueError:
            return None

        current_base = from_time or now_shanghai()
        if current_base.tzinfo is None:
            current_base = current_base.replace(tzinfo=SHANGHAI_TZ)
        else:
            current_base = current_base.astimezone(SHANGHAI_TZ)

        current = current_base.replace(second=0, microsecond=0) + timedelta(minutes=1)

        for _ in range(60 * 24 * 366 * 2):
            if not trading_calendar_service.is_trading_day(current.date()):
                next_day = trading_calendar_service.next_trading_day(current.date())
                current = datetime.combine(
                    next_day, datetime.min.time(), tzinfo=SHANGHAI_TZ
                )
                continue

            if (
                current.minute in minute_values
                and current.hour in hour_values
                and current.month in month_values
                and self._matches_cron_day(
                    current,
                    day_of_month_values=day_of_month_values,
                    day_of_week_values=day_of_week_values,
                    day_of_month_expr=day_of_month_expr,
                    day_of_week_expr=day_of_week_expr,
                )
            ):
                return current.astimezone(timezone.utc)
            current += timedelta(minutes=1)
        return None

    def _parse_cron_values(
        self,
        expression: str,
        minimum: int,
        maximum: int,
        *,
        allow_seven_as_zero: bool = False,
    ) -> set[int]:
        expr = expression.strip()
        allowed: set[int] = set()
        for part in expr.split(","):
            part = part.strip()
            if not part:
                raise ValueError("invalid cron expression")

            range_part = part
            step = 1
            if "/" in part:
                range_part, step_text = part.split("/", 1)
                step = int(step_text)
                if step <= 0:
                    raise ValueError("invalid cron step")

            if range_part == "*":
                start = minimum
                end = maximum
            elif "-" in range_part:
                start_text, end_text = range_part.split("-", 1)
                start = self._normalize_cron_value(
                    int(start_text),
                    minimum=minimum,
                    maximum=maximum,
                    allow_seven_as_zero=allow_seven_as_zero,
                )
                end = self._normalize_cron_value(
                    int(end_text),
                    minimum=minimum,
                    maximum=maximum,
                    allow_seven_as_zero=allow_seven_as_zero,
                )
                if start > end:
                    raise ValueError("invalid cron range")
            else:
                numeric = self._normalize_cron_value(
                    int(range_part),
                    minimum=minimum,
                    maximum=maximum,
                    allow_seven_as_zero=allow_seven_as_zero,
                )
                start = numeric
                end = numeric

            allowed.update(range(start, end + 1, step))

        return allowed

    def _normalize_cron_value(
        self,
        value: int,
        *,
        minimum: int,
        maximum: int,
        allow_seven_as_zero: bool = False,
    ) -> int:
        if allow_seven_as_zero and value == 7:
            value = 0
        if value < minimum or value > maximum:
            raise ValueError("cron value out of range")
        return value

    def _matches_cron_day(
        self,
        current: datetime,
        *,
        day_of_month_values: set[int],
        day_of_week_values: set[int],
        day_of_month_expr: str,
        day_of_week_expr: str,
    ) -> bool:
        day_of_month_matches = current.day in day_of_month_values
        current_day_of_week = (current.weekday() + 1) % 7
        day_of_week_matches = current_day_of_week in day_of_week_values

        day_of_month_is_wildcard = day_of_month_expr.strip() == "*"
        day_of_week_is_wildcard = day_of_week_expr.strip() == "*"

        if day_of_month_is_wildcard and day_of_week_is_wildcard:
            return True
        if day_of_month_is_wildcard:
            return day_of_week_matches
        if day_of_week_is_wildcard:
            return day_of_month_matches
        return day_of_month_matches or day_of_week_matches

    def _build_account_overview(
        self,
        balance_payload: dict[str, Any] | None,
        positions_payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        balance = (
            balance_payload.get("data") if isinstance(balance_payload, dict) else {}
        )
        positions_source = (
            positions_payload.get("data") if isinstance(positions_payload, dict) else []
        )
        if isinstance(positions_source, dict):
            rows = (
                positions_source.get("data")
                or positions_source.get("rows")
                or positions_source.get("list")
                or positions_source.get("posList")
                or []
            )
        else:
            rows = positions_source or []

        total_assets = None
        total_market_value = None
        holding_profit = None
        daily_profit = None
        daily_profit_trade_date = None
        open_date = None
        operating_days = None
        initial_capital = None
        cash_balance = None
        total_position_ratio = None
        nav = None
        if isinstance(balance, dict):
            open_date = _format_open_date(balance.get("openDate"))
            operating_days = int(_parse_float(balance.get("oprDays")) or 0) or None
            initial_capital = _parse_float(balance.get("initMoney"))
            total_assets = _parse_float(
                balance.get("totalAsset")
                or balance.get("totalAssets")
                or balance.get("asset")
                or balance.get("totalMoney")
                or (
                    (balance.get("result") or {}).get("totalAssets")
                    if isinstance(balance.get("result"), dict)
                    else None
                )
            )
            total_market_value = _parse_float(
                balance.get("marketValue")
                or balance.get("stockMarketValue")
                or balance.get("positionValue")
                or balance.get("totalPosValue")
            )
            cash_balance = _parse_float(
                balance.get("balanceActual")
                or balance.get("availBalance")
                or balance.get("cashBalance")
            )
            total_position_ratio = _normalize_percent(
                _parse_float(balance.get("totalPosPct"))
            )
            holding_profit = _parse_float(
                balance.get("holdingProfit")
                or balance.get("positionProfit")
                or balance.get("floatProfit")
                or balance.get("totalProfit")
            )
            nav = _parse_float(balance.get("nav"))
            daily_profit = _parse_float(
                balance.get("todayProfit")
                or balance.get("dailyProfit")
                or balance.get("profitToday")
            )
            raw_trade_date = (
                balance.get("tradeDate")
                or balance.get("tradingDate")
                or balance.get("date")
                or balance.get("profitDate")
            )
            if raw_trade_date:
                text = str(raw_trade_date).strip()
                if len(text) == 8 and text.isdigit():
                    daily_profit_trade_date = f"{text[:4]}-{text[4:6]}-{text[6:8]}"
                elif len(text) >= 10:
                    daily_profit_trade_date = text[:10]

        if holding_profit is None and isinstance(positions_source, dict):
            holding_profit = _parse_float(positions_source.get("totalProfit"))

        if daily_profit is None:
            daily_profit = sum(
                _parse_float(row.get("dayProfit")) or 0.0
                for row in rows
                if isinstance(row, dict)
            )

        total_return_ratio = None
        if nav is not None:
            total_return_ratio = nav - 1
        elif total_assets is not None and initial_capital not in (None, 0):
            total_return_ratio = total_assets / initial_capital - 1

        daily_return_ratio = None
        if daily_profit is not None and total_assets is not None:
            previous_assets = total_assets - daily_profit
            if previous_assets > 0:
                daily_return_ratio = daily_profit / previous_assets

        if daily_profit_trade_date is None:
            today = now_shanghai().date()
            if trading_calendar_service.is_trading_day(today):
                daily_profit_trade_date = today.isoformat()
            else:
                probe = today - timedelta(days=1)
                for _ in range(30):
                    if trading_calendar_service.is_trading_day(probe):
                        break
                    probe -= timedelta(days=1)
                daily_profit_trade_date = probe.isoformat()

        normalized_positions: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            amount = (
                _parse_float(
                    row.get("marketValue")
                    or row.get("market_amount")
                    or row.get("amount")
                    or row.get("positionValue")
                    or row.get("value")
                )
                or 0.0
            )
            profit_value = _parse_float(
                row.get("profitRatio")
                or row.get("profit_rate")
                or row.get("yieldRate")
                or row.get("profitPercent")
                or row.get("profitPct")
            )
            profit_ratio = _normalize_percent(profit_value)
            day_profit_ratio = _normalize_percent(_parse_float(row.get("dayProfitPct")))
            position_ratio = None
            if total_assets and total_assets > 0:
                position_ratio = max(0.0, min(1.0, amount / total_assets))
            if position_ratio is None:
                position_ratio = _normalize_percent(_parse_float(row.get("posPct")))

            raw_symbol = str(
                row.get("stockCode")
                or row.get("code")
                or row.get("SECURITY_CODE")
                or row.get("secCode")
                or ""
            ).strip()
            market_code = row.get("secMkt")
            if market_code is None:
                market_code = row.get("market")
            suffix = _market_suffix(market_code)
            symbol = f"{raw_symbol}.{suffix}" if raw_symbol and suffix else raw_symbol

            normalized_positions.append(
                {
                    "name": str(
                        row.get("stockName")
                        or row.get("name")
                        or row.get("SECURITY_SHORT_NAME")
                        or row.get("secName")
                        or ""
                    ).strip(),
                    "symbol": symbol,
                    "amount": amount,
                    "volume": int(_parse_float(row.get("count")) or 0),
                    "available_volume": int(_parse_float(row.get("availCount")) or 0),
                    "day_profit": _parse_float(row.get("dayProfit")),
                    "day_profit_ratio": day_profit_ratio,
                    "profit": _parse_float(row.get("profit")),
                    "profit_ratio": profit_ratio,
                    "profit_text": self._format_profit_text(profit_ratio),
                    "current_price": _scaled_decimal(
                        _coalesce(row.get("price"), row.get("currentPrice")),
                        _coalesce(row.get("priceDec"), row.get("priceDecimal")),
                    ),
                    "cost_price": _scaled_decimal(
                        _coalesce(row.get("costPrice"), row.get("cost_price")),
                        _coalesce(row.get("costPriceDec"), row.get("costPriceDecimal")),
                    ),
                    "position_ratio": position_ratio,
                }
            )

        normalized_positions.sort(key=lambda item: item["amount"], reverse=True)
        return {
            "open_date": open_date,
            "daily_profit_trade_date": daily_profit_trade_date,
            "operating_days": operating_days,
            "initial_capital": initial_capital,
            "total_assets": total_assets,
            "total_market_value": total_market_value,
            "cash_balance": cash_balance,
            "total_position_ratio": total_position_ratio,
            "holding_profit": holding_profit
            if holding_profit is not None
            else _parse_float(
                (positions_payload or {}).get("data", {}).get("totalProfit")
            )
            if isinstance((positions_payload or {}).get("data"), dict)
            else None,
            "daily_profit": daily_profit,
            "total_return_ratio": total_return_ratio,
            "nav": nav,
            "daily_return_ratio": daily_return_ratio,
            "positions": normalized_positions,
            "trade_summaries": [],
        }

    def _format_profit_text(self, profit_ratio: float | None) -> str:
        if profit_ratio is None:
            return "--"
        return f"{profit_ratio * 100:.2f}%"


aniu_service = AniuService()
