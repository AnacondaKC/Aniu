from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.db.models import Base

_engine = None
_session_local = None


def get_engine():
    global _engine
    if _engine is None:
        settings = get_settings()
        settings.sqlite_db_path.parent.mkdir(parents=True, exist_ok=True)
        _engine = create_engine(
            f"sqlite:///{settings.sqlite_db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
    return _engine


def get_session_local():
    global _session_local
    if _session_local is None:
        _session_local = sessionmaker(
            bind=get_engine(),
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
    return _session_local


def init_db() -> None:
    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    _ensure_app_settings_columns(engine)
    _ensure_chat_session_columns(engine)
    _ensure_chat_message_columns(engine)
    _ensure_strategy_schedule_columns(engine)
    _ensure_strategy_run_columns(engine)
    _ensure_chat_session_indexes(engine)
    _ensure_chat_message_indexes(engine)
    _ensure_strategy_run_indexes(engine)
    _backfill_schedule_run_types(engine)
    _backfill_strategy_run_types(engine)


def _ensure_app_settings_columns(engine) -> None:
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    if "app_settings" not in table_names:
        return

    columns = {column["name"] for column in inspector.get_columns("app_settings")}
    statements: list[str] = []
    if "mx_api_key" not in columns:
        statements.append("ALTER TABLE app_settings ADD COLUMN mx_api_key VARCHAR(255)")
    if "disabled_skill_ids_json" not in columns:
        statements.append(
            "ALTER TABLE app_settings ADD COLUMN disabled_skill_ids_json TEXT DEFAULT '[]'"
        )
    if "automation_session_id" not in columns:
        statements.append("ALTER TABLE app_settings ADD COLUMN automation_session_id INTEGER")
    if "automation_context_window_tokens" not in columns:
        statements.append(
            "ALTER TABLE app_settings ADD COLUMN automation_context_window_tokens INTEGER DEFAULT 65536"
        )
    if "automation_target_prompt_tokens" not in columns:
        statements.append(
            "ALTER TABLE app_settings ADD COLUMN automation_target_prompt_tokens INTEGER DEFAULT 24000"
        )
    if "automation_recent_message_limit" not in columns:
        statements.append(
            "ALTER TABLE app_settings ADD COLUMN automation_recent_message_limit INTEGER DEFAULT 24"
        )
    if "automation_enable_auto_compaction" not in columns:
        statements.append(
            "ALTER TABLE app_settings ADD COLUMN automation_enable_auto_compaction BOOLEAN DEFAULT 1"
        )
    if "automation_idle_summary_hours" not in columns:
        statements.append(
            "ALTER TABLE app_settings ADD COLUMN automation_idle_summary_hours INTEGER DEFAULT 12"
        )
    if "automation_context_source" not in columns:
        statements.append(
            "ALTER TABLE app_settings ADD COLUMN automation_context_source VARCHAR(32) DEFAULT 'default'"
        )
    if "automation_context_detected_at" not in columns:
        statements.append(
            "ALTER TABLE app_settings ADD COLUMN automation_context_detected_at DATETIME"
        )

    if not statements:
        return

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
        connection.execute(
            text(
                "UPDATE app_settings SET automation_context_window_tokens = COALESCE(automation_context_window_tokens, 65536), "
                "automation_target_prompt_tokens = COALESCE(automation_target_prompt_tokens, 24000), "
                "automation_recent_message_limit = COALESCE(automation_recent_message_limit, 24), "
                "automation_enable_auto_compaction = COALESCE(automation_enable_auto_compaction, 1), "
                "automation_idle_summary_hours = COALESCE(automation_idle_summary_hours, 12), "
                "automation_context_source = COALESCE(NULLIF(trim(automation_context_source), ''), 'default')"
            )
        )


def _ensure_chat_session_columns(engine) -> None:
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    if "chat_sessions" not in table_names:
        return

    required_columns = {
        "kind": "ALTER TABLE chat_sessions ADD COLUMN kind VARCHAR(32) DEFAULT 'user'",
        "slug": "ALTER TABLE chat_sessions ADD COLUMN slug VARCHAR(120)",
        "archived_summary": "ALTER TABLE chat_sessions ADD COLUMN archived_summary TEXT",
        "summary_updated_at": "ALTER TABLE chat_sessions ADD COLUMN summary_updated_at DATETIME",
        "last_compacted_message_id": "ALTER TABLE chat_sessions ADD COLUMN last_compacted_message_id INTEGER",
        "last_compacted_run_id": "ALTER TABLE chat_sessions ADD COLUMN last_compacted_run_id INTEGER",
        "summary_revision": "ALTER TABLE chat_sessions ADD COLUMN summary_revision INTEGER DEFAULT 0",
    }

    with engine.begin() as connection:
        for column_name, statement in required_columns.items():
            current_columns = {
                column["name"]
                for column in inspect(connection).get_columns("chat_sessions")
            }
            if column_name in current_columns:
                continue
            connection.execute(text(statement))
        connection.execute(
            text(
                "UPDATE chat_sessions SET kind = COALESCE(NULLIF(trim(kind), ''), 'user'), "
                "summary_revision = COALESCE(summary_revision, 0)"
            )
        )


def _ensure_chat_message_columns(engine) -> None:
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    if "chat_messages" not in table_names:
        return

    required_columns = {
        "source": "ALTER TABLE chat_messages ADD COLUMN source VARCHAR(32)",
        "run_id": "ALTER TABLE chat_messages ADD COLUMN run_id INTEGER",
        "message_kind": "ALTER TABLE chat_messages ADD COLUMN message_kind VARCHAR(32)",
        "meta_payload": "ALTER TABLE chat_messages ADD COLUMN meta_payload JSON",
    }

    with engine.begin() as connection:
        for column_name, statement in required_columns.items():
            current_columns = {
                column["name"]
                for column in inspect(connection).get_columns("chat_messages")
            }
            if column_name in current_columns:
                continue
            connection.execute(text(statement))


def _ensure_strategy_schedule_columns(engine) -> None:
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    if "strategy_schedules" not in table_names:
        return

    required_columns = {
        "run_type": "ALTER TABLE strategy_schedules ADD COLUMN run_type VARCHAR(32) DEFAULT 'analysis'",
        "cron_expression": "ALTER TABLE strategy_schedules ADD COLUMN cron_expression VARCHAR(64)",
        "task_prompt": "ALTER TABLE strategy_schedules ADD COLUMN task_prompt TEXT",
        "timeout_seconds": "ALTER TABLE strategy_schedules ADD COLUMN timeout_seconds INTEGER DEFAULT 1800",
        "retry_count": "ALTER TABLE strategy_schedules ADD COLUMN retry_count INTEGER DEFAULT 0",
        "retry_after_at": "ALTER TABLE strategy_schedules ADD COLUMN retry_after_at DATETIME",
    }

    with engine.begin() as connection:
        for column_name, statement in required_columns.items():
            current_columns = {
                column["name"]
                for column in inspect(connection).get_columns("strategy_schedules")
            }
            if column_name in current_columns:
                continue
            connection.execute(text(statement))


def _backfill_schedule_run_types(engine) -> None:
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    if "strategy_schedules" not in table_names:
        return

    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE strategy_schedules SET run_type = 'trade' "
                "WHERE name LIKE '上午运行%' OR name LIKE '下午运行%'"
            )
        )
        connection.execute(
            text(
                "UPDATE strategy_schedules SET run_type = 'analysis' "
                "WHERE run_type IS NULL OR trim(run_type) = '' OR name IN ('盘前分析', '午间复盘', '收盘分析', '默认任务')"
            )
        )


def _backfill_strategy_run_types(engine) -> None:
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    if "strategy_runs" not in table_names:
        return

    db_path = Path(get_settings().sqlite_db_path)
    if not db_path.exists():
        return

    import json
    import sqlite3

    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        runs = connection.execute(
            "SELECT id, run_type, schedule_name, executed_actions, skill_payloads, decision_payload FROM strategy_runs"
        ).fetchall()
        trade_order_counts = {
            int(row[0]): int(row[1])
            for row in connection.execute(
                "SELECT run_id, COUNT(*) FROM trade_orders GROUP BY run_id"
            ).fetchall()
        }

        for row in runs:
            schedule_name = str(row["schedule_name"] or "").strip()
            stored_run_type = str(row["run_type"] or "").strip()
            inferred = "analysis"

            if schedule_name.startswith("上午运行") or schedule_name.startswith("下午运行"):
                inferred = "trade"
            elif schedule_name in {"盘前分析", "午间复盘", "收盘分析"}:
                inferred = "analysis"
            elif trade_order_counts.get(int(row["id"]), 0) > 0:
                inferred = "trade"
            else:
                executed_actions = []
                if row["executed_actions"]:
                    try:
                        parsed_actions = json.loads(row["executed_actions"])
                        if isinstance(parsed_actions, list):
                            executed_actions = [item for item in parsed_actions if isinstance(item, dict)]
                    except Exception:
                        executed_actions = []

                if any(str(item.get("action") or "").upper() in {"BUY", "SELL", "CANCEL"} for item in executed_actions):
                    inferred = "trade"
                else:
                    tool_calls: list[dict[str, object]] = []
                    for payload_key in ("skill_payloads", "decision_payload"):
                        raw_payload = row[payload_key]
                        if not raw_payload:
                            continue
                        try:
                            parsed_payload = json.loads(raw_payload)
                        except Exception:
                            continue
                        if not isinstance(parsed_payload, dict):
                            continue
                        payload_tool_calls = parsed_payload.get("tool_calls")
                        if isinstance(payload_tool_calls, list):
                            tool_calls = [item for item in payload_tool_calls if isinstance(item, dict)]
                            if tool_calls:
                                break

                    if any(str(item.get("name") or "") in {"mx_moni_trade", "mx_moni_cancel"} for item in tool_calls):
                        inferred = "trade"
                    elif stored_run_type in {"analysis", "trade"}:
                        inferred = stored_run_type

            connection.execute(
                "UPDATE strategy_runs SET run_type = ? WHERE id = ?",
                (inferred, int(row["id"])),
            )

        connection.commit()
    finally:
        connection.close()


def _ensure_strategy_run_columns(engine) -> None:
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    if "strategy_runs" not in table_names:
        return

    required_columns = {
        "final_answer": "ALTER TABLE strategy_runs ADD COLUMN final_answer TEXT",
        "run_type": "ALTER TABLE strategy_runs ADD COLUMN run_type VARCHAR(32) DEFAULT 'analysis'",
        "schedule_name": "ALTER TABLE strategy_runs ADD COLUMN schedule_name VARCHAR(64)",
        "schedule_id": "ALTER TABLE strategy_runs ADD COLUMN schedule_id INTEGER",
        "chat_session_id": "ALTER TABLE strategy_runs ADD COLUMN chat_session_id INTEGER",
        "prompt_message_id": "ALTER TABLE strategy_runs ADD COLUMN prompt_message_id INTEGER",
        "response_message_id": "ALTER TABLE strategy_runs ADD COLUMN response_message_id INTEGER",
        "context_summary_version": "ALTER TABLE strategy_runs ADD COLUMN context_summary_version INTEGER",
        "context_tokens_estimate": "ALTER TABLE strategy_runs ADD COLUMN context_tokens_estimate INTEGER",
    }

    with engine.begin() as connection:
        for column_name, statement in required_columns.items():
            current_columns = {
                column["name"]
                for column in inspect(connection).get_columns("strategy_runs")
            }
            if column_name in current_columns:
                continue
            connection.execute(text(statement))


def _ensure_strategy_run_indexes(engine) -> None:
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    if "strategy_runs" not in table_names:
        return

    index_names = {
        index["name"]
        for index in inspector.get_indexes("strategy_runs")
        if index.get("name")
    }

    statements: list[str] = []
    if "ix_strategy_runs_started_at" not in index_names:
        statements.append(
            "CREATE INDEX ix_strategy_runs_started_at ON strategy_runs (started_at)"
        )
    if "ix_strategy_runs_chat_session_id" not in index_names:
        statements.append(
            "CREATE INDEX ix_strategy_runs_chat_session_id ON strategy_runs (chat_session_id)"
        )
    if "ix_strategy_runs_schedule_id" not in index_names:
        statements.append(
            "CREATE INDEX ix_strategy_runs_schedule_id ON strategy_runs (schedule_id)"
        )

    if not statements:
        return

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def _ensure_chat_session_indexes(engine) -> None:
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    if "chat_sessions" not in table_names:
        return

    index_names = {
        index["name"]
        for index in inspector.get_indexes("chat_sessions")
        if index.get("name")
    }

    statements: list[str] = []
    if "ix_chat_sessions_kind" not in index_names:
        statements.append("CREATE INDEX ix_chat_sessions_kind ON chat_sessions (kind)")
    if "ix_chat_sessions_slug" not in index_names:
        statements.append("CREATE INDEX ix_chat_sessions_slug ON chat_sessions (slug)")

    if not statements:
        return

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def _ensure_chat_message_indexes(engine) -> None:
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    if "chat_messages" not in table_names:
        return

    index_names = {
        index["name"]
        for index in inspector.get_indexes("chat_messages")
        if index.get("name")
    }

    statements: list[str] = []
    if "ix_chat_messages_run_id" not in index_names:
        statements.append("CREATE INDEX ix_chat_messages_run_id ON chat_messages (run_id)")

    if not statements:
        return

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def get_db() -> Generator[Session, None, None]:
    session = get_session_local()()
    try:
        yield session
    finally:
        session.close()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    session = get_session_local()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
