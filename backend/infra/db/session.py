"""Async database engine and session helpers."""

# ruff: noqa: E501

from __future__ import annotations

import os
from contextlib import suppress
from pathlib import Path

from sqlalchemy import Connection, event, inspect, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from backend.infra.db.migrations import upgrade_two_stage_pipeline
from backend.infra.runtime_paths import default_database_path

DEFAULT_SQLITE_PATH = default_database_path()


def build_database_url(sqlite_path: Path | None = None) -> str:
    """Build the database URL used by the app.

    ``ANIU_DATABASE_URL`` is useful for an isolated development database, so
    experimental reloads never contend with or mutate the normal local data.
    An explicit ``sqlite_path`` remains the highest-priority test override.
    """

    if sqlite_path is None:
        configured_url = os.getenv("ANIU_DATABASE_URL")
        if configured_url:
            return configured_url
        sqlite_path = default_database_path()
    sqlite_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with suppress(OSError):
        sqlite_path.parent.chmod(0o700)
    return f"sqlite+aiosqlite:///{sqlite_path}"


def create_engine(
    database_url: str | None = None,
    *,
    echo: bool = False,
) -> AsyncEngine:
    """Create an async SQLAlchemy engine.

    For SQLite file databases (the default local store) we apply connection
    pragmas tuned for concurrent read/write under a single writer:

    * ``journal_mode=WAL`` unlocks concurrent readers while one writer holds
      the write lock, removed the ``database is locked`` retry storms that the
      default ``DELETE`` journal mode produces under load.
    * ``busy_timeout=30000`` makes writers wait up to 30s for the lock instead
      of failing immediately, mirroring the maximum expected run duration.
    * ``synchronous=NORMAL`` keeps durability good enough for local trading
      data while halving fsync pressure per commit.
    * ``foreign_keys=ON`` enforces referential integrity at the driver level.

    Non-SQLite URLs are left untouched.  In-memory SQLite keeps foreign-key
    enforcement, while file-only concurrency pragmas are skipped.
    """

    engine = create_async_engine(database_url or build_database_url(), echo=echo)

    @event.listens_for(engine.sync_engine, "connect")
    def _apply_sqlite_pragmas(
        dbapi_connection: object,
        _connection_record: object,
    ) -> None:
        if engine.dialect.name != "sqlite":
            return

        # SQLite-only pragmas must never be sent to other dialects.  Keep
        # foreign-key enforcement for in-memory fixtures, but file-specific
        # concurrency pragmas do not apply to an in-memory database.
        cursor = dbapi_connection.cursor()  # type: ignore[attr-defined]
        try:
            if engine.url.database not in {None, ":memory:"}:
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
                cursor.execute("PRAGMA busy_timeout=30000")
            cursor.execute("PRAGMA foreign_keys=ON")
        finally:
            cursor.close()

    return engine


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create the async session factory for the given engine."""

    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


_REMOVED_SQLITE_TABLES = frozenset({"skills", "skill_stage_bindings"})


def _reject_removed_sqlite_tables(connection: Connection) -> None:
    """Fail clearly when a local database still has removed Skill tables."""

    existing_tables = set(inspect(connection).get_table_names())
    leftovers = sorted(_REMOVED_SQLITE_TABLES & existing_tables)
    if leftovers:
        raise RuntimeError(
            "SQLite database contains removed tables: "
            + ", ".join(leftovers)
            + ". Rebuild the local database according to README.md."
        )


def _upgrade_memory_schema(connection: Connection) -> None:
    """Migrate the original structured memory tables to the simple schema."""

    tables = set(inspect(connection).get_table_names())
    if "memory_items" not in tables or "memory_activities" not in tables:
        return
    item_columns = {
        column["name"] for column in inspect(connection).get_columns("memory_items")
    }
    activity_columns = {
        column["name"]
        for column in inspect(connection).get_columns("memory_activities")
    }
    if {
        "content",
        "reason",
        "created_task_id",
        "updated_task_id",
        "version",
        "deleted_at",
    }.issubset(item_columns) and {"operation", "content"}.issubset(activity_columns):
        return

    connection.execute(text("DROP TABLE IF EXISTS memory_items_simple"))
    connection.execute(text("DROP TABLE IF EXISTS memory_activities_simple"))
    connection.execute(
        text(
            """
            CREATE TABLE memory_items_simple (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                created_task_id INTEGER NOT NULL,
                updated_task_id INTEGER NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                deleted_at TEXT
            )
            """
        )
    )
    connection.execute(
        text(
            """
            INSERT INTO memory_items_simple (
                id, content, reason, created_task_id, updated_task_id, version,
                created_at, updated_at, deleted_at
            )
            SELECT
                id,
                TRIM(
                    CASE WHEN TRIM(COALESCE(title, '')) <> ''
                        THEN title || char(10) ELSE '' END
                    || CASE WHEN TRIM(COALESCE(lesson, '')) <> ''
                        THEN lesson ELSE COALESCE(condition, '') END
                ),
                TRIM(
                    CASE WHEN TRIM(COALESCE(condition, '')) <> ''
                        THEN '适用条件：' || condition || char(10) ELSE '' END
                    || CASE WHEN TRIM(COALESCE(recommended_action, '')) <> ''
                        THEN '建议行动：' || recommended_action || char(10) ELSE '' END
                    || CASE WHEN TRIM(COALESCE(invalid_when, '')) <> ''
                        THEN '失效条件：' || invalid_when ELSE '' END
                ),
                created_run_id,
                updated_run_id,
                version,
                created_at,
                updated_at,
                CASE WHEN status IN ('retired', 'superseded') THEN updated_at END
            FROM memory_items
            """
        )
    )
    connection.execute(
        text(
            """
            CREATE TABLE memory_activities_simple (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation VARCHAR(16) NOT NULL,
                memory_id INTEGER,
                content TEXT NOT NULL DEFAULT '',
                result_count INTEGER,
                created_at TEXT NOT NULL
            )
            """
        )
    )
    connection.execute(
        text(
            """
            INSERT INTO memory_activities_simple (
                id, operation, memory_id, content, result_count, created_at
            )
            SELECT
                id,
                CASE
                    WHEN action = 'read' THEN 'read'
                    WHEN operation = 'create' THEN 'create'
                    WHEN operation = 'retire' THEN 'delete'
                    ELSE 'update'
                END,
                memory_id,
                summary,
                result_count,
                created_at
            FROM memory_activities
            """
        )
    )
    for index_name in (
        "idx_memory_items_status_updated",
        "idx_memory_items_kind_updated",
        "idx_memory_activities_created",
    ):
        connection.execute(text(f"DROP INDEX IF EXISTS {index_name}"))
    connection.execute(text("DROP TABLE IF EXISTS memory_evidence"))
    connection.execute(text("DROP TABLE IF EXISTS memory_revisions"))
    connection.execute(text("DROP TABLE memory_activities"))
    connection.execute(text("DROP TABLE memory_items"))
    connection.execute(text("ALTER TABLE memory_items_simple RENAME TO memory_items"))
    connection.execute(
        text("ALTER TABLE memory_activities_simple RENAME TO memory_activities")
    )
    connection.execute(
        text("CREATE INDEX idx_memory_items_updated ON memory_items (updated_at)")
    )
    connection.execute(
        text(
            "CREATE INDEX idx_memory_activities_created "
            "ON memory_activities (created_at)"
        )
    )


def _upgrade_sqlite_schema(connection: Connection) -> None:
    """Apply additive migrations required by persisted local databases."""

    _upgrade_memory_schema(connection)

    activity_columns = {
        column["name"]
        for column in inspect(connection).get_columns("memory_activities")
    }
    if "task_id" not in activity_columns:
        connection.execute(
            text(
                "ALTER TABLE memory_activities ADD COLUMN task_id INTEGER NOT NULL DEFAULT 0"
            )
        )

    run_job_columns = {
        column["name"] for column in inspect(connection).get_columns("run_jobs")
    }
    if "claim_token" not in run_job_columns:
        connection.execute(
            text("ALTER TABLE run_jobs ADD COLUMN claim_token VARCHAR(64)")
        )

    strategy_run_columns = {
        column["name"] for column in inspect(connection).get_columns("strategy_runs")
    }
    if "failure_reason" not in strategy_run_columns:
        connection.execute(
            text("ALTER TABLE strategy_runs ADD COLUMN failure_reason TEXT")
        )
    if "summary_render_mode" not in strategy_run_columns:
        connection.execute(
            text(
                "ALTER TABLE strategy_runs ADD COLUMN summary_render_mode "
                "VARCHAR(16) NOT NULL DEFAULT 'markdown'"
            )
        )

    selected_model_columns = {
        column["name"]
        for column in inspect(connection).get_columns("channel_selected_models")
    }
    selected_model_additions = {
        "context_window_tokens": "INTEGER",
        "max_output_tokens": "INTEGER",
        "input_price_per_million": "REAL",
        "output_price_per_million": "REAL",
        "cache_read_price_per_million": "REAL",
        "cache_write_price_per_million": "REAL",
        "thinking_efforts_json": "JSON NOT NULL DEFAULT '[]'",
    }
    for column_name, column_type in selected_model_additions.items():
        if column_name not in selected_model_columns:
            connection.execute(
                text(
                    f"ALTER TABLE channel_selected_models "
                    f"ADD COLUMN {column_name} {column_type}"
                )
            )

    app_settings_columns = {
        column["name"] for column in inspect(connection).get_columns("app_settings")
    }
    if "mx_daily_limits_json" in app_settings_columns:
        connection.execute(
            text("ALTER TABLE app_settings DROP COLUMN mx_daily_limits_json")
        )
    if "decision_max_rollbacks" in app_settings_columns:
        connection.execute(
            text("ALTER TABLE app_settings DROP COLUMN decision_max_rollbacks")
        )
    if "max_tool_loop_iterations" in app_settings_columns:
        connection.execute(
            text("ALTER TABLE app_settings DROP COLUMN max_tool_loop_iterations")
        )
    if "dream_schedule_time" not in app_settings_columns:
        connection.execute(
            text(
                "ALTER TABLE app_settings "
                "ADD COLUMN dream_schedule_time VARCHAR(5) NOT NULL DEFAULT '00:30'"
            )
        )

    upgrade_two_stage_pipeline(connection)

    connection.execute(text("DROP TABLE IF EXISTS stock_api_call_usage_scopes"))

    schedule_columns = {
        column["name"]
        for column in inspect(connection).get_columns("strategy_schedules")
    }
    if "custom_schedule_times_json" not in schedule_columns:
        connection.execute(
            text(
                "ALTER TABLE strategy_schedules "
                "ADD COLUMN custom_schedule_times_json TEXT"
            )
        )

    position_cache_columns = {
        column["name"]
        for column in inspect(connection).get_columns("account_positions_cache")
    }
    if "day_profit" not in position_cache_columns:
        connection.execute(
            text("ALTER TABLE account_positions_cache ADD COLUMN day_profit REAL")
        )
    if "available_quantity" not in position_cache_columns:
        connection.execute(
            text(
                "ALTER TABLE account_positions_cache "
                "ADD COLUMN available_quantity INTEGER"
            )
        )

    _ensure_stock_api_log_schema(connection)


def _ensure_stock_api_log_schema(connection: Connection) -> None:
    expected_columns = {
        "id",
        "source",
        "provider",
        "operation_id",
        "parameters_json",
        "status",
        "duration_ms",
        "response_characters",
        "error_category",
        "error_message",
        "created_at",
    }
    columns = {
        column["name"]
        for column in inspect(connection).get_columns("stock_api_call_logs")
    }
    if columns != expected_columns:
        _rebuild_stock_api_log_schema(connection, columns)

    connection.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_stock_api_call_logs_provider_operation "
            "ON stock_api_call_logs (provider, operation_id, created_at)"
        )
    )
    connection.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_stock_api_call_logs_status "
            "ON stock_api_call_logs (status, created_at)"
        )
    )
    connection.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_stock_api_call_logs_created "
            "ON stock_api_call_logs (created_at)"
        )
    )


def _rebuild_stock_api_log_schema(
    connection: Connection,
    legacy_columns: set[str],
) -> None:
    for index_name in (
        "idx_stock_api_call_logs_credential_operation",
        "idx_stock_api_call_logs_provider_operation",
        "idx_stock_api_call_logs_status",
        "idx_stock_api_call_logs_created",
    ):
        connection.execute(text(f"DROP INDEX IF EXISTS {index_name}"))
    connection.execute(text("DROP TABLE IF EXISTS stock_api_call_usage_scopes"))
    connection.execute(text("DROP TABLE IF EXISTS stock_api_call_logs_legacy"))
    connection.execute(
        text("ALTER TABLE stock_api_call_logs RENAME TO stock_api_call_logs_legacy")
    )
    connection.execute(
        text(
            """
            CREATE TABLE stock_api_call_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source VARCHAR(32) NOT NULL DEFAULT 'unknown',
                provider VARCHAR(32) NOT NULL,
                operation_id VARCHAR(128) NOT NULL,
                parameters_json TEXT NOT NULL,
                status VARCHAR(32) NOT NULL,
                duration_ms INTEGER NOT NULL,
                response_characters INTEGER,
                error_category VARCHAR(32),
                error_message TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
    )

    source = (
        "COALESCE(source, 'unknown')" if "source" in legacy_columns else "'unknown'"
    )
    response_characters = (
        "response_characters" if "response_characters" in legacy_columns else "NULL"
    )
    error_category = (
        "CASE WHEN status = 'success' THEN NULL ELSE COALESCE(error_category, 'unknown') END"
        if "error_category" in legacy_columns
        else "CASE WHEN status = 'success' THEN NULL ELSE 'unknown' END"
    )
    connection.execute(
        text(
            "INSERT INTO stock_api_call_logs "
            "(id, source, provider, operation_id, parameters_json, status, "
            "duration_ms, response_characters, error_category, error_message, created_at) "
            "SELECT id, "
            f"{source}, provider, operation_id, parameters_json, status, "
            f"duration_ms, {response_characters}, {error_category}, error_message, created_at "
            "FROM stock_api_call_logs_legacy"
        )
    )
    connection.execute(text("DROP TABLE stock_api_call_logs_legacy"))


async def init_db(engine: AsyncEngine) -> None:
    """Create the current schema and upgrade persisted SQLite databases."""

    from backend.infra.db.models import Base

    async with engine.begin() as connection:
        if engine.url.get_backend_name() == "sqlite":
            await connection.run_sync(_reject_removed_sqlite_tables)
        await connection.run_sync(Base.metadata.create_all)
        if engine.url.get_backend_name() == "sqlite":
            await connection.run_sync(_upgrade_sqlite_schema)
    if engine.url.get_backend_name() == "sqlite" and engine.url.database:
        database_path = Path(engine.url.database)
        if database_path.exists() and database_path.name != ":memory:":
            with suppress(OSError):
                database_path.chmod(0o600)
