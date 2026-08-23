"""Tests for database URL selection."""

from __future__ import annotations

import json
import sqlite3

import pytest

from backend.business.runs.trace_metrics import metrics_from_trace_payload
from backend.infra.db.session import build_database_url, create_engine, init_db


def test_build_database_url_uses_configured_development_database(
    monkeypatch,
) -> None:
    configured_url = "sqlite+aiosqlite:////tmp/aniu-isolated-dev.sqlite3"
    monkeypatch.setenv("ANIU_DATABASE_URL", configured_url)

    assert build_database_url() == configured_url


def test_explicit_sqlite_path_overrides_configured_database(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("ANIU_DATABASE_URL", "sqlite+aiosqlite:////tmp/other.sqlite3")
    sqlite_path = tmp_path / "test.sqlite3"

    assert build_database_url(sqlite_path) == f"sqlite+aiosqlite:///{sqlite_path}"
    assert sqlite_path.parent.exists()
    assert sqlite_path.parent.stat().st_mode & 0o777 == 0o700


@pytest.mark.asyncio
async def test_init_db_restricts_sqlite_file_permissions(tmp_path) -> None:
    sqlite_path = tmp_path / "private" / "aniu.sqlite3"
    engine = create_engine(build_database_url(sqlite_path))
    try:
        await init_db(engine)
        await init_db(engine)
    finally:
        await engine.dispose()

    assert sqlite_path.exists()
    assert sqlite_path.stat().st_mode & 0o777 == 0o600
    connection = sqlite3.connect(sqlite_path)
    try:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    finally:
        connection.close()
    assert {
        "app_settings",
        "strategy_runs",
        "run_jobs",
    } <= tables
    assert "alembic_version" not in tables
    assert "run_artifacts" not in tables


@pytest.mark.asyncio
async def test_init_db_removes_unconsumed_tool_results_from_existing_traces(
    tmp_path,
) -> None:
    sqlite_path = tmp_path / "trace-migration.sqlite3"
    engine = create_engine(build_database_url(sqlite_path))
    await init_db(engine)
    await engine.dispose()

    trace = {
        "stages": [
            {
                "steps": [
                    {
                        "type": "tool",
                        "data": {
                            "arguments": {"result": "keep argument"},
                            "result": {"large": "drop tool payload"},
                        },
                    },
                    {
                        "type": "result",
                        "data": {"result": "keep non-tool payload"},
                    },
                ]
            }
        ]
    }
    expected_metrics = metrics_from_trace_payload(trace)
    connection = sqlite3.connect(sqlite_path)
    try:
        connection.execute(
            """
            INSERT INTO strategy_runs (
                id, trigger_source, status, current_state, snapshot_json,
                trace_json, summary_render_mode, tool_calls_count,
                thinking_count, total_tokens, trade_count, started_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "manual",
                "COMPLETED",
                "Completed",
                "{}",
                json.dumps(trace, ensure_ascii=False),
                "markdown",
                *expected_metrics,
                "2026-08-22T00:00:00+00:00",
            ),
        )
        connection.execute("PRAGMA user_version = 1")
        connection.commit()
    finally:
        connection.close()

    migrated_engine = create_engine(build_database_url(sqlite_path))
    await init_db(migrated_engine)
    await migrated_engine.dispose()

    connection = sqlite3.connect(sqlite_path)
    try:
        row = connection.execute(
            "SELECT trace_json, tool_calls_count, thinking_count, "
            "total_tokens, trade_count "
            "FROM strategy_runs WHERE id = 1"
        ).fetchone()
        assert row is not None
        stored = json.loads(row[0])
        stored_metrics = tuple(int(value) for value in row[1:])
        version = connection.execute("PRAGMA user_version").fetchone()[0]
    finally:
        connection.close()

    tool_data = stored["stages"][0]["steps"][0]["data"]
    result_data = stored["stages"][0]["steps"][1]["data"]
    assert "result" not in tool_data
    assert tool_data["model_content_characters"] == len(
        json.dumps(
            {"large": "drop tool payload"},
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )
    assert tool_data["arguments"]["result"] == "keep argument"
    assert result_data["result"] == "keep non-tool payload"
    assert stored_metrics == metrics_from_trace_payload(stored) == expected_metrics
    assert version == 3


@pytest.mark.asyncio
async def test_init_db_backfills_metrics_for_existing_v2_traces(tmp_path) -> None:
    sqlite_path = tmp_path / "trace-metrics-v2.sqlite3"
    engine = create_engine(build_database_url(sqlite_path))
    await init_db(engine)
    await engine.dispose()

    trace = {
        "stages": [
            {
                "key": "run",
                "status": "completed",
                "steps": [
                    {
                        "type": "tool",
                        "data": {
                            "arguments": {"symbol": "600000"},
                            "model_content_characters": 12,
                        },
                    },
                    {
                        "type": "result",
                        "content": "完成交易",
                        "data": {"trade_count": 3},
                    },
                ],
            }
        ]
    }
    expected_metrics = metrics_from_trace_payload(trace)
    connection = sqlite3.connect(sqlite_path)
    try:
        connection.execute(
            """
            INSERT INTO strategy_runs (
                id, trigger_source, status, current_state, snapshot_json,
                trace_json, summary_render_mode, tool_calls_count,
                thinking_count, total_tokens, trade_count, started_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                2,
                "manual",
                "COMPLETED",
                "Completed",
                "{}",
                json.dumps(trace, ensure_ascii=False),
                "markdown",
                99,
                99,
                99,
                99,
                "2026-08-22T00:00:00+00:00",
            ),
        )
        connection.execute("PRAGMA user_version = 2")
        connection.commit()
    finally:
        connection.close()

    migrated_engine = create_engine(build_database_url(sqlite_path))
    await init_db(migrated_engine)
    await migrated_engine.dispose()

    connection = sqlite3.connect(sqlite_path)
    try:
        row = connection.execute(
            "SELECT tool_calls_count, thinking_count, total_tokens, trade_count "
            "FROM strategy_runs WHERE id = 2"
        ).fetchone()
        version = connection.execute("PRAGMA user_version").fetchone()[0]
    finally:
        connection.close()

    assert row is not None
    assert tuple(int(value) for value in row) == expected_metrics
    assert version == 3


@pytest.mark.asyncio
async def test_init_db_rejects_removed_skill_tables(tmp_path) -> None:
    sqlite_path = tmp_path / "legacy-skills.sqlite3"
    connection = sqlite3.connect(sqlite_path)
    try:
        connection.execute("CREATE TABLE skills (id INTEGER PRIMARY KEY)")
        connection.execute(
            "CREATE TABLE skill_stage_bindings (skill_id INTEGER PRIMARY KEY)"
        )
        connection.commit()
    finally:
        connection.close()

    engine = create_engine(build_database_url(sqlite_path))
    try:
        with pytest.raises(RuntimeError, match="skills"):
            await init_db(engine)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_init_db_adds_run_job_claim_token_to_existing_sqlite_database(
    tmp_path,
) -> None:
    sqlite_path = tmp_path / "existing-run-jobs.sqlite3"
    connection = sqlite3.connect(sqlite_path)
    try:
        connection.execute(
            """
            CREATE TABLE strategy_runs (
                id INTEGER PRIMARY KEY,
                status VARCHAR(32) NOT NULL,
                started_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE run_jobs (
                run_id INTEGER PRIMARY KEY,
                status VARCHAR(32) NOT NULL,
                attempt INTEGER NOT NULL,
                worker_id VARCHAR(64),
                lease_expires_at TEXT,
                heartbeat_at TEXT,
                available_at TEXT NOT NULL,
                last_error_code VARCHAR(64),
                last_error_message_preview TEXT,
                cancel_reason TEXT,
                active_guard INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.commit()
    finally:
        connection.close()

    engine = create_engine(build_database_url(sqlite_path))
    try:
        await init_db(engine)
        await init_db(engine)
    finally:
        await engine.dispose()

    connection = sqlite3.connect(sqlite_path)
    try:
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(run_jobs)").fetchall()
        }
        run_columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(strategy_runs)").fetchall()
        }
    finally:
        connection.close()

    assert "claim_token" in columns
    assert "failure_reason" in run_columns


@pytest.mark.asyncio
async def test_init_db_adds_schedule_and_position_columns_to_existing_database(
    tmp_path,
) -> None:
    sqlite_path = tmp_path / "existing-schedule-cache.sqlite3"
    connection = sqlite3.connect(sqlite_path)
    try:
        connection.execute(
            """
            CREATE TABLE strategy_schedules (
                id INTEGER PRIMARY KEY,
                task_type VARCHAR(32) NOT NULL,
                enabled INTEGER NOT NULL,
                interval_minutes INTEGER NOT NULL,
                revision INTEGER NOT NULL,
                runtime_synced_revision INTEGER NOT NULL,
                sync_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE account_positions_cache (
                id INTEGER PRIMARY KEY,
                account_snapshot_id INTEGER NOT NULL,
                symbol VARCHAR(32) NOT NULL,
                stock_name TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                avg_cost REAL NOT NULL,
                current_price REAL NOT NULL,
                market_value REAL NOT NULL,
                profit_ratio REAL NOT NULL,
                captured_at TEXT NOT NULL
            )
            """
        )
        connection.commit()
    finally:
        connection.close()

    engine = create_engine(build_database_url(sqlite_path))
    try:
        await init_db(engine)
        await init_db(engine)
    finally:
        await engine.dispose()

    connection = sqlite3.connect(sqlite_path)
    try:
        schedule_columns = {
            str(row[1])
            for row in connection.execute(
                "PRAGMA table_info(strategy_schedules)"
            ).fetchall()
        }
        position_columns = {
            str(row[1])
            for row in connection.execute(
                "PRAGMA table_info(account_positions_cache)"
            ).fetchall()
        }
    finally:
        connection.close()

    assert "custom_schedule_times_json" in schedule_columns
    assert "day_profit" in position_columns
    assert "available_quantity" in position_columns


@pytest.mark.asyncio
async def test_init_db_rebuilds_stock_api_logs_to_display_schema(
    tmp_path,
) -> None:
    sqlite_path = tmp_path / "existing-stock-api-logs.sqlite3"
    connection = sqlite3.connect(sqlite_path)
    try:
        connection.execute(
            """
            CREATE TABLE stock_api_call_logs (
                id INTEGER PRIMARY KEY,
                provider VARCHAR(32) NOT NULL,
                operation_id VARCHAR(128) NOT NULL,
                credential_fingerprint VARCHAR(64),
                endpoint TEXT NOT NULL,
                method VARCHAR(16) NOT NULL,
                parameters_json TEXT NOT NULL,
                status VARCHAR(32) NOT NULL,
                status_code INTEGER,
                duration_ms INTEGER NOT NULL,
                error_message TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.commit()
    finally:
        connection.close()

    engine = create_engine(build_database_url(sqlite_path))
    try:
        await init_db(engine)
        await init_db(engine)
    finally:
        await engine.dispose()

    connection = sqlite3.connect(sqlite_path)
    try:
        columns = {
            str(row[1])
            for row in connection.execute(
                "PRAGMA table_info(stock_api_call_logs)"
            ).fetchall()
        }
    finally:
        connection.close()

    assert columns == {
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


@pytest.mark.asyncio
async def test_init_db_removes_retired_settings_and_quota_storage(tmp_path) -> None:
    sqlite_path = tmp_path / "retired-mx-quota.sqlite3"
    engine = create_engine(build_database_url(sqlite_path))
    try:
        await init_db(engine)
    finally:
        await engine.dispose()

    connection = sqlite3.connect(sqlite_path)
    try:
        connection.execute(
            "ALTER TABLE app_settings "
            "ADD COLUMN mx_daily_limits_json TEXT NOT NULL DEFAULT '{}'"
        )
        connection.execute(
            "ALTER TABLE app_settings "
            "ADD COLUMN max_tool_loop_iterations INTEGER NOT NULL DEFAULT 20"
        )
        connection.execute(
            """
            CREATE TABLE stock_api_call_usage_scopes (
                log_id INTEGER PRIMARY KEY,
                credential_fingerprint VARCHAR(64) NOT NULL
            )
            """
        )
        connection.commit()
    finally:
        connection.close()

    engine = create_engine(build_database_url(sqlite_path))
    try:
        await init_db(engine)
    finally:
        await engine.dispose()

    connection = sqlite3.connect(sqlite_path)
    try:
        app_settings_columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(app_settings)").fetchall()
        }
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    finally:
        connection.close()

    assert "mx_daily_limits_json" not in app_settings_columns
    assert "max_tool_loop_iterations" not in app_settings_columns
    assert "stock_api_call_usage_scopes" not in tables
