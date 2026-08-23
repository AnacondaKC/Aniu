"""Shared task-number allocation across run, schedule, and review entities."""

from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.business.runs.numbering import RUN_TASK_TYPE, build_run_id, run_id_prefix


async def next_task_id(
    session: AsyncSession,
    reference_date: date | None = None,
    *,
    task_type: int = RUN_TASK_TYPE,
) -> int:
    target_date = reference_date or datetime.now(tz=UTC).date()
    prefix = run_id_prefix(target_date, task_type=task_type)
    pattern = f"{prefix}%"

    await session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS task_id_counters (
                task_date TEXT NOT NULL,
                task_type INTEGER NOT NULL,
                sequence INTEGER NOT NULL,
                PRIMARY KEY (task_date, task_type)
            )
            """
        )
    )
    counter_key = {"task_date": target_date.isoformat(), "task_type": task_type}
    counter_exists = await session.scalar(
        text(
            """
            SELECT sequence
            FROM task_id_counters
            WHERE task_date = :task_date AND task_type = :task_type
            """
        ),
        counter_key,
    )
    if counter_exists is None:
        existing_sequence = int(
            (
                await session.scalar(
                    text(
                        """
                        SELECT COALESCE(MAX(sequence), 0)
                        FROM (
                            SELECT CAST(
                                substr(CAST(id AS TEXT), 10) AS INTEGER
                            ) AS sequence
                            FROM strategy_runs
                            WHERE CAST(id AS TEXT) LIKE :pattern
                            UNION ALL
                            SELECT CAST(
                                substr(CAST(id AS TEXT), 10) AS INTEGER
                            ) AS sequence
                            FROM strategy_schedules
                            WHERE CAST(id AS TEXT) LIKE :pattern
                            UNION ALL
                            SELECT CAST(
                                substr(CAST(id AS TEXT), 10) AS INTEGER
                            ) AS sequence
                            FROM memory_dreams
                            WHERE CAST(id AS TEXT) LIKE :pattern
                        )
                        """
                    ),
                    {"pattern": pattern},
                )
            )
            or 0
        )
        await session.execute(
            text(
                """
                INSERT OR IGNORE INTO task_id_counters (task_date, task_type, sequence)
                VALUES (:task_date, :task_type, :sequence)
                """
            ),
            {**counter_key, "sequence": existing_sequence},
        )
    sequence = await session.scalar(
        text(
            """
            UPDATE task_id_counters
            SET sequence = sequence + 1
            WHERE task_date = :task_date AND task_type = :task_type
            RETURNING sequence
            """
        ),
        {"task_date": target_date.isoformat(), "task_type": task_type},
    )
    if sequence is None:
        raise RuntimeError("failed to allocate task sequence")
    return build_run_id(target_date, sequence=int(sequence), task_type=task_type)
