"""SQLAlchemy persistence adapter for simple task-sourced memories."""

from __future__ import annotations

import re
from datetime import UTC, datetime

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.business.memories.models import (
    MemoryActivity,
    MemoryActivityOperation,
    MemoryItem,
    MemoryMatchMode,
    MemoryOperation,
    MemoryWriteCommand,
)
from backend.infra.db.models import MemoryActivityModel, MemoryItemModel, utc_now_iso


class MemoryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def search(
        self,
        *,
        keywords: str,
        match_mode: MemoryMatchMode,
        limit: int,
    ) -> list[MemoryItem]:
        statement = select(MemoryItemModel).where(MemoryItemModel.deleted_at.is_(None))
        tokens = [token for token in re.split(r"[\s,，、;；]+", keywords) if token][:8]
        if tokens:
            token_conditions = [
                or_(
                    MemoryItemModel.content.contains(token, autoescape=True),
                    MemoryItemModel.reason.contains(token, autoescape=True),
                )
                for token in tokens
            ]
            condition = (
                and_(*token_conditions)
                if match_mode is MemoryMatchMode.AND
                else or_(*token_conditions)
            )
            statement = statement.where(condition)
        statement = statement.order_by(
            MemoryItemModel.updated_at.desc(), MemoryItemModel.id.desc()
        ).limit(limit)
        rows = list((await self._session.scalars(statement)).all())
        return [_item_from_row(row) for row in rows]

    async def write(self, command: MemoryWriteCommand) -> MemoryItem:
        if command.operation is MemoryOperation.CREATE:
            return await self._create(command)

        if command.memory_id is None:
            raise ValueError("memory id is required")
        row = await self._session.get(MemoryItemModel, command.memory_id)
        if row is None or row.deleted_at is not None:
            raise ValueError("memory not found")

        now = utc_now_iso()
        previous_content = row.content
        values: dict[str, object] = {
            "version": MemoryItemModel.version + 1,
            "updated_task_id": command.task_id,
            "updated_at": now,
        }
        if command.operation is MemoryOperation.UPDATE:
            content = _required_text(command.content)
            reason = _required_text(command.reason)
            values.update({"content": content, "reason": reason})
            operation = MemoryActivityOperation.UPDATE
        elif command.operation is MemoryOperation.DELETE:
            values["deleted_at"] = now
            operation = MemoryActivityOperation.DELETE
        else:
            raise ValueError("unsupported memory operation")

        statement = update(MemoryItemModel).where(
            MemoryItemModel.id == command.memory_id,
            MemoryItemModel.deleted_at.is_(None),
        )
        if command.expected_version is not None:
            statement = statement.where(
                MemoryItemModel.version == command.expected_version
            )
        result = await self._session.execute(statement.values(**values))
        if getattr(result, "rowcount", None) != 1:
            await self._session.refresh(row)
            if row.deleted_at is not None:
                raise ValueError("memory not found")
            raise ValueError(
                "memory version conflict: "
                f"expected {command.expected_version}, current {row.version}"
            )

        await self._session.refresh(row)
        self._session.add(
            MemoryActivityModel(
                operation=operation.value,
                memory_id=row.id,
                content=(
                    row.content
                    if operation is MemoryActivityOperation.UPDATE
                    else previous_content
                ),
                task_id=command.task_id,
            )
        )
        await self._session.flush()
        return _item_from_row(row)

    async def list_items(
        self, *, limit: int, offset: int, keywords: str = ""
    ) -> list[MemoryItem]:
        statement = select(MemoryItemModel).where(MemoryItemModel.deleted_at.is_(None))
        cleaned_keywords = keywords.strip()
        if cleaned_keywords:
            statement = statement.where(
                or_(
                    MemoryItemModel.content.contains(cleaned_keywords, autoescape=True),
                    MemoryItemModel.reason.contains(cleaned_keywords, autoescape=True),
                )
            )
        statement = (
            statement.order_by(
                MemoryItemModel.updated_at.desc(), MemoryItemModel.id.desc()
            )
            .limit(limit)
            .offset(offset)
        )
        rows = list((await self._session.scalars(statement)).all())
        return [_item_from_row(row) for row in rows]

    async def count_items(self, *, keywords: str = "") -> int:
        statement = select(func.count(MemoryItemModel.id)).where(
            MemoryItemModel.deleted_at.is_(None)
        )
        cleaned_keywords = keywords.strip()
        if cleaned_keywords:
            statement = statement.where(
                or_(
                    MemoryItemModel.content.contains(cleaned_keywords, autoescape=True),
                    MemoryItemModel.reason.contains(cleaned_keywords, autoescape=True),
                )
            )
        return int((await self._session.execute(statement)).scalar_one())

    async def list_activities(
        self,
        *,
        limit: int,
        offset: int,
        task_id: int | None = None,
        operation: str | None = None,
    ) -> list[MemoryActivity]:
        statement = (
            select(MemoryActivityModel)
            .order_by(
                MemoryActivityModel.created_at.desc(),
                MemoryActivityModel.id.desc(),
            )
            .limit(limit)
            .offset(offset)
        )
        if task_id is not None:
            statement = statement.where(MemoryActivityModel.task_id == task_id)
        if operation is not None:
            statement = statement.where(MemoryActivityModel.operation == operation)
        rows = list((await self._session.scalars(statement)).all())
        return [_activity_from_row(row) for row in rows]

    async def count_activities(
        self,
        *,
        task_id: int | None = None,
        operation: str | None = None,
    ) -> int:
        statement = select(func.count(MemoryActivityModel.id))
        if task_id is not None:
            statement = statement.where(MemoryActivityModel.task_id == task_id)
        if operation is not None:
            statement = statement.where(MemoryActivityModel.operation == operation)
        result = await self._session.execute(statement)
        return int(result.scalar_one())

    async def record_read(
        self, *, keywords: str, result_count: int, task_id: int = 0
    ) -> MemoryActivity:
        row = MemoryActivityModel(
            operation=MemoryActivityOperation.READ.value,
            content=keywords,
            result_count=result_count,
            task_id=task_id,
        )
        self._session.add(row)
        await self._session.flush()
        return _activity_from_row(row)

    async def _create(self, command: MemoryWriteCommand) -> MemoryItem:
        now = utc_now_iso()
        row = MemoryItemModel(
            content=_required_text(command.content),
            reason=(command.reason or "").strip(),
            created_task_id=command.task_id,
            updated_task_id=command.task_id,
            version=1,
            created_at=now,
            updated_at=now,
            deleted_at=None,
        )
        self._session.add(row)
        await self._session.flush()
        self._session.add(
            MemoryActivityModel(
                operation=MemoryActivityOperation.CREATE.value,
                memory_id=row.id,
                content=row.content,
                task_id=command.task_id,
            )
        )
        await self._session.flush()
        return _item_from_row(row)


def _required_text(value: str | None) -> str:
    result = (value or "").strip()
    if not result:
        raise ValueError("memory content must not be empty")
    return result


def _as_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _activity_from_row(row: MemoryActivityModel) -> MemoryActivity:
    return MemoryActivity(
        id=row.id,
        operation=MemoryActivityOperation(row.operation),
        memory_id=row.memory_id,
        content=row.content,
        result_count=row.result_count,
        task_id=row.task_id,
        created_at=_as_datetime(row.created_at) or datetime.now(tz=UTC),
    )


def _item_from_row(row: MemoryItemModel) -> MemoryItem:
    return MemoryItem(
        id=row.id,
        content=row.content,
        reason=row.reason,
        created_task_id=row.created_task_id,
        updated_task_id=row.updated_task_id,
        version=row.version,
        created_at=_as_datetime(row.created_at) or datetime.now(tz=UTC),
        updated_at=_as_datetime(row.updated_at) or datetime.now(tz=UTC),
        deleted_at=_as_datetime(row.deleted_at),
    )


__all__ = ["MemoryRepository"]
