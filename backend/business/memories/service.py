"""Application service for simple task-sourced memories."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from backend.business.memories.models import (
    MemoryActivity,
    MemoryItem,
    MemoryMatchMode,
    MemoryOperation,
    MemoryOverview,
    MemoryWriteCommand,
)
from backend.business.memories.ports import MemoryRepositoryPort
from backend.business.shared.ports import CommitterPort

_MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")
_DEFAULT_ACTIVITY_LIMIT = 20
_MAX_ACTIVITY_LIMIT = 100
_DEFAULT_ITEM_LIMIT = 20
_MAX_ITEM_LIMIT = 100
_MAX_ITEM_KEYWORDS_LENGTH = 200
NowProvider = Callable[[], datetime]


class MemoryService:
    def __init__(
        self,
        repository: MemoryRepositoryPort,
        *,
        now_provider: NowProvider | None = None,
        committer: CommitterPort | None = None,
    ) -> None:
        self._repository = repository
        self._now_provider = now_provider or (lambda: datetime.now(tz=UTC))
        self._committer = committer

    async def read(
        self,
        *,
        keywords: str,
        match_mode: MemoryMatchMode = MemoryMatchMode.AND,
        limit: int = 5,
    ) -> list[MemoryItem]:
        cleaned_keywords = keywords.strip()
        if not cleaned_keywords:
            raise ValueError("keywords must not be empty")
        if limit < 1 or limit > 10:
            raise ValueError("limit must be between 1 and 10")
        return await self._repository.search(
            keywords=cleaned_keywords,
            match_mode=match_mode,
            limit=limit,
        )

    async def write(self, command: MemoryWriteCommand) -> MemoryItem:
        self._validate_command(command)
        item = await self._repository.write(command)
        await self._commit()
        return item

    async def record_read(
        self,
        *,
        keywords: str,
        result_count: int,
        task_id: int = 0,
    ) -> MemoryActivity:
        if not keywords.strip():
            raise ValueError("memory reads require keywords")
        if result_count < 0:
            raise ValueError("result_count must not be negative")
        return await self._repository.record_read(
            keywords=keywords.strip()[:500],
            result_count=result_count,
            task_id=task_id,
        )

    async def activities(
        self,
        *,
        task_id: int | None = None,
        limit: int = _DEFAULT_ACTIVITY_LIMIT,
        offset: int = 0,
    ) -> tuple[list[MemoryActivity], int]:
        if task_id is not None and task_id < 1:
            raise ValueError("task_id must be positive")
        if limit < 1 or limit > _MAX_ACTIVITY_LIMIT:
            raise ValueError("activity_limit must be between 1 and 100")
        if offset < 0:
            raise ValueError("activity_offset must be >= 0")
        total = await self._repository.count_activities(task_id=task_id)
        rows = await self._repository.list_activities(
            limit=limit,
            offset=offset,
            task_id=task_id,
        )
        return list(rows), total

    async def overview(
        self,
        *,
        activity_limit: int = _DEFAULT_ACTIVITY_LIMIT,
        activity_offset: int = 0,
        activity_task_id: int | None = None,
        activity_operation: str | None = None,
        item_limit: int = _DEFAULT_ITEM_LIMIT,
        item_offset: int = 0,
        item_keywords: str = "",
    ) -> MemoryOverview:
        if activity_limit < 1 or activity_limit > _MAX_ACTIVITY_LIMIT:
            raise ValueError("activity_limit must be between 1 and 100")
        if activity_offset < 0:
            raise ValueError("activity_offset must be >= 0")
        if activity_task_id is not None and activity_task_id < 1:
            raise ValueError("activity_task_id must be positive")
        if activity_operation not in {None, "read", "create", "update", "delete"}:
            raise ValueError("activity_operation is not supported")
        if item_limit < 1 or item_limit > _MAX_ITEM_LIMIT:
            raise ValueError("item_limit must be between 1 and 100")
        if item_offset < 0:
            raise ValueError("item_offset must be >= 0")
        cleaned_item_keywords = item_keywords.strip()
        if len(cleaned_item_keywords) > _MAX_ITEM_KEYWORDS_LENGTH:
            raise ValueError("item_keywords must not exceed 200 characters")
        generated_at = self._now_provider().astimezone(_MARKET_TIMEZONE)
        activity_total = await self._repository.count_activities(
            task_id=activity_task_id,
            operation=activity_operation,
        )
        activities = await self._repository.list_activities(
            limit=activity_limit,
            offset=activity_offset,
            task_id=activity_task_id,
            operation=activity_operation,
        )
        item_total = await self._repository.count_items()
        item_match_total = (
            item_total
            if not cleaned_item_keywords
            else await self._repository.count_items(keywords=cleaned_item_keywords)
        )
        items = await self._repository.list_items(
            limit=item_limit,
            offset=item_offset,
            keywords=cleaned_item_keywords,
        )
        return MemoryOverview(
            activities=tuple(activities),
            activity_total=activity_total,
            items=tuple(items),
            item_total=item_total,
            item_match_total=item_match_total,
            generated_at=generated_at,
        )

    def _validate_command(self, command: MemoryWriteCommand) -> None:
        if command.task_id < 0:
            raise ValueError("memory writes require a valid task id")
        if command.operation is MemoryOperation.CREATE:
            if command.memory_id is not None:
                raise ValueError("create cannot target an existing memory")
            if not _clean_text(command.content):
                raise ValueError("create requires content")
            if not _clean_text(command.reason):
                raise ValueError("create requires reason")
            return
        if command.memory_id is None or command.memory_id < 1:
            raise ValueError("update or delete requires memory_id")
        if command.expected_version is None or command.expected_version < 1:
            raise ValueError("update or delete requires expected_version")
        if command.operation is MemoryOperation.UPDATE:
            if not _clean_text(command.content):
                raise ValueError("update requires content")
            if not _clean_text(command.reason):
                raise ValueError("update requires reason")

    async def _commit(self) -> None:
        if self._committer is not None:
            await self._committer.commit()


def _clean_text(value: str | None) -> str:
    return value.strip() if value is not None else ""


__all__ = ["MemoryService"]
