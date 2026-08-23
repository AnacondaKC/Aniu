"""Run-stage Agent tools for simple task-sourced memories."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, cast

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.business.memories import (
    MemoryItem,
    MemoryMatchMode,
    MemoryOperation,
    MemoryService,
    MemoryWriteCommand,
)
from backend.infra.integrations.tool_policy import SideEffectLevel
from backend.infra.repositories.memory_repo import MemoryRepository
from backend.llm import AbortSignal, ProviderJsonObject, ToolDefinition

_RUN_STAGES = ("Run",)
_MATCH_MODES = tuple(item.value for item in MemoryMatchMode)
logger = logging.getLogger(__name__)


def _schema(properties: dict[str, object], required: list[str]) -> ProviderJsonObject:
    return cast(
        ProviderJsonObject,
        {
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": False,
        },
    )


def _one_of(branches: list[ProviderJsonObject]) -> ProviderJsonObject:
    return cast(ProviderJsonObject, {"type": "object", "oneOf": branches})


def _item_payload(item: MemoryItem) -> dict[str, object]:
    return {
        "id": item.id,
        "content": item.content,
        "reason": item.reason,
        "version": item.version,
        "created_task_id": item.created_task_id,
        "updated_task_id": item.updated_task_id,
        "created_at": item.created_at.isoformat(),
        "updated_at": item.updated_at.isoformat(),
        "deleted_at": item.deleted_at.isoformat() if item.deleted_at else None,
    }


@dataclass(slots=True)
class MemoryReadTool:
    session_factory: async_sessionmaker[AsyncSession]
    name: str = "memory_read"
    enabled_stages: tuple[str, ...] = field(default=_RUN_STAGES)
    side_effect_level: SideEffectLevel = SideEffectLevel.READ
    execution_mode: str = "parallel"

    def to_tool_definition(self) -> ToolDefinition:
        return {
            "name": self.name,
            "description": (
                "从长期记忆库检索与关键词相关的记忆，"
                "并根据检索意图选择 AND 或 OR 匹配。"
            ),
            "parameters": _schema(
                {
                    "keywords": {
                        "type": "string",
                        "description": "用于检索记忆的简洁关键词，使用空格分隔。",
                    },
                    "match_mode": {
                        "type": "string",
                        "enum": list(_MATCH_MODES),
                        "description": (
                            "关键词匹配方式：and 要求全部关键词命中，"
                            "or 命中任一关键词。请根据检索意图自主选择。"
                        ),
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 10,
                        "default": 5,
                    },
                },
                ["keywords", "match_mode"],
            ),
        }

    async def run(
        self,
        keywords: str,
        match_mode: str = MemoryMatchMode.AND.value,
        limit: int = 5,
    ) -> object:
        try:
            parsed_match_mode = MemoryMatchMode(match_mode)
        except ValueError as exc:
            raise ValueError("match_mode must be 'and' or 'or'") from exc
        async with self.session_factory() as session:
            items = await MemoryService(MemoryRepository(session)).read(
                keywords=keywords,
                match_mode=parsed_match_mode,
                limit=limit,
            )
        return {"status": "ok", "items": [_item_payload(item) for item in items]}

    async def run_for_call(
        self,
        *,
        run_id: int,
        tool_call_id: str,
        abort_signal: AbortSignal | None = None,
        **kwargs: object,
    ) -> object:
        del tool_call_id
        if abort_signal is not None and abort_signal.aborted:
            raise RuntimeError("memory read aborted")
        payload = await self.run(**cast(dict[str, Any], kwargs))
        assert isinstance(payload, dict)
        items = payload.get("items")
        result_count = len(items) if isinstance(items, list) else 0
        keywords = str(kwargs.get("keywords") or "")
        try:
            async with self.session_factory() as session:
                await MemoryService(MemoryRepository(session)).record_read(
                    keywords=keywords,
                    result_count=result_count,
                    task_id=run_id,
                )
                await session.commit()
        except Exception:
            logger.warning(
                "failed to record memory read activity",
                extra={"task_id": run_id, "result_count": result_count},
                exc_info=True,
            )
        return payload


@dataclass(slots=True)
class MemoryWriteTool:
    session_factory: async_sessionmaker[AsyncSession]
    name: str = "memory_write"
    enabled_stages: tuple[str, ...] = field(default=_RUN_STAGES)
    side_effect_level: SideEffectLevel = SideEffectLevel.WRITE
    execution_mode: str = "sequential"
    requires_market_open: bool = False

    def is_write_call(self, arguments: object) -> bool:
        del arguments
        return True

    def to_tool_definition(self) -> ToolDefinition:
        content = {
            "type": "string",
            "description": "记忆内容，create/update 必填。",
        }
        reason = {
            "type": "string",
            "description": "形成或变更该记忆的依据，create/update 必填。",
        }
        memory_id = {
            "type": "integer",
            "minimum": 1,
            "description": "update/delete 必须填写 memory_read 返回的记忆 id。",
        }
        expected_version = {
            "type": "integer",
            "minimum": 1,
            "description": "update/delete 必须填写最近读取到的记忆 version。",
        }
        return {
            "name": self.name,
            "description": "创建、修改或软删除一条长期记忆。",
            "parameters": _one_of(
                [
                    _schema(
                        {
                            "operation": {"const": MemoryOperation.CREATE.value},
                            "content": content,
                            "reason": reason,
                        },
                        ["operation", "content", "reason"],
                    ),
                    _schema(
                        {
                            "operation": {"const": MemoryOperation.UPDATE.value},
                            "memory_id": memory_id,
                            "expected_version": expected_version,
                            "content": content,
                            "reason": reason,
                        },
                        [
                            "operation",
                            "memory_id",
                            "expected_version",
                            "content",
                            "reason",
                        ],
                    ),
                    _schema(
                        {
                            "operation": {"const": MemoryOperation.DELETE.value},
                            "memory_id": memory_id,
                            "expected_version": expected_version,
                        },
                        ["operation", "memory_id", "expected_version"],
                    ),
                ]
            ),
        }

    async def run_for_call(
        self,
        *,
        run_id: int,
        tool_call_id: str,
        abort_signal: AbortSignal | None = None,
        operation: str,
        memory_id: int | None = None,
        content: str | None = None,
        reason: str | None = None,
        expected_version: int | None = None,
    ) -> object:
        del tool_call_id
        if abort_signal is not None and abort_signal.aborted:
            raise RuntimeError("memory write aborted")
        try:
            parsed_operation = MemoryOperation(operation)
        except ValueError as exc:
            raise ValueError("unsupported memory operation") from exc
        command = MemoryWriteCommand(
            operation=parsed_operation,
            task_id=run_id,
            memory_id=memory_id,
            content=content,
            reason=reason,
            expected_version=expected_version,
        )
        async with self.session_factory() as session:
            item = await MemoryService(MemoryRepository(session)).write(command)
            await session.commit()
        return {"status": "ok", "item": _item_payload(item)}

    async def run(self, **_: object) -> object:
        raise RuntimeError("memory_write requires trusted run call context")


__all__ = ["MemoryReadTool", "MemoryWriteTool"]
