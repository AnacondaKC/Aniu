"""Allowlisted report and memory tools for the Dream Agent."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, cast

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.business.memories import MemoryItem, MemoryService
from backend.business.runs import RunReportRecord
from backend.infra.integrations.tool_policy import SideEffectLevel
from backend.infra.repositories.memory_repo import MemoryRepository
from backend.infra.repositories.run_repo import RunRepository
from backend.llm import AbortSignal, ProviderJsonObject, ToolDefinition


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


def _memory_payload(item: MemoryItem) -> dict[str, object]:
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
class MemoryListTool:
    session_factory: async_sessionmaker[AsyncSession]
    name: str = "memory_list"
    side_effect_level: SideEffectLevel = SideEffectLevel.READ
    execution_mode: str = "parallel"

    def to_tool_definition(self) -> ToolDefinition:
        return {
            "name": self.name,
            "description": "分页读取当前所有未删除的长期记忆及其版本。",
            "parameters": _schema(
                {
                    "offset": {"type": "integer", "minimum": 0, "default": 0},
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "default": 50,
                    },
                },
                [],
            ),
        }

    async def run(self, offset: int = 0, limit: int = 50) -> object:
        if offset < 0 or limit < 1 or limit > 100:
            raise ValueError("memory_list pagination is invalid")
        async with self.session_factory() as session:
            repository = MemoryRepository(session)
            total = await repository.count_items()
            page = await repository.list_items(limit=limit, offset=offset)
        return {
            "status": "ok",
            "offset": offset,
            "limit": limit,
            "total": total,
            "has_more": offset + len(page) < total,
            "items": [_memory_payload(item) for item in page],
        }

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
            raise RuntimeError("memory list aborted")
        payload = await self.run(**cast(dict[str, Any], kwargs))
        items = payload.get("items", []) if isinstance(payload, dict) else []
        try:
            async with self.session_factory() as session:
                await MemoryService(MemoryRepository(session)).record_read(
                    keywords="*",
                    result_count=len(items) if isinstance(items, list) else 0,
                    task_id=run_id,
                )
                await session.commit()
        except Exception:
            pass
        return payload


@dataclass(slots=True)
class DreamReportReadTool:
    session_factory: async_sessionmaker[AsyncSession]
    target_date: date
    name: str = "dream_read_reports"
    side_effect_level: SideEffectLevel = SideEffectLevel.READ
    execution_mode: str = "parallel"

    def to_tool_definition(self) -> ToolDefinition:
        return {
            "name": self.name,
            "description": "分页读取指定整理日期内已完成运行的 Markdown 报告。",
            "parameters": _schema(
                {
                    "offset": {"type": "integer", "minimum": 0, "default": 0},
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 20,
                        "default": 10,
                    },
                },
                [],
            ),
        }

    async def run(self, offset: int = 0, limit: int = 10) -> object:
        if offset < 0 or limit < 1 or limit > 20:
            raise ValueError("dream report pagination is invalid")
        async with self.session_factory() as session:
            reports = await RunRepository(session).list_completed_reports(
                self.target_date,
                limit=limit,
                offset=offset,
            )
        return {
            "status": "ok",
            "target_date": self.target_date.isoformat(),
            "offset": offset,
            "limit": limit,
            "reports": [_report_payload(report) for report in reports],
            "has_more": len(reports) == limit,
        }

    async def run_for_call(
        self,
        *,
        run_id: int,
        tool_call_id: str,
        abort_signal: AbortSignal | None = None,
        **kwargs: object,
    ) -> object:
        del run_id, tool_call_id
        if abort_signal is not None and abort_signal.aborted:
            raise RuntimeError("dream report read aborted")
        return await self.run(**cast(dict[str, Any], kwargs))


def _report_payload(report: RunReportRecord) -> dict[str, object]:
    return report.as_payload()


__all__ = ["DreamReportReadTool", "MemoryListTool"]
