"""Durable idempotency ledger for write-side tool calls."""

from __future__ import annotations

import json
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.infra.db.models import ToolInvocationModel, utc_now_iso


class ToolInvocationConflictError(RuntimeError):
    """Raised when replay cannot safely execute an external side effect."""


@dataclass(frozen=True, slots=True)
class ToolInvocationReservation:
    completed_result: object | None = None
    is_completed: bool = False


class ToolInvocationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def reserve(
        self,
        *,
        run_id: int,
        tool_call_id: str,
        tool_name: str,
        arguments: dict[str, object],
    ) -> ToolInvocationReservation:
        arguments_json = json.dumps(
            arguments,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        statement = select(ToolInvocationModel).where(
            ToolInvocationModel.run_id == run_id,
            ToolInvocationModel.tool_call_id == tool_call_id,
        )
        existing = await self._session.scalar(statement)
        if existing is not None:
            if (
                existing.tool_name != tool_name
                or existing.arguments_json != arguments_json
            ):
                raise ToolInvocationConflictError(
                    "tool call id was reused with different arguments"
                )
            if existing.status in {"COMPLETED", "FAILED"}:
                return ToolInvocationReservation(
                    completed_result=json.loads(existing.result_json or "null"),
                    is_completed=True,
                )
            raise ToolInvocationConflictError(
                "write tool result is unknown; refusing duplicate execution"
            )

        now = utc_now_iso()
        self._session.add(
            ToolInvocationModel(
                run_id=run_id,
                tool_call_id=tool_call_id,
                tool_name=tool_name,
                arguments_json=arguments_json,
                status="STARTED",
                created_at=now,
                updated_at=now,
            )
        )
        await self._session.flush()
        return ToolInvocationReservation()

    async def complete(
        self,
        *,
        run_id: int,
        tool_call_id: str,
        result: object,
    ) -> None:
        statement = select(ToolInvocationModel).where(
            ToolInvocationModel.run_id == run_id,
            ToolInvocationModel.tool_call_id == tool_call_id,
        )
        invocation = await self._session.scalar(statement)
        if invocation is None or invocation.status != "STARTED":
            raise ToolInvocationConflictError("write tool invocation is not reserved")
        invocation.status = "COMPLETED"
        invocation.result_json = json.dumps(
            result,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )
        invocation.updated_at = utc_now_iso()
        await self._session.flush()

    async def fail(
        self,
        *,
        run_id: int,
        tool_call_id: str,
        error: str,
    ) -> None:
        """Finalize a failed write call without permitting a second side effect."""

        statement = select(ToolInvocationModel).where(
            ToolInvocationModel.run_id == run_id,
            ToolInvocationModel.tool_call_id == tool_call_id,
        )
        invocation = await self._session.scalar(statement)
        if invocation is None or invocation.status != "STARTED":
            raise ToolInvocationConflictError("write tool invocation is not reserved")
        invocation.status = "FAILED"
        invocation.result_json = json.dumps(
            {"status": "error", "error": error},
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )
        invocation.updated_at = utc_now_iso()
        await self._session.flush()


__all__ = [
    "ToolInvocationConflictError",
    "ToolInvocationRepository",
    "ToolInvocationReservation",
]
