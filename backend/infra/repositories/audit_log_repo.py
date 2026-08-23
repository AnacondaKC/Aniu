"""Append-only administrative audit log persistence."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from backend.infra.db.models import AuditLogModel


@dataclass(frozen=True, slots=True)
class AuditRecord:
    event_type: str
    method: str
    path: str
    request_id: str
    status_code: int
    actor_name: str | None = None
    resource_id: str | None = None
    source_ip: str | None = None


class AuditLogRepository:
    """Write-only adapter; records are intentionally never updated/deleted."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def append(self, record: AuditRecord) -> None:
        self._session.add(
            AuditLogModel(
                event_type=record.event_type,
                method=record.method,
                path=record.path,
                resource_id=record.resource_id,
                actor_name=record.actor_name,
                request_id=record.request_id,
                source_ip=record.source_ip,
                status_code=record.status_code,
            )
        )
        await self._session.flush()
