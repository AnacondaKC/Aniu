"""Tests for append-only administrative audit records."""

from __future__ import annotations

import json

import pytest
from sqlalchemy import select

from backend.infra.db.models import AuditLogModel
from backend.infra.repositories import AuditLogRepository, AuditRecord


@pytest.mark.asyncio
async def test_audit_record_contains_metadata_but_no_request_secret(session) -> None:
    secret = "must-not-appear-in-audit"
    repository = AuditLogRepository(session)
    await repository.append(
        AuditRecord(
            event_type="settings.update",
            method="PUT",
            path="/api/aniu/settings",
            request_id="req-1",
            actor_name="aniu",
            status_code=200,
            source_ip="127.0.0.1",
        )
    )
    await session.commit()

    row = await session.scalar(select(AuditLogModel))
    assert row is not None
    assert row.event_type == "settings.update"
    assert row.actor_name == "aniu"
    assert secret not in json.dumps(
        {key: value for key, value in vars(row).items() if not key.startswith("_sa_")},
        default=str,
    )
