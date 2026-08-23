"""API tests for versioned memory mutations."""

from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_memory_update_and_delete_require_current_version(
    api_client: AsyncClient,
) -> None:
    created_response = await api_client.post(
        "/api/aniu/memories",
        json={"content": "记住量价确认", "reason": "重复观察得到"},
    )
    assert created_response.status_code == 201
    created = created_response.json()

    missing_version = await api_client.put(
        f"/api/aniu/memories/{created['id']}",
        json={"content": "新内容", "reason": "新依据"},
    )
    assert missing_version.status_code == 422

    updated_response = await api_client.put(
        f"/api/aniu/memories/{created['id']}",
        json={
            "expected_version": created["version"],
            "content": "记住量价和趋势确认",
            "reason": "补充趋势条件",
        },
    )
    assert updated_response.status_code == 200
    updated = updated_response.json()
    assert updated["version"] == 2

    stale_delete = await api_client.request(
        "DELETE",
        f"/api/aniu/memories/{created['id']}",
        json={"expected_version": created["version"]},
    )
    assert stale_delete.status_code == 422

    deleted = await api_client.request(
        "DELETE",
        f"/api/aniu/memories/{created['id']}",
        json={"expected_version": updated["version"]},
    )
    assert deleted.status_code == 204
