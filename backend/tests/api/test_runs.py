"""API tests for run snapshot endpoints."""

from __future__ import annotations

import asyncio

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_run_detail_endpoint_returns_trace_snapshot(
    run_api_client: AsyncClient,
) -> None:
    started = await run_api_client.post("/api/aniu/runs/start", json={})
    run_id = started.json()["run_id"]

    detail = None
    for _ in range(50):
        detail = await run_api_client.get(f"/api/aniu/runs/{run_id}")
        assert detail.status_code == 200
        if detail.json()["trace"]["stages"] and detail.json()["status"] != "RUNNING":
            break
        await asyncio.sleep(0.05)
    runs = await run_api_client.get("/api/aniu/runs")

    assert started.status_code == 201
    assert detail is not None
    assert detail.status_code == 200
    assert runs.status_code == 200
    assert detail.json()["trace"]["stages"]
    assert detail.json()["summary_render_mode"] in {"markdown", "html"}
    summary = runs.json()[0]
    assert summary["run_id"] == run_id
    assert isinstance(summary["tool_calls_count"], int)
    assert isinstance(summary["thinking_count"], int)
    assert isinstance(summary["total_tokens"], int)
    assert isinstance(summary["trade_count"], int)
    assert summary["summary_render_mode"] in {"markdown", "html"}
    assert "trade_stage_status" not in summary


@pytest.mark.asyncio
async def test_run_detail_endpoint_returns_not_found(
    run_api_client: AsyncClient,
) -> None:
    detail = await run_api_client.get("/api/aniu/runs/999")

    assert detail.status_code == 404
