"""API tests for full run snapshot SSE streaming."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from httpx import AsyncClient

from backend.api.security import require_stream_authenticated
from backend.api.sse.stream_hub import StreamHub, stream_run_snapshot
from backend.business.auth.service import UnauthorizedError


@pytest.mark.asyncio
async def test_sse_run_stream_returns_snapshot_payload(
    run_api_client: AsyncClient,
) -> None:
    started = await run_api_client.post("/api/aniu/runs/start", json={})
    run_id = started.json()["run_id"]
    for _ in range(100):
        detail = await run_api_client.get(f"/api/aniu/runs/{run_id}")
        if detail.json()["status"] != "RUNNING":
            break
        await asyncio.sleep(0.01)

    response = await run_api_client.get(f"/api/aniu/sse/run/{run_id}")

    assert response.status_code == 200
    assert '"run_id":' in response.text
    assert '"trace":' in response.text
    compact = response.text.replace(" ", "")
    assert '"schema_version":3' in compact
    assert '"kind":"checkpoint"' in compact


@pytest.mark.asyncio
async def test_sse_run_stream_returns_not_found_for_unknown_run(
    run_api_client: AsyncClient,
) -> None:
    response = await run_api_client.get("/api/aniu/sse/run/999")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_sse_releases_initial_snapshot_session_before_streaming() -> None:
    session_active = False

    @asynccontextmanager
    async def session_factory():
        nonlocal session_active
        session_active = True
        try:
            yield object()
        finally:
            session_active = False

    class Service:
        async def get_run_detail(self, _query):
            assert session_active
            return SimpleNamespace(
                run_id=47,
                status="COMPLETED",
                trace={"event_seq": 1, "stages": []},
            )

    class Runtime:
        stream_hub = StreamHub()

        def require_stream_hub(self):
            return self.stream_hub

        def require_session_factory(self):
            return session_factory

        def run_service(self, _session):
            return Service()

    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(runtime=Runtime())),
        query_params={},
    )

    response = await stream_run_snapshot(47, request)  # type: ignore[arg-type]

    assert session_active is False
    chunks = [chunk async for chunk in response.body_iterator]
    assert any('"kind":"checkpoint"' in str(chunk).replace(" ", "") for chunk in chunks)


@pytest.mark.asyncio
async def test_sse_not_found_clears_its_subscription_generation() -> None:
    @asynccontextmanager
    async def session_factory():
        yield object()

    class Service:
        async def get_run_detail(self, _query):
            raise HTTPException(status_code=404, detail="run not found")

    class Runtime:
        stream_hub = StreamHub()

        def require_stream_hub(self):
            return self.stream_hub

        def require_session_factory(self):
            return session_factory

        def run_service(self, _session):
            return Service()

    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(runtime=Runtime())),
        query_params={},
    )

    with pytest.raises(HTTPException):
        await stream_run_snapshot(48, request)  # type: ignore[arg-type]
    assert not Runtime.stream_hub.has_subscribers(48)
    assert Runtime.stream_hub.current_seq(48) == 0


@pytest.mark.asyncio
async def test_sse_authentication_releases_its_session_before_route_execution() -> None:
    session_active = False

    @asynccontextmanager
    async def session_factory():
        nonlocal session_active
        session_active = True
        try:
            yield object()
        finally:
            session_active = False

    class Auth:
        async def resolve_session(self, _raw):
            assert session_active
            return None

        async def identity_initialized(self):
            assert session_active
            return False

    class Runtime:
        def require_session_factory(self):
            return session_factory

        def auth_service(self, _session):
            return Auth()

    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(runtime=Runtime())),
    )

    with pytest.raises(UnauthorizedError):
        await require_stream_authenticated(  # type: ignore[arg-type]
            request,
            None,
        )

    assert session_active is False
