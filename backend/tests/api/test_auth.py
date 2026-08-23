"""API tests for session authentication."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from backend.api.sse import StreamHub
from backend.business.auth.service import AuthLoginThrottle
from backend.infra.db.models import AuditLogModel, AuthSessionModel
from backend.main import app


@pytest.fixture
async def api_client(session_factory) -> AsyncIterator[AsyncClient]:
    from backend.api.deps import get_session_factory

    app.dependency_overrides[get_session_factory] = lambda: session_factory
    app.state.runtime.session_factory = session_factory
    app.state.runtime.stream_hub = StreamHub()
    app.state.runtime.auth_login_throttle = AuthLoginThrottle()

    class _NoopWorker:
        async def submit(self, run_id: int) -> None:
            del run_id

        def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

    from backend.tests.api.conftest import DisabledJobRunner

    app.state.runtime.run_worker = _NoopWorker()
    app.state.runtime.job_runner = DisabledJobRunner()
    app.state.runtime.llm_client = object()
    app.state.runtime.model_connectivity_tester = object()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    app.dependency_overrides.clear()
    for key in (
        "session_factory",
        "stream_hub",
        "run_worker",
        "job_runner",
        "llm_client",
        "model_connectivity_tester",
    ):
        if hasattr(app.state, key):
            delattr(app.state, key)
    app.state.runtime.auth_login_throttle = AuthLoginThrottle()


@pytest.mark.asyncio
async def test_open_mode_until_setup(api_client: AsyncClient) -> None:
    session = await api_client.get("/api/aniu/auth/session")
    assert session.status_code == 200
    body = session.json()
    assert body["authenticated"] is False
    assert body["identity_initialized"] is False

    # Protected routes remain usable before setup.
    settings = await api_client.get("/api/aniu/settings")
    assert settings.status_code == 200


@pytest.mark.asyncio
async def test_setup_rejects_non_loopback(api_client: AsyncClient) -> None:
    del api_client  # fixture wires the isolated database into the app
    transport = ASGITransport(app=app, client=("192.0.2.10", 1234))
    async with AsyncClient(transport=transport, base_url="http://test") as remote:
        response = await remote.post(
            "/api/aniu/auth/setup",
            json={"username": "aniu", "password": "password123"},
        )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_setup_login_and_lockdown(
    api_client: AsyncClient,
    session_factory,
) -> None:
    setup = await api_client.post(
        "/api/aniu/auth/setup",
        json={"username": "aniu", "password": "password123"},
    )
    assert setup.status_code == 201
    assert setup.json()["authenticated"] is True
    assert setup.json()["username"] == "aniu"
    assert "role" not in setup.json()
    assert "permissions" not in setup.json()
    assert setup.json()["csrf_token"]
    csrf = setup.json()["csrf_token"]

    # A deployment can initialize exactly one local identity.
    duplicate = await api_client.post(
        "/api/aniu/auth/setup",
        json={"username": "other", "password": "password123"},
    )
    assert duplicate.status_code == 400

    # Without cookie, locked down.
    anon = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
    denied = await anon.get("/api/aniu/settings")
    assert denied.status_code == 401
    await anon.aclose()

    # Authenticated GET works with cookie from setup.
    ok = await api_client.get("/api/aniu/settings")
    assert ok.status_code == 200

    # Write without CSRF is rejected.
    no_csrf = await api_client.put(
        "/api/aniu/settings",
        json={"expected_revision": 0},
    )
    assert no_csrf.status_code == 403

    # Login-issued CSRF remains valid across read-only session probes.
    with_csrf = await api_client.put(
        "/api/aniu/settings",
        json={
            "expected_revision": ok.json()["revision"],
            "prompt_profile": ok.json()["prompt_profile"],
        },
        headers={"X-CSRF-Token": csrf},
    )
    assert with_csrf.status_code == 200

    # Session probes return the same stable token, including under concurrency.
    first_probe, second_probe = await asyncio.gather(
        api_client.get("/api/aniu/auth/session"),
        api_client.get("/api/aniu/auth/session"),
    )
    assert first_probe.status_code == 200
    assert second_probe.status_code == 200
    assert first_probe.json()["csrf_token"] == csrf
    assert second_probe.json()["csrf_token"] == csrf
    probed_write = await api_client.put(
        "/api/aniu/settings",
        json={
            "expected_revision": with_csrf.json()["revision"],
            "prompt_profile": with_csrf.json()["prompt_profile"],
        },
        headers={"X-CSRF-Token": csrf},
    )
    assert probed_write.status_code == 200

    async with session_factory() as session:
        audit_rows = list(
            (
                await session.scalars(
                    select(AuditLogModel).where(
                        AuditLogModel.event_type == "settings.update",
                        AuditLogModel.status_code == 200,
                    )
                )
            ).all()
        )
    assert len(audit_rows) == 2
    assert {row.actor_name for row in audit_rows} == {"aniu"}
    assert all(row.status_code == 200 for row in audit_rows)

    # Logout clears session.
    out = await api_client.post("/api/aniu/auth/logout")
    assert out.status_code == 204
    after = await api_client.get("/api/aniu/settings")
    assert after.status_code == 401


@pytest.mark.asyncio
async def test_http_login_rate_limit_is_process_scoped(
    api_client: AsyncClient,
) -> None:
    setup = await api_client.post(
        "/api/aniu/auth/setup",
        json={"username": "aniu", "password": "password123"},
    )
    assert setup.status_code == 201

    for _ in range(5):
        failed = await api_client.post(
            "/api/aniu/auth/login",
            json={"username": "aniu", "password": "wrong-password"},
        )
        assert failed.status_code == 401

    locked = await api_client.post(
        "/api/aniu/auth/login",
        json={"username": "aniu", "password": "wrong-password"},
    )
    assert locked.status_code == 401
    assert "too many failed login attempts" in locked.json()["error"]["message"]


@pytest.mark.asyncio
async def test_failed_login_before_setup_does_not_block_atomic_setup(
    api_client: AsyncClient,
) -> None:
    for _ in range(5):
        failed = await api_client.post(
            "/api/aniu/auth/login",
            json={"username": "aniu", "password": "wrong-password"},
        )
        assert failed.status_code == 401

    setup = await api_client.post(
        "/api/aniu/auth/setup",
        json={"username": "aniu", "password": "password123"},
    )

    assert setup.status_code == 201
    assert setup.json()["authenticated"] is True
    assert setup.json()["csrf_token"]


@pytest.mark.asyncio
async def test_frequent_session_probes_do_not_write_session(
    api_client: AsyncClient,
    session_factory,
) -> None:
    setup = await api_client.post(
        "/api/aniu/auth/setup",
        json={"username": "aniu", "password": "password123"},
    )
    csrf = setup.json()["csrf_token"]
    async with session_factory() as session:
        before = await session.scalar(select(AuthSessionModel))
        assert before is not None
        before_times = (before.last_seen_at, before.expires_at)

    responses = await asyncio.gather(
        *(api_client.get("/api/aniu/auth/session") for _ in range(20))
    )

    assert all(response.status_code == 200 for response in responses)
    assert {response.json()["csrf_token"] for response in responses} == {csrf}
    async with session_factory() as session:
        after = await session.scalar(select(AuthSessionModel))
        assert after is not None
        assert (after.last_seen_at, after.expires_at) == before_times


@pytest.mark.asyncio
async def test_openapi_declares_session_cookie_security(
    api_client: AsyncClient,
) -> None:
    response = await api_client.get("/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    cookie_scheme = schema["components"]["securitySchemes"]["AniuSessionCookie"]
    assert cookie_scheme == {
        "type": "apiKey",
        "in": "cookie",
        "name": "aniu_session",
    }
    assert schema["paths"]["/api/aniu/runs"]["get"]["security"] == [
        {"AniuSessionCookie": []}
    ]
