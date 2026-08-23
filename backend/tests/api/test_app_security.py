"""Application factory network and startup security tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from backend.bootstrap import app_factory
from backend.bootstrap.app_factory import _audit_event
from backend.bootstrap.runtime_config import RuntimeConfig
from backend.main import create_app


def test_audit_event_prefers_specific_path_prefix() -> None:
    assert (
        _audit_event("PUT", "/api/aniu/settings/channels/7/with-models")
        == "model_channel.update"
    )
    assert _audit_event("PUT", "/api/aniu/settings") == "settings.update"


def test_lan_config_requires_non_loopback_allowed_host() -> None:
    with pytest.raises(ValueError, match="requires a non-loopback allowed host"):
        RuntimeConfig(lan_mode=True)


@pytest.mark.asyncio
async def test_trusted_host_cors_and_security_headers() -> None:
    application = create_app(
        RuntimeConfig(
            serve_frontend=False,
            allowed_hosts=("test",),
            cors_origins=("http://localhost:5173",),
        )
    )
    transport = ASGITransport(app=application)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health/live")
        assert response.status_code == 200
        assert response.headers["x-content-type-options"] == "nosniff"
        assert response.headers["x-frame-options"] == "DENY"
        assert "frame-ancestors 'none'" in response.headers["content-security-policy"]

        preflight = await client.options(
            "/api/aniu/settings",
            headers={
                "Origin": "http://localhost:5173",
                "Access-Control-Request-Method": "PUT",
                "Access-Control-Request-Headers": "X-CSRF-Token,Content-Type",
            },
        )
        assert preflight.status_code == 200
        assert preflight.headers["access-control-allow-origin"] == (
            "http://localhost:5173"
        )
        assert "PUT" in preflight.headers["access-control-allow-methods"]

    evil_transport = ASGITransport(app=application)
    async with AsyncClient(
        transport=evil_transport,
        base_url="http://evil.example",
    ) as evil_client:
        denied = await evil_client.get("/health/live")
        assert denied.status_code == 400


@pytest.mark.asyncio
async def test_trusted_host_allows_explicit_wildcard() -> None:
    application = create_app(
        RuntimeConfig(
            serve_frontend=False,
            lan_mode=True,
            allowed_hosts=("*",),
        )
    )
    transport = ASGITransport(app=application)
    async with AsyncClient(transport=transport, base_url="http://aniu.lan") as client:
        response = await client.get("/health/live")

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_readiness_checks_database_worker_and_calendar_in_lan_mode(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "ready.sqlite3"
    application = create_app(
        RuntimeConfig(
            database_url=f"sqlite+aiosqlite:///{db_path}",
            serve_frontend=False,
            lan_mode=True,
            allowed_hosts=("test", "aniu.lan"),
        )
    )
    async with application.router.lifespan_context(application):
        transport = ASGITransport(app=application)
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            response = await client.get("/health/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert body["checks"]["db_initialized"] is True
    assert body["checks"]["db_writable"] is True
    assert body["checks"]["worker_running"] is True
    assert body["checks"]["calendar_covers_next_year"] is True
    assert body["checks"]["lan_mode"] is True


@pytest.mark.asyncio
async def test_lan_startup_allows_uninitialized_account(tmp_path: Path) -> None:
    db_path = tmp_path / "lan.sqlite3"
    application = create_app(
        RuntimeConfig(
            database_url=f"sqlite+aiosqlite:///{db_path}",
            lan_mode=True,
            allowed_hosts=("aniu.lan",),
            cors_origins=("https://aniu.lan",),
            serve_frontend=False,
        )
    )
    async with application.router.lifespan_context(application):
        pass


@pytest.mark.asyncio
async def test_startup_failure_closes_partially_initialized_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_recovery(*_args) -> None:
        raise RuntimeError("recovery failed")

    monkeypatch.setattr(app_factory, "_recover_stale_run_jobs", fail_recovery)
    application = create_app(
        RuntimeConfig(
            database_url=f"sqlite+aiosqlite:///{tmp_path / 'failed.sqlite3'}",
            serve_frontend=False,
        )
    )

    with pytest.raises(RuntimeError, match="recovery failed"):
        async with application.router.lifespan_context(application):
            pass

    runtime = application.state.runtime
    assert runtime.job_runner is None
    assert runtime.run_worker is None
    assert runtime.mx_http_client is None
    assert runtime.model_connectivity_tester is None
    assert runtime.llm_client is None
    assert runtime.engine is None
    assert runtime.session_factory is None
