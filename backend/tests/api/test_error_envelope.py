"""Structured error envelope contract tests."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient

from backend.api.sse import StreamHub
from backend.main import app


@pytest.fixture
async def api_client(session_factory) -> AsyncIterator[AsyncClient]:
    from backend.api.deps import get_session_factory

    app.dependency_overrides[get_session_factory] = lambda: session_factory
    from backend.tests.api.conftest import DisabledJobRunner

    app.state.runtime.session_factory = session_factory
    app.state.runtime.stream_hub = StreamHub()
    app.state.runtime.llm_client = object()
    app.state.runtime.model_connectivity_tester = object()
    app.state.runtime.job_runner = DisabledJobRunner()
    app.state.runtime.run_worker = type(
        "W",
        (),
        {
            "submit": staticmethod(lambda run_id: None),
            "start": staticmethod(lambda: None),
            "stop": staticmethod(lambda: None),
        },
    )()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_error_envelope_includes_request_id(api_client: AsyncClient) -> None:
    response = await api_client.get(
        "/api/aniu/runs/999999999",
        headers={"X-Request-ID": "req-test-123"},
    )
    assert response.status_code == 404
    body = response.json()
    assert body["error"]["code"] == "RunNotFoundError"
    assert body["error"]["request_id"] == "req-test-123"
    assert response.headers.get("X-Request-ID") == "req-test-123"


@pytest.mark.asyncio
async def test_request_validation_uses_shared_error_envelope(
    api_client: AsyncClient,
) -> None:
    response = await api_client.get(
        "/api/aniu/runs/not-an-integer",
        headers={"X-Request-ID": "req-validation-123"},
    )

    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "RequestValidationError"
    assert body["error"]["message"] == "request validation failed"
    assert body["error"]["request_id"] == "req-validation-123"
    assert body["error"]["details"]["errors"][0]["loc"] == ["path", "run_id"]
    assert response.headers["X-Request-ID"] == "req-validation-123"
