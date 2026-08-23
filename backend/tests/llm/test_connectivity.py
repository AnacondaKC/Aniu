"""Tests for provider-specific model connectivity probes."""

from __future__ import annotations

import httpx
import pytest

from backend.llm import ModelConnectivityTester, ModelProtocol


@pytest.mark.asyncio
async def test_model_connectivity_tester_lists_openai_models() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        return httpx.Response(
            200,
            json={"data": [{"id": "gpt-4.1-mini"}, {"id": "gpt-4.1"}]},
        )

    tester = ModelConnectivityTester(transport=httpx.MockTransport(handler))
    result = await tester.list_models(
        protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        base_url="https://api.openai.com/v1",
        api_key="test-key",
    )

    assert captured["path"] == "/v1/models"
    assert [item.model for item in result] == ["gpt-4.1-mini", "gpt-4.1"]


@pytest.mark.asyncio
async def test_model_connectivity_tester_lists_claude_models() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["headers"] = dict(request.headers)
        return httpx.Response(
            200,
            json={"data": [{"id": "claude-sonnet-4-7"}, {"id": "claude-4-5"}]},
        )

    tester = ModelConnectivityTester(transport=httpx.MockTransport(handler))
    result = await tester.list_models(
        protocol=ModelProtocol.CLAUDE_API,
        base_url="https://api.anthropic.com/v1",
        api_key="test-key",
    )
    assert captured["path"] == "/v1/models"
    assert "x-api-key" in captured["headers"]
    assert [item.model for item in result] == ["claude-sonnet-4-7", "claude-4-5"]
