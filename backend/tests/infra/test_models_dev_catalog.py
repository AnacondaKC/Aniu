"""Tests for exact models.dev metadata lookup and HTTP caching."""

from __future__ import annotations

import json

import httpx
import pytest

from backend.infra.integrations.models_dev_catalog import ModelsDevCatalog


@pytest.mark.asyncio
async def test_lookup_reads_limits_and_provider_prices() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("If-None-Match") is None
        return httpx.Response(
            200,
            headers={"ETag": '"v1"'},
            json={
                "models": {
                    "openai/gpt-test": {
                        "name": "GPT Test",
                        "limit": {"context": 200_000, "output": 32_000},
                        "reasoning_options": [
                            {
                                "type": "effort",
                                "values": [
                                    "none",
                                    "minimal",
                                    "medium",
                                    "high",
                                    "default",
                                    None,
                                ],
                            }
                        ],
                    }
                },
                "providers": {
                    "openai": {
                        "models": {
                            "gpt-test": {
                                "name": "GPT Test",
                                "limit": {"context": 200_000, "output": 32_000},
                                "cost": {
                                    "input": 1.25,
                                    "output": 5,
                                    "cache_read": 0,
                                    "cache_write": 1.5,
                                },
                            }
                        }
                    }
                },
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        catalog = ModelsDevCatalog(client=client, catalog_url="https://catalog.test")
        model = await catalog.lookup("gpt-test")

    assert model is not None
    assert model.model_name == "gpt-test"
    assert model.provider_id == "openai/gpt-test"
    assert model.context_window_tokens == 200_000
    assert model.max_output_tokens == 32_000
    assert model.input_price_per_million == 1.25
    assert model.output_price_per_million == 5
    assert model.cache_read_price_per_million == 0
    assert model.cache_write_price_per_million == 1.5
    assert model.thinking_efforts == ("minimal", "medium", "high")


@pytest.mark.asyncio
async def test_lookup_prefers_provider_specific_reasoning_efforts() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "models": {
                    "anthropic/claude-test": {
                        "limit": {"context": 200_000, "output": 32_000},
                        "reasoning_options": [
                            {"type": "effort", "values": ["low", "medium", "high"]}
                        ],
                    }
                },
                "providers": {
                    "anthropic": {
                        "models": {
                            "claude-test": {
                                "limit": {"context": 200_000, "output": 32_000},
                                "reasoning_options": [
                                    {
                                        "type": "effort",
                                        "values": ["low", "xhigh", "max"],
                                    }
                                ],
                            }
                        }
                    }
                },
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        catalog = ModelsDevCatalog(client=client, catalog_url="https://catalog.test")
        model = await catalog.lookup("anthropic/claude-test")

    assert model is not None
    assert model.thinking_efforts == ("low", "xhigh", "max")


@pytest.mark.asyncio
async def test_lookup_does_not_fallback_from_explicit_non_effort_options() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "models": {
                    "provider/model": {
                        "limit": {"context": 200_000, "output": 32_000},
                        "reasoning_options": [
                            {"type": "effort", "values": ["low", "high"]}
                        ],
                    }
                },
                "providers": {
                    "provider": {
                        "models": {
                            "model": {
                                "limit": {"context": 200_000, "output": 32_000},
                                "reasoning_options": [
                                    {"type": "toggle"},
                                    {"type": "budget_tokens", "min": 1024},
                                ],
                            }
                        }
                    }
                },
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        catalog = ModelsDevCatalog(client=client, catalog_url="https://catalog.test")
        model = await catalog.lookup("provider/model")

    assert model is not None
    assert model.thinking_efforts == ()
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls > 1:
            assert request.headers["If-None-Match"] == '"v1"'
            return httpx.Response(304)
        return httpx.Response(
            200,
            headers={"ETag": '"v1"'},
            content=json.dumps(
                {
                    "models": {
                        "a/shared": {"limit": {"context": 1000, "output": 100}},
                        "b/shared": {"limit": {"context": 2000, "output": 200}},
                    },
                    "providers": {
                        "a": {"models": {"shared": {}}},
                        "b": {"models": {"shared": {}}},
                    },
                }
            ),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        catalog = ModelsDevCatalog(client=client, catalog_url="https://catalog.test")
        assert await catalog.lookup("shared") is None
        exact = await catalog.lookup("a/shared")

    assert exact is not None
    assert exact.context_window_tokens == 1000
    assert calls == 2
