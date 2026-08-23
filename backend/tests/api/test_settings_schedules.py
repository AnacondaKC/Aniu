"""API tests for the retained settings, channel, and schedule contracts."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from backend.main import app
from backend.tests.api.conftest import FakeModelConnectivityTester


def channel_payload(*, name: str = "OpenAI Main") -> dict[str, object]:
    return {
        "name": name,
        "protocol": "openai_chat_completions",
        "model_name": "gpt-4.1-mini",
        "base_url": "https://llm.example.com/v1",
        "api_key": "test-key",
        "enabled": True,
        "sort_order": 10,
        "selected_models": [
            {
                "model_name": "gpt-4.1-mini",
                "label": "gpt-4.1-mini",
                "provider_id": "gpt-4.1-mini",
                "context_window_tokens": 128000,
                "max_output_tokens": 32768,
                "input_price_per_million": 0.4,
                "output_price_per_million": 1.6,
                "cache_read_price_per_million": 0.1,
                "cache_write_price_per_million": 0.5,
                "thinking_efforts": ["low", "high"],
                "sort_order": 0,
            }
        ],
    }


async def create_channel(client: AsyncClient) -> dict[str, object]:
    response = await client.post(
        "/api/aniu/settings/channels/with-models",
        json=channel_payload(),
    )
    assert response.status_code == 201
    return response.json()


@pytest.mark.asyncio
async def test_models_dev_lookup_returns_limits_and_prices(
    api_client: AsyncClient,
) -> None:
    response = await api_client.post(
        "/api/aniu/settings/models/models-dev/lookup",
        json={"model_name": "gpt-4.1-mini"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "model_name": "gpt-4.1-mini",
        "label": "GPT 4.1 Mini",
        "provider_id": "openai/gpt-4.1-mini",
        "context_window_tokens": 1_000_000,
        "max_output_tokens": 32_768,
        "thinking_efforts": ["low", "high"],
        "input_price_per_million": 0.4,
        "output_price_per_million": 1.6,
        "cache_read_price_per_million": 0.1,
        "cache_write_price_per_million": None,
    }


@pytest.mark.asyncio
async def test_channel_persists_model_limits_and_prices(
    api_client: AsyncClient,
) -> None:
    channel = await create_channel(api_client)
    model = channel["selected_models"][0]

    assert model["context_window_tokens"] == 128000
    assert model["max_output_tokens"] == 32768
    assert model["input_price_per_million"] == 0.4
    assert model["cache_write_price_per_million"] == 0.5
    assert model["thinking_efforts"] == ["low", "high"]


@pytest.mark.asyncio
async def test_default_settings_have_one_prompt_and_three_stage_models(
    api_client: AsyncClient,
) -> None:
    response = await api_client.get("/api/aniu/settings")

    assert response.status_code == 200
    body = response.json()
    assert body["dream_schedule_time"] == "00:30"
    assert body["prompt_profile"]["schema"] == "aniu.prompt-profile.v3"
    assert len(body["stage_settings"]) == 3
    assert {item["stage_id"] for item in body["stage_settings"]} == {
        "Run",
        "Summary",
        "Dream",
    }
    assert "default_selected_model_id" not in body
    assert "decision_selected_model_id" not in body
    assert all("max_tool_rounds" not in item for item in body["stage_settings"])
    assert all(item["thinking_effort"] is None for item in body["stage_settings"])
    assert body["mx"] == {"api_key_configured": False, "api_key_last_four": None}
    assert "mx_api_key_configured" not in body
    assert "mx_api_key_last_four" not in body
    assert "context_window_tokens" not in body
    assert all("max_output_tokens" not in item for item in body["stage_settings"])
    assert all("render_mode" not in item for item in body["stage_settings"])
    assert all("html_prompt" not in item for item in body["stage_settings"])


@pytest.mark.asyncio
async def test_settings_update_persists_prompt_and_stage_models(
    api_client: AsyncClient,
) -> None:
    channel = await create_channel(api_client)
    selected_model_id = channel["selected_models"][0]["selected_model_id"]
    current = (await api_client.get("/api/aniu/settings")).json()
    stages = current["stage_settings"]
    for stage in stages:
        stage["model_selected_model_id"] = selected_model_id
        stage["thinking_effort"] = "high"
    current["prompt_profile"]["global_prompt"] = "全局风险约束"
    current["dream_schedule_time"] = "04:15"

    updated = await api_client.put(
        "/api/aniu/settings",
        json={
            "expected_revision": current["revision"],
            "dream_schedule_time": current["dream_schedule_time"],
            "prompt_profile": current["prompt_profile"],
            "stage_settings": stages,
        },
    )

    assert updated.status_code == 200
    body = updated.json()
    assert body["prompt_profile"]["global_prompt"] == "全局风险约束"
    assert body["dream_schedule_time"] == "04:15"
    assert "context_window_tokens" not in body
    assert all(
        item["model_selected_model_id"] == selected_model_id
        for item in body["stage_settings"]
    )
    assert all(item["thinking_effort"] == "high" for item in body["stage_settings"])
    assert all("max_output_tokens" not in item for item in body["stage_settings"])
    assert all("render_mode" not in item for item in body["stage_settings"])
    assert all("html_prompt" not in item for item in body["stage_settings"])

    reloaded = await api_client.get("/api/aniu/settings")
    assert reloaded.status_code == 200
    assert reloaded.json()["dream_schedule_time"] == "04:15"
    assert len(reloaded.json()["stage_settings"]) == 3


@pytest.mark.asyncio
async def test_settings_reject_invalid_dream_schedule_time(
    api_client: AsyncClient,
) -> None:
    current = (await api_client.get("/api/aniu/settings")).json()

    response = await api_client.put(
        "/api/aniu/settings",
        json={
            "expected_revision": current["revision"],
            "dream_schedule_time": "24:00",
        },
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_channel_model_effort_removal_resets_affected_stage_settings(
    api_client: AsyncClient,
) -> None:
    channel = await create_channel(api_client)
    selected_model_id = channel["selected_models"][0]["selected_model_id"]
    current = (await api_client.get("/api/aniu/settings")).json()
    stages = [
        {
            **stage,
            "model_selected_model_id": selected_model_id,
            "thinking_effort": "high",
        }
        for stage in current["stage_settings"]
    ]
    saved_settings = await api_client.put(
        "/api/aniu/settings",
        json={
            "expected_revision": current["revision"],
            "stage_settings": stages,
        },
    )
    assert saved_settings.status_code == 200

    updated_channel = await api_client.put(
        f"/api/aniu/settings/channels/{channel['profile_id']}/with-models",
        json={
            **channel_payload(),
            "expected_revision": channel["revision"],
            "selected_models": [
                {
                    **channel_payload()["selected_models"][0],
                    "thinking_efforts": [],
                }
            ],
        },
    )
    assert updated_channel.status_code == 200

    reloaded = await api_client.get("/api/aniu/settings")
    assert reloaded.status_code == 200
    assert all(
        stage["thinking_effort"] is None for stage in reloaded.json()["stage_settings"]
    )
    current = (await api_client.get("/api/aniu/settings")).json()
    updated_profile = {**current["prompt_profile"], "description": "updated"}
    updated = await api_client.put(
        "/api/aniu/settings",
        json={
            "expected_revision": current["revision"],
            "prompt_profile": updated_profile,
        },
    )
    stale = await api_client.put(
        "/api/aniu/settings",
        json={
            "expected_revision": current["revision"],
            "prompt_profile": updated_profile,
        },
    )

    assert updated.status_code == 200
    assert stale.status_code == 409
    assert stale.json()["error"]["details"]["resource"] == "app_settings"


@pytest.mark.asyncio
async def test_settings_update_rejects_removed_tool_loop_limit(
    api_client: AsyncClient,
) -> None:
    current = (await api_client.get("/api/aniu/settings")).json()

    response = await api_client.put(
        "/api/aniu/settings",
        json={
            "expected_revision": current["revision"],
            "max_tool_loop_iterations": 1,
        },
    )

    assert response.status_code == 422


def test_update_settings_openapi_omits_removed_tool_loop_limit() -> None:
    schema = app.openapi()["components"]["schemas"]["UpdateSettingsRequest"]

    assert "max_tool_loop_iterations" not in schema["properties"]
    assert "context_window_tokens" not in schema["properties"]
    assert "decision_max_rollbacks" not in schema["properties"]


@pytest.mark.asyncio
async def test_settings_update_rejects_removed_stage_output_limit(
    api_client: AsyncClient,
) -> None:
    current = (await api_client.get("/api/aniu/settings")).json()
    stage_settings = [
        {**stage, "max_output_tokens": 1000} for stage in current["stage_settings"]
    ]

    response = await api_client.put(
        "/api/aniu/settings",
        json={
            "expected_revision": current["revision"],
            "stage_settings": stage_settings,
        },
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_settings_update_rejects_removed_global_context_window(
    api_client: AsyncClient,
) -> None:
    current = (await api_client.get("/api/aniu/settings")).json()

    response = await api_client.put(
        "/api/aniu/settings",
        json={
            "expected_revision": current["revision"],
            "context_window_tokens": 256000,
        },
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_channel_update_rejects_zero_revision(api_client: AsyncClient) -> None:
    created = await create_channel(api_client)

    response = await api_client.put(
        f"/api/aniu/settings/channels/{created['profile_id']}/with-models",
        json={**channel_payload(), "expected_revision": 0},
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_channel_aggregate_update_and_secret_redaction(
    api_client: AsyncClient,
) -> None:
    created = await create_channel(api_client)
    assert created["api_key_configured"] is True
    assert "api_key" not in created

    updated = await api_client.put(
        f"/api/aniu/settings/channels/{created['profile_id']}/with-models",
        json={
            **channel_payload(name="OpenAI Updated"),
            "api_key": None,
            "provider_config": {
                "auth_mode": "api_key",
                "openai": {
                    "max_tokens_field": "max_completion_tokens",
                    "supports_temperature": False,
                    "supports_top_p": True,
                    "supports_stream_usage": False,
                    "replay_reasoning_content": True,
                },
            },
            "expected_revision": created["revision"],
        },
    )

    assert updated.status_code == 200
    assert updated.json()["name"] == "OpenAI Updated"
    assert updated.json()["api_key_configured"] is True
    assert updated.json()["provider_config"] == {
        "auth_mode": "api_key",
        "openai": {
            "max_tokens_field": "max_completion_tokens",
            "supports_temperature": False,
            "supports_top_p": True,
            "supports_stream_usage": False,
            "replay_reasoning_content": True,
        },
    }
    assert updated.json()["revision"] > created["revision"]


@pytest.mark.asyncio
async def test_model_catalog_uses_stored_secret_when_request_omits_it(
    api_client: AsyncClient,
    fake_model_tester: FakeModelConnectivityTester,
) -> None:
    channel = await create_channel(api_client)

    response = await api_client.post(
        f"/api/aniu/settings/channels/{channel['profile_id']}/models/fetch",
        json={
            "llm_protocol": "openai_chat_completions",
            "llm_base_url": "https://llm.example.com/v1",
            "llm_api_key": None,
        },
    )

    assert response.status_code == 200
    assert response.json()[0]["model"] == "gpt-4.1-mini"
    assert fake_model_tester.calls[-1]["api_key"] == "test-key"


@pytest.mark.asyncio
async def test_channel_clear_and_delete_are_revision_guarded(
    api_client: AsyncClient,
) -> None:
    channel = await create_channel(api_client)
    channel_id = channel["profile_id"]
    stale = await api_client.delete(
        f"/api/aniu/settings/channels/{channel_id}/api-key",
        params={"expected_revision": channel["revision"] + 1},
    )
    cleared = await api_client.delete(
        f"/api/aniu/settings/channels/{channel_id}/api-key",
        params={"expected_revision": channel["revision"]},
    )
    deleted = await api_client.delete(
        f"/api/aniu/settings/channels/{channel_id}",
        params={"expected_revision": cleared.json()["revision"]},
    )

    assert stale.status_code == 409
    assert cleared.status_code == 200
    assert cleared.json()["api_key_configured"] is False
    assert deleted.status_code == 204


@pytest.mark.asyncio
async def test_settings_expose_mx_configuration_and_public_catalog(
    api_client: AsyncClient,
) -> None:
    settings_response = await api_client.get("/api/aniu/settings")
    stock_api_response = await api_client.get("/api/aniu/settings/stock-api")

    assert settings_response.status_code == 200
    assert settings_response.json()["mx"] == {
        "api_key_configured": False,
        "api_key_last_four": None,
    }
    assert stock_api_response.status_code == 200
    body = stock_api_response.json()
    assert set(body) == {"mx", "public_stock"}
    assert set(body["mx"]) == {"interfaces"}
    assert set(body["public_stock"]) == {
        "name",
        "summary",
        "providers",
        "features",
        "tools",
    }
    assert len(body["public_stock"]["tools"]) == 12


@pytest.mark.asyncio
async def test_mx_key_is_managed_by_general_settings_without_daily_quota(
    api_client: AsyncClient,
) -> None:
    current = (await api_client.get("/api/aniu/settings")).json()

    rejected = await api_client.put(
        "/api/aniu/settings",
        json={
            "expected_revision": current["revision"],
            "mx_daily_limits": {"news": 80},
        },
    )
    updated = await api_client.put(
        "/api/aniu/settings",
        json={
            "expected_revision": current["revision"],
            "mx_api_key": "test-only",
        },
    )
    cleared = await api_client.put(
        "/api/aniu/settings",
        json={
            "expected_revision": updated.json()["revision"],
            "mx_api_key": None,
        },
    )

    assert rejected.status_code == 422
    assert updated.status_code == 200
    assert updated.json()["mx"] == {
        "api_key_configured": True,
        "api_key_last_four": "only",
    }
    assert cleared.status_code == 200
    assert cleared.json()["mx"] == {
        "api_key_configured": False,
        "api_key_last_four": None,
    }


@pytest.mark.asyncio
async def test_stock_api_settings_expose_detailed_mx_interfaces_without_quota_endpoints(
    api_client: AsyncClient,
) -> None:
    response = await api_client.get("/api/aniu/settings/stock-api")
    assert response.status_code == 200
    interfaces = response.json()["mx"]["interfaces"]
    assert [item["id"] for item in interfaces] == [
        "news",
        "data",
        "screening",
        "portfolio",
    ]
    for item in interfaces:
        assert item["name"]
        assert item["summary"]
        assert item["features"]
        assert item["examples"]
        assert item["access_modes"]
    assert interfaces[-1]["id"] == "portfolio"
    assert interfaces[-1]["name"] == "模拟交易"
    assert interfaces[-1]["access_modes"] == ["read", "write"]
    assert all(item["access_modes"] == ["read"] for item in interfaces[:-1])

    update_response = await api_client.put("/api/aniu/settings/stock-api", json={})
    usage_response = await api_client.get("/api/aniu/settings/stock-api/mx-usage")

    assert update_response.status_code == 405
    assert usage_response.status_code == 404


@pytest.mark.asyncio
async def test_schedule_create_update_and_sync_state(api_client: AsyncClient) -> None:
    created = await api_client.post(
        "/api/aniu/schedules",
        json={
            "enabled": True,
            "task_type": "market_analysis",
            "interval_minutes": 30,
        },
    )
    updated = await api_client.put(
        f"/api/aniu/schedules/{created.json()['schedule_id']}",
        json={
            "enabled": False,
            "task_type": "market_analysis",
            "interval_minutes": 45,
            "expected_revision": created.json()["revision"],
        },
    )

    assert created.status_code == 201
    assert created.json()["runtime_synced_revision"] == created.json()["revision"]
    assert updated.status_code == 200
    assert updated.json()["interval_minutes"] == 45
    assert updated.json()["runtime_synced_revision"] == updated.json()["revision"]
    assert "cron_expression" not in updated.json()
    assert "next_run_at" not in updated.json()


@pytest.mark.asyncio
async def test_schedule_update_rejects_stale_revision(api_client: AsyncClient) -> None:
    body = {
        "enabled": True,
        "task_type": "market_analysis",
        "interval_minutes": 30,
    }
    created = await api_client.post("/api/aniu/schedules", json=body)
    first = await api_client.put(
        f"/api/aniu/schedules/{created.json()['schedule_id']}",
        json={**body, "expected_revision": created.json()["revision"]},
    )
    stale = await api_client.put(
        f"/api/aniu/schedules/{created.json()['schedule_id']}",
        json={**body, "expected_revision": created.json()["revision"]},
    )

    assert first.status_code == 200
    assert stale.status_code == 409


@pytest.mark.asyncio
async def test_schedule_custom_times_create_update_and_fallback(
    api_client: AsyncClient,
) -> None:
    created = await api_client.post(
        "/api/aniu/schedules",
        json={
            "enabled": True,
            "task_type": "market_analysis",
            "interval_minutes": 30,
            "schedule_times": ["09:30", "14:00"],
        },
    )
    assert created.status_code == 201
    created_body = created.json()
    assert created_body["custom_schedule_times"] == ["09:30", "14:00"]
    assert created_body["schedule_times"] == ["09:30", "14:00"]

    updated = await api_client.put(
        f"/api/aniu/schedules/{created_body['schedule_id']}",
        json={
            "enabled": True,
            "task_type": "market_analysis",
            "interval_minutes": 30,
            "schedule_times": ["10:00"],
            "expected_revision": created_body["revision"],
        },
    )
    assert updated.status_code == 200
    assert updated.json()["custom_schedule_times"] == ["10:00"]
    assert updated.json()["schedule_times"] == ["10:00"]

    fallback = await api_client.put(
        f"/api/aniu/schedules/{created_body['schedule_id']}",
        json={
            "enabled": True,
            "task_type": "market_analysis",
            "interval_minutes": 60,
            "schedule_times": None,
            "expected_revision": updated.json()["revision"],
        },
    )
    assert fallback.status_code == 200
    assert fallback.json()["custom_schedule_times"] is None
    assert fallback.json()["schedule_times"] == ["09:30", "10:30", "13:00", "14:00"]


@pytest.mark.asyncio
async def test_schedule_custom_times_rejects_invalid_format(
    api_client: AsyncClient,
) -> None:
    invalid = await api_client.post(
        "/api/aniu/schedules",
        json={
            "enabled": True,
            "task_type": "market_analysis",
            "interval_minutes": 30,
            "schedule_times": ["9:30", "25:00"],
        },
    )

    assert invalid.status_code == 422


@pytest.mark.asyncio
async def test_removed_and_legacy_configuration_surfaces_are_rejected(
    api_client: AsyncClient,
) -> None:
    settings = await api_client.get("/api/aniu/settings")
    legacy_write = await api_client.put(
        "/api/aniu/settings",
        json={
            "expected_revision": settings.json()["revision"],
            "decision_temperature": 0.2,
        },
    )
    legacy_prompt = await api_client.get("/api/aniu/settings/prompt-profiles")
    legacy_schedule = await api_client.post(
        "/api/aniu/schedules",
        json={
            "name": "legacy",
            "task_type": "market_analysis",
            "interval_minutes": 30,
            "schedule_time": "15:00",
        },
    )

    assert legacy_write.status_code == 422
    assert legacy_prompt.status_code == 404
    assert legacy_schedule.status_code == 422
