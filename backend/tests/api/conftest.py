"""Shared API test fixtures and fakes."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import replace

import pytest
from httpx import ASGITransport, AsyncClient

from backend.api.deps import get_session_factory
from backend.api.sse import StreamHub
from backend.business.account import AccountSnapshot
from backend.business.settings import (
    AppSettings,
    ModelProfile,
    ModelsDevModel,
    SelectedModel,
    default_stage_settings,
)
from backend.infra.repositories import (
    ModelProfileRepository,
    ScheduleRepository,
    SelectedModelRepository,
    SettingsRepository,
)
from backend.infra.workers.run_worker import build_run_worker
from backend.llm import ModelCatalogItem, ModelProtocol
from backend.main import app
from backend.stock_api import MxClients

TEST_AUTH_TOKEN = "test-auth-token"


async def authenticate_api_client(client: AsyncClient) -> None:
    response = await client.post(
        "/api/aniu/auth/setup",
        json={"token": TEST_AUTH_TOKEN},
    )
    assert response.status_code == 201
    client.headers["X-CSRF-Token"] = response.json()["csrf_token"]


class DisabledJobRunner:
    """No-op schedule runner for API tests that do not assert scheduler side effects."""

    async def sync_schedule(self, schedule: object) -> None:
        del schedule

    async def sync_all(self, schedules: object) -> None:
        del schedules

    async def remove_schedule(self, schedule_id: int) -> None:
        del schedule_id


class FakeDreamWorker:
    def __init__(self) -> None:
        self.submitted: list[int] = []

    async def submit(self, task_id: int) -> None:
        self.submitted.append(task_id)


class FakeJobRunner:
    def __init__(self, session_factory) -> None:
        self._session_factory = session_factory
        self.synced: list[int] = []

    async def sync_schedule(self, schedule) -> None:
        async with self._session_factory() as session:
            persisted = await ScheduleRepository(session).get_by_id(
                schedule.schedule_id
            )
        assert persisted is not None
        assert persisted.revision == schedule.revision
        assert persisted.enabled == schedule.enabled
        assert persisted.interval_minutes == schedule.interval_minutes
        self.synced.append(schedule.schedule_id)

    async def sync_all(self, schedules) -> None:
        del schedules


class FakeModelConnectivityTester:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def list_models(
        self,
        *,
        protocol,
        base_url,
        api_key,
        provider_config=None,
    ) -> list[ModelCatalogItem]:
        self.calls.append(
            {
                "protocol": protocol,
                "base_url": base_url,
                "api_key": api_key,
                "provider_config": provider_config,
            }
        )
        return [
            ModelCatalogItem(
                model="gpt-4.1-mini",
                label="gpt-4.1-mini",
                provider_id="gpt-4.1-mini",
            )
        ]


class FakeModelsDevCatalog:
    async def lookup(self, model_name: str) -> ModelsDevModel | None:
        if model_name.casefold() not in {"gpt-4.1-mini", "openai/gpt-4.1-mini"}:
            return None
        return ModelsDevModel(
            model_name=model_name,
            label="GPT 4.1 Mini",
            provider_id="openai/gpt-4.1-mini",
            context_window_tokens=1_000_000,
            max_output_tokens=32_768,
            input_price_per_million=0.4,
            output_price_per_million=1.6,
            cache_read_price_per_million=0.1,
            thinking_efforts=("low", "high"),
        )


@pytest.fixture
def fake_model_tester() -> FakeModelConnectivityTester:
    return FakeModelConnectivityTester()


@pytest.fixture
async def api_client(session_factory, fake_model_tester) -> AsyncIterator[AsyncClient]:
    async with session_factory() as session:
        await SettingsRepository(session).save(AppSettings())
        await session.commit()
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    app.state.runtime.session_factory = session_factory
    app.state.runtime.model_connectivity_tester = fake_model_tester
    app.state.runtime.models_dev_catalog = FakeModelsDevCatalog()
    app.state.runtime.job_runner = FakeJobRunner(session_factory)
    app.state.runtime.dream_worker = FakeDreamWorker()

    async def resolve_mx_api_key() -> str | None:
        return "test-only"

    mx_clients = MxClients.create(api_key_resolver=resolve_mx_api_key)
    app.state.runtime.mx_clients = mx_clients
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await authenticate_api_client(client)
        yield client
    app.dependency_overrides.clear()
    await mx_clients.aclose()
    app.state.runtime.mx_clients = None
    app.state.runtime.models_dev_catalog = None
    app.state.runtime.dream_worker = None


class FakeMxMoniClient:
    async def get_account_snapshot(self) -> AccountSnapshot:
        return AccountSnapshot(
            total_asset=100000.0,
            available_cash=25000.0,
            frozen_cash=0.0,
            market_value=75000.0,
            total_profit=5000.0,
            daily_profit=300.0,
        )

    async def get_positions(self) -> list[object]:
        return []

    async def get_orders(self) -> list[object]:
        return []


class FakeLLMClient:
    async def chat(self, **kwargs) -> dict[str, object]:
        messages = kwargs.get("messages") or []
        prompt = "\n".join(
            str(item.get("content") or "")
            for item in messages
            if isinstance(item, dict)
        )
        if "Active stage: Run" in prompt or "run_stage_payload" in prompt:
            return {"content": "# 分析报告\n继续观察。", "tool_calls": []}
        if "Active stage: Summary" in prompt or "summary_stage_payload" in prompt:
            return {
                "content": (
                    "<article><h1>运行总结</h1><p>本轮未执行交易。</p></article>"
                ),
                "tool_calls": [],
            }
        return {"content": "不需要补充工具材料。", "tool_calls": []}

    async def generate_text(self, **kwargs) -> str:
        prompt = kwargs["user_prompt"]
        if "Summary 阶段" in prompt:
            return "<article><h1>运行总结</h1><p>本轮未执行交易。</p></article>"
        raise AssertionError(f"unexpected prompt: {prompt}")


async def save_default_model_settings(session_factory) -> None:
    async with session_factory() as session:
        profile = await ModelProfileRepository(session).create(
            ModelProfile(
                name="默认运行渠道",
                protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
                model_name="gpt-4.1-mini",
                base_url="https://llm.example.com/v1",
                api_key="test-key",
            )
        )
        selected = (
            await SelectedModelRepository(session).replace_for_channel(
                profile.profile_id,
                [
                    SelectedModel(
                        channel_profile_id=profile.profile_id,
                        model_name="gpt-4.1-mini",
                        label="gpt-4.1-mini",
                        provider_id="gpt-4.1-mini",
                    )
                ],
            )
        )[0]
        stages = {
            stage_id: replace(
                stage,
                model_selected_model_id=selected.selected_model_id,
            )
            for stage_id, stage in default_stage_settings().items()
        }
        await SettingsRepository(session).save(AppSettings(stage_settings=stages))
        await session.commit()


@pytest.fixture
async def run_api_client(session_factory) -> AsyncIterator[AsyncClient]:
    """API client with model settings, worker, and LLM/MX fakes for run/SSE tests."""

    await save_default_model_settings(session_factory)
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    app.state.runtime.session_factory = session_factory
    app.state.runtime.mx_clients = None
    mx_http_client = AsyncClient()
    app.state.runtime.mx_http_client = mx_http_client
    stream_hub = StreamHub()
    app.state.runtime.stream_hub = stream_hub
    app.state.runtime.llm_client = FakeLLMClient()  # type: ignore[assignment]
    app.state.runtime.model_connectivity_tester = object()
    app.state.runtime.job_runner = DisabledJobRunner()
    run_worker = build_run_worker(
        session_factory=session_factory,
        executor_factory=app.state.runtime.run_executor,
        abort_registry=app.state.runtime.abort_registry,
    )
    run_worker.start()
    app.state.runtime.run_worker = run_worker
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await authenticate_api_client(client)
        yield client
    await run_worker.stop()
    app.state.runtime.mx_clients = None
    app.state.runtime.mx_http_client = None
    await mx_http_client.aclose()
    app.dependency_overrides.clear()
