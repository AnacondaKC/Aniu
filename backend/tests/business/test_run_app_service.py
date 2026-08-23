"""Tests for the run application service trace snapshot flow."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from backend.business.account import AccountSnapshot, PortfolioOrderSnapshot
from backend.business.runs import (
    RunJob,
    RunJobStatus,
    StrategyRun,
    StrategySnapshot,
    build_run_id,
)
from backend.business.runs.abort_registry import ActiveRunAbortRegistry
from backend.business.runs.commands import StartRunCommand
from backend.business.runs.dto import RunDetailDTO, to_run_detail_dto
from backend.business.runs.executor import RunExecutor
from backend.business.runs.queries import ListRunsQuery
from backend.business.runs.service import RunService
from backend.business.settings import (
    AppSettings,
    ModelProfile,
    SelectedModel,
    default_stage_settings,
)
from backend.business.shared import ConcurrentRunError, RunNotFoundError
from backend.business.shared.enums import RunStatus, TriggerSource
from backend.infra.integrations.agent_runner import AgentRunnerFactoryAdapter
from backend.infra.integrations.agent_runtime import AgentRuntimeFactory
from backend.llm import ModelProtocol

FIXED_NOW = datetime(2026, 5, 8, 9, 30, tzinfo=UTC)


class NullRunJobRepo:
    """No-op job repository for unit tests that only exercise RunAppService."""

    def __init__(self, cancel_result: RunJob | None = None) -> None:
        self._cancel_result = cancel_result

    async def get_active_job(self) -> None:
        return None

    async def get_by_run_id(self, run_id: int) -> None:
        del run_id
        return None

    async def create_pending(self, run_id: int) -> None:
        del run_id
        return None

    async def request_cancel(self, run_id: int, *, reason: str) -> RunJob | None:
        del run_id, reason
        return self._cancel_result


def expected_run_id(sequence: int = 1) -> int:
    return build_run_id(FIXED_NOW.date(), sequence=sequence)


def make_configured_settings() -> AppSettings:
    return AppSettings(
        prompt_profile={
            "name": "测试提示词",
            "global_prompt": "整体稳健。",
            "run_prompt": "偏稳健。",
        },
        stage_settings={
            stage_id: replace(
                stage,
                model_selected_model_id=1,
                prompt="偏稳健。" if stage_id == "Run" else stage.prompt,
            )
            for stage_id, stage in default_stage_settings().items()
        },
    )


class InMemoryRunRepository:
    def __init__(self) -> None:
        self.runs: dict[int, StrategyRun] = {}

    async def next_run_id(self, reference_date=None, task_type: int = 1) -> int:
        del task_type
        target_date = FIXED_NOW.date() if reference_date is None else reference_date
        existing = [
            run_id
            for run_id in self.runs
            if str(run_id).startswith(f"{target_date:%Y%m%d}1")
        ]
        if not existing:
            return build_run_id(target_date, sequence=1)
        max_sequence = max(int(str(run_id)[9:]) for run_id in existing)
        return build_run_id(target_date, sequence=max_sequence + 1)

    async def get_running_run(self) -> StrategyRun | None:
        running_runs = [
            run for run in self.runs.values() if run.status is RunStatus.RUNNING
        ]
        if not running_runs:
            return None
        return sorted(running_runs, key=lambda run: run.run_id)[0]

    async def add(self, run: StrategyRun) -> StrategyRun:
        self.runs[run.run_id] = run
        return run

    async def save(self, run: StrategyRun) -> StrategyRun:
        self.runs[run.run_id] = run
        return run

    async def get_by_id(self, run_id: int) -> StrategyRun | None:
        return self.runs.get(run_id)

    async def list_runs(
        self, limit: int = 100, offset: int = 0, started_date=None
    ) -> list[StrategyRun]:
        runs = [self.runs[run_id] for run_id in sorted(self.runs, reverse=True)]
        if started_date is not None:
            runs = [
                run
                for run in runs
                if (run.completed_at or run.started_at).date() == started_date
            ]
        return runs[offset : offset + limit]

    async def list_run_summaries(
        self, limit: int = 100, offset: int = 0, started_date=None
    ) -> list[dict[str, object]]:
        from backend.business.runs.dto import to_run_summary_dto

        return [
            {
                "run_id": item.run_id,
                "trigger_source": item.trigger_source,
                "schedule_id": item.schedule_id,
                "status": item.status,
                "current_state": item.current_state,
                "summary": item.summary,
                "summary_render_mode": item.summary_render_mode,
                "started_at": item.started_at,
                "completed_at": item.completed_at,
                "tool_calls_count": item.tool_calls_count,
                "thinking_count": item.thinking_count,
                "total_tokens": item.total_tokens,
                "trade_count": item.trade_count,
            }
            for item in (
                to_run_summary_dto(run)
                for run in await self.list_runs(
                    limit=limit, offset=offset, started_date=started_date
                )
            )
        ]

    async def delete(self, run_id: int) -> None:
        self.runs.pop(run_id, None)


class InMemorySettingsRepository:
    def __init__(self, settings: AppSettings | None = None) -> None:
        self.settings = settings

    async def get(self) -> AppSettings | None:
        return self.settings

    async def save(self, settings: AppSettings) -> AppSettings:
        self.settings = settings
        return settings


class InMemoryModelProfileRepository:
    def __init__(self) -> None:
        self.profiles: dict[int, ModelProfile] = {}
        self.get_by_id_calls = 0
        self.get_by_ids_calls = 0

    async def get_by_id(self, profile_id: int) -> ModelProfile | None:
        self.get_by_id_calls += 1
        return self.profiles.get(profile_id)

    async def get_by_ids(self, profile_ids: set[int]) -> dict[int, ModelProfile]:
        self.get_by_ids_calls += 1
        return {
            profile_id: profile
            for profile_id in profile_ids
            if (profile := self.profiles.get(profile_id)) is not None
        }


class InMemorySelectedModelRepository:
    def __init__(self) -> None:
        self.models: dict[int, SelectedModel] = {}
        self.get_by_id_calls = 0
        self.get_by_ids_calls = 0

    async def get_by_id(self, selected_model_id: int) -> SelectedModel | None:
        self.get_by_id_calls += 1
        return self.models.get(selected_model_id)

    async def get_by_ids(
        self, selected_model_ids: set[int]
    ) -> dict[int, SelectedModel]:
        self.get_by_ids_calls += 1
        return {
            selected_model_id: model
            for selected_model_id in selected_model_ids
            if (model := self.models.get(selected_model_id)) is not None
        }


def make_configured_model_repositories() -> tuple[
    InMemorySettingsRepository,
    InMemoryModelProfileRepository,
    InMemorySelectedModelRepository,
]:
    profile_repo = InMemoryModelProfileRepository()
    profile_repo.profiles[1] = ModelProfile(
        profile_id=1,
        name="Configured OpenAI",
        protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
        model_name="gpt-4.1-mini",
        base_url="https://llm.example.com/v1",
        api_key="test-key",
    )
    selected_model_repo = InMemorySelectedModelRepository()
    selected_model_repo.models[1] = SelectedModel(
        selected_model_id=1,
        channel_profile_id=1,
        model_name="gpt-4.1-mini",
        label="gpt-4.1-mini",
        provider_id="gpt-4.1-mini",
    )
    return (
        InMemorySettingsRepository(make_configured_settings()),
        profile_repo,
        selected_model_repo,
    )


class FakePortfolioExecutionClient:
    def __init__(self) -> None:
        self.buy_requests: list[tuple[str, int]] = []

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

    async def get_orders(self) -> list[PortfolioOrderSnapshot]:
        if not self.buy_requests:
            return []
        return [
            PortfolioOrderSnapshot(
                order_id="trade-order-1",
                symbol="000001",
                stock_name="平安银行",
                direction="BUY",
                quantity=100,
                order_price=10.0,
                status="FILLED",
                filled_quantity=100,
                filled_price=10.0,
            )
        ]

    async def buy(self, symbol: str, quantity: int) -> dict[str, object]:
        self.buy_requests.append((symbol, quantity))
        return {"orderId": "trade-order-1"}

    async def sell(self, symbol: str, quantity: int) -> dict[str, object]:
        self.buy_requests.append((symbol, quantity))
        return {"orderId": "trade-order-1"}

    async def cancel_order(
        self, order_id: str, stock_code: str | None = None
    ) -> dict[str, object]:
        del order_id, stock_code
        return {"ok": True}


class FakeMarketDataQuery:
    async def query(self, tool_query: str) -> dict[str, object]:
        return {"query": tool_query, "summary": f"market::{tool_query}"}


class FakeNewsSearch:
    async def search(self, query: str) -> dict[str, object]:
        return {
            "query": query,
            "items": [
                {
                    "title": f"{query}-标题",
                    "summary": f"{query}-摘要",
                    "publishedAt": "2026-04-27T10:00:00+08:00",
                    "source": "测试来源",
                }
            ],
        }


class NullSummaryRepo:
    async def save(self, review):
        return review


class FakeLLMClient:
    async def chat(self, **kwargs) -> dict[str, object]:
        messages = kwargs.get("messages") or []
        prompt = "\n".join(
            str(item.get("content") or "")
            for item in messages
            if isinstance(item, dict)
        )
        if "summary_source_data" in prompt:
            return {
                "content": "<section><p>本轮运行完成，未执行交易。</p></section>",
                "tool_calls": [],
            }
        if "偏稳健" in prompt:
            return {
                "content": "# 分析报告\n\n## 此轮观察\n继续观察。",
                "tool_calls": [],
            }
        return {"content": "不需要补充工具材料。", "tool_calls": []}

    async def generate_text(self, **kwargs) -> str:
        prompt = kwargs["user_prompt"]
        if "summary_source_data" in prompt:
            return "<section><p>本轮运行完成，未执行交易。</p></section>"
        return "# 分析报告\n\n## 此轮观察\n继续观察。"


class FailingResearchLLMClient(FakeLLMClient):
    async def chat(self, **kwargs) -> dict[str, object]:
        del kwargs
        raise RuntimeError("研究模型连接超时")


class SegmentedResearchLLMClient(FakeLLMClient):
    def __init__(self) -> None:
        self.research_calls = 0

    async def chat(self, **kwargs) -> dict[str, object]:
        messages = kwargs["messages"]
        first_prompt = "\n".join(
            str(item.get("content") or "")
            for item in messages
            if isinstance(item, dict)
        )
        if "decision_stage_payload" in first_prompt:
            return await super().chat(**kwargs)
        if (
            "偏稳健" in first_prompt
            or "research_payload" in first_prompt
            or "研究阶段" in first_prompt
            or "研究阶段提示词" in first_prompt
        ):
            callback = kwargs.get("on_reasoning_delta")
            if self.research_calls == 0:
                self.research_calls += 1
                if callable(callback):
                    await callback("先梳理市场环境，再按需补充客观数据。")
                return {
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "name": "query_market_data",
                            "arguments": {
                                "query": "上证指数、深证成指、创业板指最新点位和涨跌幅",
                            },
                        },
                        {
                            "id": "call-2",
                            "name": "search_news",
                            "arguments": {
                                "query": "A股市场热点板块与政策消息",
                            },
                        },
                    ],
                }

            if callable(callback):
                await callback("结合已获取的市场数据和资讯，整理研究结论。")
            return {
                "content": "# 分析报告\n\n## 此轮观察\n市场情绪一般，继续观察。",
                "tool_calls": [],
            }

        return await super().chat(**kwargs)


class StreamingResearchResultBeforeToolLLMClient(FakeLLMClient):
    def __init__(self) -> None:
        self.research_calls = 0

    async def chat(self, **kwargs) -> dict[str, object]:
        messages = kwargs["messages"]
        first_prompt = "\n".join(
            str(item.get("content") or "")
            for item in messages
            if isinstance(item, dict)
        )
        if "decision_stage_payload" in first_prompt:
            return await super().chat(**kwargs)
        if (
            "偏稳健" in first_prompt
            or "research_payload" in first_prompt
            or "研究阶段" in first_prompt
            or "研究阶段提示词" in first_prompt
        ):
            callback = kwargs.get("on_text_delta")
            if self.research_calls == 0:
                self.research_calls += 1
                if callable(callback):
                    await callback("# 分析报告\n\n")
                return {
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "call-early-tool",
                            "name": "query_market_data",
                            "arguments": {"query": "上证指数 最新点位"},
                        },
                    ],
                }

            if callable(callback):
                await callback("## 此轮观察\n工具后整理。")
            return {
                "content": "# 分析报告\n\n## 此轮观察\n工具后整理。",
                "tool_calls": [],
            }

        return await super().chat(**kwargs)


class StreamingSummaryLLMClient(FakeLLMClient):
    async def chat(self, **kwargs) -> dict[str, object]:
        messages = kwargs.get("messages") or []
        prompt = "\n".join(
            str(item.get("content") or "")
            for item in messages
            if isinstance(item, dict)
        )
        if "summary_source_data" in prompt:
            callback = kwargs.get("on_text_delta")
            if callable(callback):
                await callback("<section><p>本轮运行完成")
            return {
                "content": "<section><p>本轮运行完成，未执行交易。</p></section>",
                "tool_calls": [],
            }
        return await super().chat(**kwargs)

    async def generate_text(self, **kwargs) -> str:
        prompt = kwargs["user_prompt"]
        if "summary_source_data" in prompt:
            callback = kwargs.get("on_text_delta")
            if callable(callback):
                await callback("<section><p>本轮运行完成")
            return "<section><p>本轮运行完成，未执行交易。</p></section>"
        return await super().generate_text(**kwargs)


class SnapshotCollector:
    def __init__(self) -> None:
        self.snapshots: list[object] = []
        self.step_deltas: list[dict[str, object]] = []

    async def publish(self, run_id: int, snapshot: object) -> None:
        del run_id
        self.snapshots.append(snapshot)

    async def publish_step_delta(
        self,
        run_id: int,
        *,
        stage_id: str,
        step_id: str,
        channel: str,
        delta: str,
    ) -> None:
        self.step_deltas.append(
            {
                "run_id": run_id,
                "stage_id": stage_id,
                "step_id": step_id,
                "channel": channel,
                "delta": delta,
            }
        )


class RunHarness:
    def __init__(self, service: RunService, executor: RunExecutor) -> None:
        self._service = service
        self._executor = executor

    async def create_run(self, command: StartRunCommand):
        return await self._service.create_run(command)

    async def execute_run(self, run_id: int):
        return await self._executor.execute(run_id)

    async def list_runs(self, query: ListRunsQuery):
        return await self._service.list_runs(query)

    async def abort_run(self, run_id: int, reason: str):
        return await self._service.abort_run(run_id, reason)


async def run_to_completion(
    service: RunHarness,
    command: StartRunCommand,
):
    created = await service.create_run(command)
    return await service.execute_run(created.run_id)


def make_agent_factory(
    llm_client,
    settings_repo,
    model_profile_repo,
    selected_model_repo,
) -> AgentRunnerFactoryAdapter:
    return AgentRunnerFactoryAdapter(
        llm_client,
        AgentRuntimeFactory(
            model_profile_repo=model_profile_repo,
            selected_model_repo=selected_model_repo,
        ),
    )


def make_service(
    *,
    llm_client,
    trading_client: FakePortfolioExecutionClient | None = None,
    run_job_repo: NullRunJobRepo | None = None,
) -> tuple[RunHarness, InMemoryRunRepository, SnapshotCollector]:
    run_repo = InMemoryRunRepository()
    settings_repo, model_profile_repo, selected_model_repo = (
        make_configured_model_repositories()
    )
    trading = trading_client or FakePortfolioExecutionClient()
    del trading
    collector = SnapshotCollector()
    registry = ActiveRunAbortRegistry()
    service = RunService(
        run_repo=run_repo,
        settings_repo=settings_repo,
        model_profile_repo=model_profile_repo,
        selected_model_repo=selected_model_repo,
        snapshot_publisher=collector.publish,
        now_provider=lambda: FIXED_NOW,
        run_job_repo=run_job_repo or NullRunJobRepo(),
        abort_registry=registry,
    )
    executor = RunExecutor(
        run_repo=run_repo,
        snapshot_publisher=collector.publish,
        trace_step_delta_publisher=collector.publish_step_delta,
        agent_runner_factory=make_agent_factory(
            llm_client, settings_repo, model_profile_repo, selected_model_repo
        ),
        now_provider=lambda: FIXED_NOW,
        market_session_is_open=lambda _moment: True,
        abort_registry=registry,
    )
    return RunHarness(service, executor), run_repo, collector


@pytest.mark.asyncio
async def test_create_run_bulk_loads_stage_models_once() -> None:
    settings_repo, profile_repo, selected_repo = make_configured_model_repositories()
    run_repo = InMemoryRunRepository()
    service = RunService(
        run_repo=run_repo,
        settings_repo=settings_repo,
        model_profile_repo=profile_repo,
        selected_model_repo=selected_repo,
        run_job_repo=NullRunJobRepo(),
        abort_registry=ActiveRunAbortRegistry(),
        now_provider=lambda: FIXED_NOW,
    )

    await service.create_run(StartRunCommand())

    assert selected_repo.get_by_ids_calls == 1
    assert profile_repo.get_by_ids_calls == 1
    assert selected_repo.get_by_id_calls == 0
    assert profile_repo.get_by_id_calls == 0


@pytest.mark.asyncio
async def test_run_service_builds_trace_for_hold_flow() -> None:
    service, run_repo, collector = make_service(llm_client=FakeLLMClient())

    result = await run_to_completion(
        service,
        StartRunCommand(
            trigger_source=TriggerSource.MANUAL,
            prompt_version="prompt-v2",
            risk_rules_version="risk-v2",
        ),
    )
    stored = await run_repo.get_by_id(expected_run_id())

    assert result.run_id == expected_run_id()
    assert result.status == "COMPLETED"
    assert stored is not None
    assert set(stored.snapshot.stage_models) == {"Run", "Summary"}
    assert all(
        model.model_name == "gpt-4.1-mini"
        and model.base_url == "https://llm.example.com/v1"
        for model in stored.snapshot.stage_models.values()
    )
    assert [stage["key"] for stage in result.trace["stages"]] == [
        "run",
        "summary",
    ]
    assert result.trace["stages"][0]["status"] == "completed"
    assert result.trace["stages"][1]["status"] == "completed"
    public_steps = result.trace["stages"][0]["steps"]
    assert all(step["type"] != "prompt" for step in public_steps)
    assert stored.trace.stages[0].steps[0].type == "prompt"
    assert collector.snapshots
    latest_snapshot = collector.snapshots[-1]
    assert isinstance(latest_snapshot, RunDetailDTO)
    assert stored.summary_render_mode == "html"
    assert latest_snapshot.summary_render_mode in {"markdown", "html"}
    assert all(
        "data" not in step
        for stage in latest_snapshot.trace["stages"]
        for step in stage["steps"]
    )


@pytest.mark.asyncio
async def test_failed_run_keeps_the_reason_on_its_actual_stage() -> None:
    service, run_repo, collector = make_service(llm_client=FailingResearchLLMClient())

    created = await service.create_run(StartRunCommand())

    with pytest.raises(RuntimeError, match="研究模型连接超时"):
        await service.execute_run(created.run_id)

    stored = await run_repo.get_by_id(created.run_id)
    assert stored is not None
    assert stored.status is RunStatus.FAILED
    assert stored.failure_reason == "研究模型连接超时"
    assert [stage.key for stage in stored.trace.stages] == ["run"]
    assert stored.trace.stages[0].status == "failed"
    detail = to_run_detail_dto(stored)
    assert detail.failure_reason == "研究模型连接超时"
    assert collector.snapshots


@pytest.mark.asyncio
async def test_run_service_segments_run_thinking_and_tool_steps() -> None:
    service, _run_repo, _collector = make_service(
        llm_client=SegmentedResearchLLMClient()
    )

    result = await run_to_completion(
        service,
        StartRunCommand(
            trigger_source=TriggerSource.MANUAL,
        ),
    )
    run_stage = next(
        stage for stage in result.trace["stages"] if stage["key"] == "run"
    )
    steps = run_stage["steps"]

    assert [step["step_id"] for step in steps] == [
        "thinking:1",
        "tool:call-1",
        "tool:call-2",
        "thinking:2",
        "result",
    ]
    assert [step["title"] for step in steps] == [
        "深度思考",
        "金融数据查询",
        "资讯搜索",
        "深度思考",
        "生成 Markdown 运行报告",
    ]
    assert steps[0]["status"] == "completed"
    assert steps[1]["status"] == "blocked"
    assert steps[2]["status"] == "blocked"
    assert steps[3]["status"] == "completed"
    assert steps[1]["tool_call"] == {
        "call_id": "call-1",
        "intent_line": "金融数据查询 · 上证指数、深证成指、创业板指最新点位和涨跌幅",
        "source": "mx",
        "tool_name": "query_market_data",
        "display_name": "金融数据查询",
        "query_parameters": "query=上证指数、深证成指、创业板指最新点位和涨跌幅",
    }
    assert steps[2]["tool_call"] == {
        "call_id": "call-2",
        "intent_line": "资讯搜索 · A股市场热点板块与政策消息",
        "source": "mx",
        "tool_name": "search_news",
        "display_name": "资讯搜索",
        "query_parameters": "query=A股市场热点板块与政策消息",
    }
    assert "先梳理市场环境" in str(steps[0]["content"])
    assert "结合已获取的市场数据和资讯" in str(steps[3]["content"])
    assert steps[1]["summary"] is None
    assert steps[2]["summary"] is None


@pytest.mark.asyncio
async def test_run_service_keeps_streamed_run_result_after_tools() -> None:
    service, _run_repo, collector = make_service(
        llm_client=StreamingResearchResultBeforeToolLLMClient(),
    )

    result = await run_to_completion(
        service,
        StartRunCommand(
            trigger_source=TriggerSource.MANUAL,
        ),
    )
    run_stage = next(
        stage for stage in result.trace["stages"] if stage["key"] == "run"
    )
    steps = run_stage["steps"]

    assert [step["step_id"] for step in steps] == [
        "tool:call-early-tool",
        "result",
    ]
    assert steps[-1]["title"] == "生成 Markdown 运行报告"
    assert "工具后整理" in str(steps[-1]["content"])
    assert any(
        item["step_id"] == "result"
        and item["channel"] == "text"
        and "工具后整理" in str(item["delta"])
        for item in collector.step_deltas
    )


@pytest.mark.asyncio
async def test_run_service_streams_review_result_delta() -> None:
    service, _run_repo, collector = make_service(llm_client=StreamingSummaryLLMClient())

    result = await run_to_completion(
        service,
        StartRunCommand(
            trigger_source=TriggerSource.MANUAL,
        ),
    )
    review_stage = next(
        stage for stage in result.trace["stages"] if stage["key"] == "summary"
    )

    assert any(
        step["step_id"] == "result"
        and step["title"] == "生成 HTML 总结"
        and "本轮运行完成" in str(step["content"])
        for step in review_stage["steps"]
    )
    assert any(
        item["stage_id"] == review_stage["stage_id"]
        and item["step_id"] == "result"
        and item["channel"] == "text"
        and "本轮运行完成" in str(item["delta"])
        for item in collector.step_deltas
    )


@pytest.mark.asyncio
async def test_run_service_lists_runs_with_trace_detail() -> None:
    service, _run_repo, _collector = make_service(llm_client=FakeLLMClient())

    await run_to_completion(service, StartRunCommand())
    runs = await service.list_runs(ListRunsQuery(limit=10, offset=0))

    assert len(runs) == 1
    assert runs[0].run_id == expected_run_id()


@pytest.mark.asyncio
async def test_abort_registry_crosses_request_and_executor_scopes() -> None:
    registry = ActiveRunAbortRegistry()
    signal = registry.activate(expected_run_id())

    observed = registry.abort(expected_run_id(), "user_requested")

    assert observed is True
    assert signal.aborted is True
    assert signal.reason == "user_requested"


@pytest.mark.asyncio
async def test_concurrent_start_raises_concurrent_run_error() -> None:
    run_repo = InMemoryRunRepository()
    seeded = StrategyRun(
        run_id=expected_run_id(),
        trigger_source=TriggerSource.MANUAL,
        schedule_id=None,
        snapshot=StrategySnapshot(
            prompt_version="v1",
            risk_rules_version="risk-v1",
        ),
    )
    run_repo.runs[seeded.run_id] = seeded
    settings_repo, model_profile_repo, selected_model_repo = (
        make_configured_model_repositories()
    )
    service = RunService(
        run_repo=run_repo,
        settings_repo=settings_repo,
        model_profile_repo=model_profile_repo,
        selected_model_repo=selected_model_repo,
        now_provider=lambda: FIXED_NOW,
        abort_registry=ActiveRunAbortRegistry(),
        run_job_repo=NullRunJobRepo(),
    )

    with pytest.raises(ConcurrentRunError) as exc:
        await service.create_run(StartRunCommand())
    assert exc.value.running_run_id == seeded.run_id


@pytest.mark.asyncio
async def test_abort_run_raises_not_found_when_no_run_matches() -> None:
    service, _run_repo, _collector = make_service(llm_client=FakeLLMClient())

    with pytest.raises(RunNotFoundError):
        await service.abort_run(99999, reason="user_requested")


@pytest.mark.asyncio
async def test_abort_run_with_terminal_job_aborts_orphaned_run() -> None:
    run_id = expected_run_id()
    service, run_repo, _collector = make_service(
        llm_client=FakeLLMClient(),
        run_job_repo=NullRunJobRepo(RunJob(run_id=run_id, status=RunJobStatus.FAILED)),
    )

    created = await service.create_run(StartRunCommand())
    await service.abort_run(created.run_id, reason="manual_stop")

    stored = await run_repo.get_by_id(run_id)
    assert stored is not None
    assert stored.status is RunStatus.ABORTED
