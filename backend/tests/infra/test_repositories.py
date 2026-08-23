"""Tests for persistence repositories."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from sqlalchemy import event, func, select

import backend.business.runs.traces.recorder as recorder_module
from backend.business.runs import (
    StrategyRun,
    StrategySnapshot,
    build_run_id,
)
from backend.business.runs.run_trace import RunTrace, TraceStage, TraceStep
from backend.business.runs.traces.recorder import RunTraceRecorder
from backend.business.settings import (
    AniuAgentPrompt,
    AppSettings,
    ModelProfile,
    SelectedModel,
)
from backend.business.shared.enums import RunState, TriggerSource
from backend.infra.db.models import AppSettingsModel, SecretStoreModel
from backend.infra.repositories import (
    ModelProfileRepository,
    RunRepository,
    SelectedModelRepository,
    SettingsRepository,
)
from backend.llm import ModelProtocol


def make_snapshot() -> StrategySnapshot:
    return StrategySnapshot(
        prompt_version="v1",
        risk_rules_version="risk-v1",
    )


def make_run(run_id: int = 1) -> StrategyRun:
    return StrategyRun(
        run_id=run_id,
        trigger_source=TriggerSource.MANUAL,
        schedule_id=None,
        snapshot=make_snapshot(),
    )


@pytest.mark.asyncio
async def test_run_repository_persists_run_snapshot(session, session_factory) -> None:
    repository = RunRepository(session)
    run = make_run()
    run.trace = RunTrace(
        current_stage_id="run:na",
        stages=[
            TraceStage(
                stage_id="run:na",
                key="run",
                round=None,
                title="任务执行",
                description="完成研究、判断、交易与报告",
                status="completed",
                steps=[
                    TraceStep(
                        step_id="result",
                        type="result",
                        title="生成分析报告",
                        status="completed",
                        summary="分析报告已生成",
                        content="# 分析报告\n继续观察。",
                    )
                ],
            )
        ],
    )
    run.current_state = RunState.RUN

    await repository.add(run)
    run.set_summary("分析阶段已开始。")
    await repository.save(run)
    await session.commit()

    async with session_factory() as verify_session:
        verify_repository = RunRepository(verify_session)
        loaded_run = await verify_repository.get_by_id(run.run_id)
        listed_runs = await verify_repository.list_runs()

    assert loaded_run is not None
    assert loaded_run.summary == "分析阶段已开始。"
    assert loaded_run.current_state is RunState.RUN
    assert loaded_run.trace.current_stage_id == "run:na"
    assert loaded_run.trace.stages[0].steps[0].content == "# 分析报告\n继续观察。"
    assert len(listed_runs) == 1
    assert listed_runs[0].run_id == run.run_id


@pytest.mark.asyncio
async def test_set_stage_summary_commits_for_other_sqlite_session(
    session_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second database session must observe the summary immediately."""

    monkeypatch.setattr(recorder_module, "perf_counter", lambda: 0.0)

    async with session_factory() as writer_session:
        run = make_run()
        repository = RunRepository(writer_session)
        await repository.add(run)
        await writer_session.commit()

        async def persist_run(stored_run: StrategyRun) -> StrategyRun:
            return await repository.save(stored_run)

        async def commit() -> None:
            await writer_session.commit()

        recorder = RunTraceRecorder(
            run=run,
            persist_run=persist_run,
            publish_snapshot=lambda *_a, **_k: None,
            commit=commit,
            has_snapshot_subscribers=lambda _run_id: False,
        )
        await recorder.enter_stage("Run")
        await recorder.set_stage_summary("Run", "调用工具 1 次")

        async with session_factory() as reader_session:
            persisted = await RunRepository(reader_session).get_by_id(run.run_id)

    assert persisted is not None
    run_stage = next(stage for stage in persisted.trace.stages if stage.key == "run")
    assert run_stage.summary == "调用工具 1 次"


@pytest.mark.asyncio
async def test_run_repository_round_trips_summary_render_mode(
    session, session_factory
) -> None:
    repository = RunRepository(session)
    run = make_run()
    run.set_summary("<section><p>报告</p></section>", render_mode="html")

    await repository.add(run)
    await session.commit()

    async with session_factory() as verify_session:
        loaded_run = await RunRepository(verify_session).get_by_id(run.run_id)

    assert loaded_run is not None
    assert loaded_run.summary == "<section><p>报告</p></section>"
    assert loaded_run.summary_render_mode == "html"


@pytest.mark.asyncio
async def test_run_repository_next_run_id_uses_existing_runs(session) -> None:
    repository = RunRepository(session)
    run = make_run(
        run_id=build_run_id(datetime(2026, 5, 8, tzinfo=UTC).date(), sequence=5)
    )

    await repository.add(run)
    await session.commit()

    next_run_id = await repository.next_run_id(datetime(2026, 5, 8, tzinfo=UTC).date())

    assert next_run_id == build_run_id(
        datetime(2026, 5, 8, tzinfo=UTC).date(), sequence=6
    )


@pytest.mark.asyncio
async def test_run_repository_allocates_unique_ids_concurrently(
    session_factory,
) -> None:
    reference_date = datetime(2026, 5, 9, tzinfo=UTC).date()

    async def allocate() -> int:
        async with session_factory() as concurrent_session:
            run_id = await RunRepository(concurrent_session).next_run_id(reference_date)
            await concurrent_session.commit()
            return run_id

    allocated = await asyncio.gather(*(allocate() for _ in range(5)))

    assert len(set(allocated)) == 5
    assert sorted(allocated) == [
        build_run_id(reference_date, sequence=sequence) for sequence in range(1, 6)
    ]


@pytest.mark.asyncio
async def test_settings_repository_upserts_single_row(session, session_factory) -> None:
    repository = SettingsRepository(session)

    saved = await repository.save(AppSettings(mx_api_key="mx-plain"))
    await session.commit()

    await repository.save(
        AppSettings(
            mx_api_key="mx-updated",
            prompt_profile=AniuAgentPrompt(
                name="稳健配置",
                global_prompt="严格控制风险。",
            ),
            created_at=saved.created_at,
            updated_at=datetime.now(tz=UTC),
        )
    )
    await session.commit()

    async with session_factory() as verify_session:
        verify_repository = SettingsRepository(verify_session)
        loaded_settings = await verify_repository.get()
        row_count = await verify_session.scalar(
            select(func.count()).select_from(AppSettingsModel)
        )
        secret_rows = list(
            (
                await verify_session.scalars(
                    select(SecretStoreModel).where(
                        SecretStoreModel.namespace == "app_settings"
                    )
                )
            ).all()
        )

    assert loaded_settings is not None
    assert loaded_settings.mx_api_key == "mx-updated"
    assert len(secret_rows) == 1
    assert secret_rows[0].encrypted_value != "mx-updated"
    assert loaded_settings.revision >= 2
    assert loaded_settings.prompt_profile.name == "稳健配置"
    assert loaded_settings.prompt_profile.global_prompt == "严格控制风险。"
    assert row_count == 1


@pytest.mark.asyncio
async def test_model_profile_repository_crud(session, session_factory) -> None:
    repository = ModelProfileRepository(session)

    saved = await repository.create(
        ModelProfile(
            name="OpenAI Main",
            protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
            model_name="gpt-4.1",
            base_url="https://api.openai.com/v1",
            api_key="test-key",
            sort_order=1,
        )
    )
    await session.commit()

    await repository.update(
        ModelProfile(
            profile_id=saved.profile_id,
            revision=saved.revision,
            name="OpenAI Main Updated",
            protocol=ModelProtocol.CLAUDE_API,
            model_name="gpt-4.1-mini",
            base_url="https://api.openai.com/v1",
            api_key="test-key-2",
            sort_order=2,
            created_at=saved.created_at,
            updated_at=datetime.now(tz=UTC),
        )
    )
    await session.commit()

    async with session_factory() as verify_session:
        verify_repository = ModelProfileRepository(verify_session)
        listed = await verify_repository.list_profiles()
        loaded = await verify_repository.get_by_id(saved.profile_id)
        secret = await verify_session.scalar(
            select(SecretStoreModel).where(
                SecretStoreModel.namespace == "model_profile",
                SecretStoreModel.owner_id == str(saved.profile_id),
            )
        )
        await verify_repository.delete(saved.profile_id)
        await verify_session.commit()
        listed_after = await verify_repository.list_profiles()

    assert loaded is not None
    assert loaded.name == "OpenAI Main Updated"
    assert loaded.protocol is ModelProtocol.CLAUDE_API
    assert loaded.api_key == "test-key-2"
    assert loaded.revision >= 2
    assert secret is not None
    assert secret.encrypted_value != "test-key-2"
    assert listed[0].profile_id == saved.profile_id
    assert listed_after == []


@pytest.mark.asyncio
async def test_repository_rejects_stale_configuration_revision(session) -> None:
    from backend.business.shared import ConfigurationConflictError

    repository = ModelProfileRepository(session)
    saved = await repository.create(
        ModelProfile(
            name="CAS",
            model_name="m1",
            base_url="https://example.com/v1",
            api_key="secret",
        )
    )
    await session.commit()

    current = await repository.get_by_id(saved.profile_id)
    assert current is not None
    updated = await repository.update(
        ModelProfile(
            profile_id=current.profile_id,
            revision=current.revision,
            name="CAS updated",
            model_name=current.model_name,
            base_url=current.base_url,
            api_key=current.api_key,
            created_at=current.created_at,
        )
    )
    await session.commit()
    assert updated.revision > current.revision

    with pytest.raises(ConfigurationConflictError, match="stale model_profile"):
        await repository.update(
            ModelProfile(
                profile_id=current.profile_id,
                revision=current.revision,
                name="stale",
                model_name=current.model_name,
                base_url=current.base_url,
                api_key=current.api_key,
                created_at=current.created_at,
            )
        )


@pytest.mark.asyncio
async def test_selected_model_repository_replace_and_query(
    session,
    session_factory,
) -> None:
    channel_repo = ModelProfileRepository(session)
    selected_repo = SelectedModelRepository(session)

    channel = await channel_repo.create(
        ModelProfile(
            name="OpenAI Main",
            protocol=ModelProtocol.OPENAI_CHAT_COMPLETIONS,
            model_name="gpt-4.1",
            base_url="https://api.openai.com/v1",
            api_key="test-key",
            sort_order=1,
        )
    )
    replaced = await selected_repo.replace_for_channel(
        channel.profile_id,
        [
            SelectedModel(
                channel_profile_id=channel.profile_id,
                model_name="gpt-4.1-mini",
                label="gpt-4.1-mini",
                provider_id="gpt-4.1-mini",
                thinking_efforts=("high", "low", "high"),
                sort_order=0,
            ),
            SelectedModel(
                channel_profile_id=channel.profile_id,
                model_name="gpt-4.1",
                label="gpt-4.1",
                provider_id="gpt-4.1",
                sort_order=1,
            ),
        ],
    )
    original_ids = {item.model_name: item.selected_model_id for item in replaced}
    replaced_again = await selected_repo.replace_for_channel(
        channel.profile_id,
        [
            SelectedModel(
                channel_profile_id=channel.profile_id,
                model_name="gpt-4.1",
                label="GPT 4.1 updated",
                provider_id="gpt-4.1",
                sort_order=0,
            ),
            SelectedModel(
                channel_profile_id=channel.profile_id,
                model_name="gpt-4.1-mini",
                label="GPT 4.1 Mini updated",
                provider_id="gpt-4.1-mini",
                thinking_efforts=("low", "high"),
                sort_order=1,
            ),
        ],
    )
    await session.commit()

    async with session_factory() as verify_session:
        verify_repo = SelectedModelRepository(verify_session)
        listed = await verify_repo.list_by_channel(channel.profile_id)
        loaded = await verify_repo.get_by_id(replaced[0].selected_model_id)
        listed_all = await verify_repo.list_all()

    assert [item.model_name for item in listed] == ["gpt-4.1", "gpt-4.1-mini"]
    assert {item.model_name: item.selected_model_id for item in replaced_again} == (
        original_ids
    )
    assert loaded is not None
    assert loaded.label == "GPT 4.1 Mini updated"
    assert loaded.thinking_efforts == ("low", "high")
    assert listed_all[0].channel_profile_id == channel.profile_id


@pytest.mark.asyncio
async def test_run_list_summaries_use_projection_columns(
    session, session_factory
) -> None:
    repository = RunRepository(session)
    run = StrategyRun(
        run_id=await repository.next_run_id(),
        trigger_source=TriggerSource.MANUAL,
        schedule_id=None,
        snapshot=make_snapshot(),
        trace=RunTrace(
            stages=[
                TraceStage(
                    stage_id="run:na",
                    key="run",
                    round=None,
                    title="任务执行",
                    description="完成研究、判断、交易与报告",
                    status="completed",
                    steps=[
                        TraceStep(
                            step_id="t1",
                            type="tool",
                            title="tool",
                            status="completed",
                            data={"arguments": {"q": "x"}, "result": {"ok": True}},
                        ),
                        TraceStep(
                            step_id="th1",
                            type="thinking",
                            title="think",
                            status="completed",
                            content="hello world",
                        ),
                    ],
                ),
                TraceStage(
                    stage_id="summary:na",
                    key="summary",
                    round=None,
                    title="展示总结",
                    description="生成 HTML 总结",
                    status="degraded",
                    steps=[],
                ),
            ]
        ),
    )
    await repository.add(run)
    await session.commit()

    statements: list[str] = []
    sync_engine = session.get_bind().engine

    def capture_sql(_conn, _cursor, statement, _parameters, _context, _executemany):
        statements.append(str(statement))

    event.listen(sync_engine, "before_cursor_execute", capture_sql)
    try:
        async with session_factory() as verify_session:
            verify_repo = RunRepository(verify_session)
            rows = await verify_repo.list_run_summaries(limit=10)
    finally:
        event.remove(sync_engine, "before_cursor_execute", capture_sql)

    assert rows
    list_selects = [sql for sql in statements if "FROM strategy_runs" in sql]
    assert list_selects
    assert all("trace_json" not in sql for sql in list_selects)
    assert all("snapshot_json" not in sql for sql in list_selects)
    summary = next(item for item in rows if item["run_id"] == run.run_id)
    assert summary["tool_calls_count"] == 1
    assert summary["thinking_count"] == 1
    assert summary["total_tokens"] > 0
    assert summary["trade_count"] == 0
    assert summary["summary_render_mode"] == "markdown"
