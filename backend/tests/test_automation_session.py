from pathlib import Path
import sys
from types import SimpleNamespace

from fastapi.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.core.config import get_settings
from app.core.rate_limit import _limiter
from app.db import database as database_module
from app.db.database import session_scope
from app.db.models import ChatMessageRecord, ChatSession, StrategyRun, StrategySchedule
from app.main import create_app
from app.services.aniu_service import aniu_service
from app.services.event_bus import event_bus
from app.services.llm_service import llm_service
from app.services.scheduler_service import scheduler_service
from app.services.token_estimator import estimate_messages_tokens, estimate_text_tokens
from app.services.trading_calendar_service import trading_calendar_service


def create_test_client(monkeypatch, tmp_path) -> TestClient:
    monkeypatch.setenv("APP_LOGIN_PASSWORD", "release-pass")
    monkeypatch.setenv("SQLITE_DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setattr(trading_calendar_service, "ensure_years", lambda years: None)
    monkeypatch.setattr(scheduler_service, "start", lambda: None)
    monkeypatch.setattr(scheduler_service, "stop", lambda: None)
    get_settings.cache_clear()
    database_module._engine = None
    database_module._session_local = None
    _limiter._buckets.clear()
    aniu_service._account_overview_cache = None
    aniu_service._account_overview_cache_expires_at = None
    app = create_app()
    return TestClient(app)


def reset_db_state() -> None:
    database_module._engine = None
    database_module._session_local = None
    _limiter._buckets.clear()
    get_settings.cache_clear()


def _prepare_schedule(db, *, name: str, run_type: str = "analysis") -> StrategySchedule:
    schedule = StrategySchedule(
        name=name,
        run_type=run_type,
        cron_expression="*/30 * * * *",
        task_prompt=f"{name} task prompt",
        timeout_seconds=180,
        enabled=True,
    )
    db.add(schedule)
    db.flush()
    return schedule


def _fake_run_result(final_answer: str, tool_calls=None):
    return (
        {
            "final_answer": final_answer,
            "tool_calls": tool_calls or [],
        },
        {"messages": []},
        {"responses": [], "final_message": {"content": final_answer}},
        {"messages": []},
    )


def test_schedule_runs_share_single_automation_session(monkeypatch, tmp_path) -> None:
    captured_messages: list[list[dict[str, object]]] = []

    def fake_run_agent_with_messages(*, messages, **kwargs):
        del kwargs
        captured_messages.append(messages)
        return _fake_run_result("scheduled decision")

    monkeypatch.setattr(llm_service, "run_agent_with_messages", fake_run_agent_with_messages)

    with create_test_client(monkeypatch, tmp_path):
        with session_scope() as db:
            settings = aniu_service.get_or_create_settings(db)
            settings.mx_api_key = "mx-key"
            settings.llm_base_url = "https://example.com/v1"
            settings.llm_api_key = "llm-key"
            settings.llm_model = "demo-model"
            first = _prepare_schedule(db, name="盘前分析", run_type="analysis")
            second = _prepare_schedule(db, name="上午运行 1", run_type="trade")
            first_id = first.id
            second_id = second.id

        first_run = aniu_service.execute_run(trigger_source="schedule", schedule_id=first_id)
        second_run = aniu_service.execute_run(trigger_source="schedule", schedule_id=second_id)

        assert first_run.chat_session_id is not None
        assert second_run.chat_session_id == first_run.chat_session_id
        assert first_run.prompt_message_id is not None
        assert first_run.response_message_id is not None
        assert second_run.prompt_message_id is not None
        assert second_run.response_message_id is not None

        assert len(captured_messages) == 2
        assert any(msg["role"] == "user" for msg in captured_messages[0])
        second_history = captured_messages[1]
        assert any("时间：" in str(msg.get("content") or "") for msg in second_history)
        assert any("scheduled decision" in str(msg.get("content") or "") for msg in second_history)

        with session_scope() as db:
            sessions = db.query(ChatSession).filter(ChatSession.kind == "automation").all()
            assert len(sessions) == 1
            session = sessions[0]
            messages = (
                db.query(ChatMessageRecord)
                .filter(ChatMessageRecord.session_id == session.id)
                .order_by(ChatMessageRecord.id.asc())
                .all()
            )
            assert len(messages) == 4
            assert [item.role for item in messages] == ["user", "assistant", "user", "assistant"]
            assert session.slug == "automation-default"

    reset_db_state()


def test_manual_runs_share_automation_session_with_scheduled_runs(monkeypatch, tmp_path) -> None:
    captured_messages: list[list[dict[str, object]]] = []

    def fake_run_agent_with_messages(*, messages, **kwargs):
        del kwargs
        captured_messages.append(messages)
        if len(captured_messages) == 1:
            return _fake_run_result("scheduled decision")
        return _fake_run_result("manual decision")

    monkeypatch.setattr(llm_service, "run_agent_with_messages", fake_run_agent_with_messages)

    with create_test_client(monkeypatch, tmp_path):
        with session_scope() as db:
            settings = aniu_service.get_or_create_settings(db)
            settings.mx_api_key = "mx-key"
            settings.llm_base_url = "https://example.com/v1"
            settings.llm_api_key = "llm-key"
            settings.llm_model = "demo-model"
            schedule = _prepare_schedule(db, name="收盘分析", run_type="analysis")
            schedule_id = schedule.id

        scheduled_run = aniu_service.execute_run(
            trigger_source="schedule",
            schedule_id=schedule_id,
        )
        manual_run = aniu_service.execute_run(trigger_source="manual", schedule_id=None)

        assert scheduled_run.chat_session_id is not None
        assert manual_run.chat_session_id == scheduled_run.chat_session_id
        assert manual_run.prompt_message_id is not None
        assert manual_run.response_message_id is not None
        assert len(captured_messages) == 2
        second_history = captured_messages[1]
        assert any("来源: 手动触发" in str(msg.get("content") or "") for msg in second_history)
        assert any("scheduled decision" in str(msg.get("content") or "") for msg in second_history)

        with session_scope() as db:
            sessions = db.query(ChatSession).filter(ChatSession.kind == "automation").all()
            assert len(sessions) == 1
            session = sessions[0]
            messages = (
                db.query(ChatMessageRecord)
                .filter(ChatMessageRecord.session_id == session.id)
                .order_by(ChatMessageRecord.id.asc())
                .all()
            )
            assert len(messages) == 4
            assert [item.role for item in messages] == ["user", "assistant", "user", "assistant"]
            assert any(record.run_id == manual_run.id for record in messages)

    reset_db_state()


def test_scheduled_runs_can_read_prior_manual_history(monkeypatch, tmp_path) -> None:
    captured_messages: list[list[dict[str, object]]] = []

    def fake_run_agent_with_messages(*, messages, **kwargs):
        del kwargs
        captured_messages.append(messages)
        if len(captured_messages) == 1:
            return _fake_run_result("manual decision")
        return _fake_run_result("scheduled decision")

    monkeypatch.setattr(llm_service, "run_agent_with_messages", fake_run_agent_with_messages)

    with create_test_client(monkeypatch, tmp_path):
        with session_scope() as db:
            settings = aniu_service.get_or_create_settings(db)
            settings.mx_api_key = "mx-key"
            settings.llm_base_url = "https://example.com/v1"
            settings.llm_api_key = "llm-key"
            settings.llm_model = "demo-model"
            schedule = _prepare_schedule(db, name="收盘分析", run_type="analysis")
            schedule_id = schedule.id

        manual_run = aniu_service.execute_run(trigger_source="manual", schedule_id=None)
        scheduled_run = aniu_service.execute_run(
            trigger_source="schedule",
            schedule_id=schedule_id,
        )

        assert manual_run.chat_session_id is not None
        assert scheduled_run.chat_session_id == manual_run.chat_session_id
        assert len(captured_messages) == 2
        second_history = captured_messages[1]
        assert any("来源: 手动触发" in str(msg.get("content") or "") for msg in second_history)
        assert any("manual decision" in str(msg.get("content") or "") for msg in second_history)

    reset_db_state()


def test_schedule_run_failure_persists_failed_assistant_message(monkeypatch, tmp_path) -> None:
    def fake_run_agent_with_messages(**kwargs):
        del kwargs
        raise RuntimeError("llm unavailable")

    monkeypatch.setattr(llm_service, "run_agent_with_messages", fake_run_agent_with_messages)

    with create_test_client(monkeypatch, tmp_path):
        with session_scope() as db:
            settings = aniu_service.get_or_create_settings(db)
            settings.mx_api_key = "mx-key"
            settings.llm_base_url = "https://example.com/v1"
            settings.llm_api_key = "llm-key"
            settings.llm_model = "demo-model"
            schedule = _prepare_schedule(db, name="午间复盘", run_type="analysis")
            schedule_id = schedule.id

        try:
            aniu_service.execute_run(trigger_source="schedule", schedule_id=schedule_id)
        except RuntimeError as exc:
            assert str(exc) == "llm unavailable"
        else:
            raise AssertionError("expected scheduled run to fail")

        with session_scope() as db:
            run = db.query(StrategyRun).order_by(StrategyRun.id.desc()).first()
            assert run is not None
            assert run.status == "failed"
            assert run.chat_session_id is not None
            session = db.get(ChatSession, run.chat_session_id)
            assert session is not None
            messages = (
                db.query(ChatMessageRecord)
                .filter(ChatMessageRecord.session_id == session.id)
                .order_by(ChatMessageRecord.id.asc())
                .all()
            )
            assert len(messages) == 2
            assert messages[0].role == "user"
            assert messages[1].role == "assistant"
            assert "执行失败：llm unavailable" in messages[1].content
            assert run.response_message_id == messages[1].id

    reset_db_state()


def test_chat_session_list_excludes_automation_sessions(monkeypatch, tmp_path) -> None:
    with create_test_client(monkeypatch, tmp_path) as client:
        response = client.post("/api/aniu/login", json={"password": "release-pass"})
        headers = {"Authorization": f"Bearer {response.json()['token']}"}

        with session_scope() as db:
            db.add(ChatSession(title="Automation", kind="automation", slug="automation-default"))
            db.add(ChatSession(title="User", kind="user"))

        result = client.get("/api/aniu/chat/sessions", headers=headers)
        assert result.status_code == 200
        payload = result.json()
        assert len(payload) == 1
        assert payload[0]["title"] == "User"
        assert payload[0]["kind"] == "user"

    reset_db_state()


def test_safe_prompt_budget_uses_85_percent_of_max_context(monkeypatch, tmp_path) -> None:
    with create_test_client(monkeypatch, tmp_path):
        budget = aniu_service._safe_prompt_budget(
            SimpleNamespace(automation_context_window_tokens=128000)
        )

    assert budget == 108800

    reset_db_state()


def test_context_compaction_messages_are_not_reinjected_into_model_history(
    monkeypatch, tmp_path
) -> None:
    with create_test_client(monkeypatch, tmp_path):
        with session_scope() as db:
            session = ChatSession(
                title="自动化交易会话",
                kind="automation",
                slug="automation-default",
            )
            db.add(session)
            db.flush()
            db.add_all(
                [
                    ChatMessageRecord(
                        session_id=session.id,
                        role="user",
                        content="用户历史",
                        message_kind="live_turn",
                    ),
                    ChatMessageRecord(
                        session_id=session.id,
                        role="system",
                        content="[上下文压缩摘要]\n已压缩",
                        message_kind="context_compaction",
                    ),
                    ChatMessageRecord(
                        session_id=session.id,
                        role="assistant",
                        content="助手历史",
                        message_kind="live_turn",
                    ),
                ]
            )

        with session_scope() as db:
            records = aniu_service._list_persistent_session_history_records(
                db=db,
                session_id=session.id,
                recent_limit=24,
            )
            messages = aniu_service._build_persistent_session_history_messages(records)

    assert messages == [
        {"role": "user", "content": "用户历史"},
        {"role": "assistant", "content": "助手历史"},
    ]

    reset_db_state()


def test_token_estimate_does_not_double_count_archived_summary(monkeypatch, tmp_path) -> None:
    with create_test_client(monkeypatch, tmp_path):
        session = ChatSession(
            title="自动化交易会话",
            kind="automation",
            slug="automation-default",
            archived_summary="## 当前策略\n- 继续观察",
        )
        settings = SimpleNamespace(system_prompt="system")
        messages = aniu_service._build_persistent_session_prompt_messages(
            session=session,
            history_messages=[{"role": "user", "content": "最新用户消息"}],
            memory_messages=[],
        )

        estimate = aniu_service._estimate_persistent_session_context_tokens(
            session=session,
            settings=settings,
            messages=messages,
        )

    assert estimate == (
        estimate_messages_tokens(messages)
        + estimate_text_tokens(settings.system_prompt)
    )

    reset_db_state()


def test_compaction_uses_only_uncompacted_history(monkeypatch, tmp_path) -> None:
    with create_test_client(monkeypatch, tmp_path):
        with session_scope() as db:
            session = ChatSession(
                title="自动化交易会话",
                kind="automation",
                slug="automation-default",
                archived_summary="已压缩历史",
                last_compacted_message_id=6,
                summary_revision=2,
            )
            db.add(session)
            db.flush()
            db.add_all(
                [
                    ChatMessageRecord(
                        session_id=session.id,
                        role="user",
                        content=f"旧消息-{index}",
                        message_kind="live_turn",
                    )
                    for index in range(1, 7)
                ]
                + [
                    ChatMessageRecord(
                        session_id=session.id,
                        role="user",
                        content="最新用户消息",
                        message_kind="live_turn",
                    ),
                    ChatMessageRecord(
                        session_id=session.id,
                        role="assistant",
                        content="最新助手消息",
                        message_kind="live_turn",
                    ),
                ]
            )
            db.flush()
            settings = SimpleNamespace(
                automation_enable_auto_compaction=True,
                automation_recent_message_limit=4,
                automation_idle_summary_hours=12,
                automation_context_window_tokens=128000,
            )

            archived_summary, summary_revision = aniu_service._maybe_compact_persistent_session(
                db=db,
                session=session,
                settings=settings,
                estimated_tokens=0,
            )

    assert archived_summary == "已压缩历史"
    assert summary_revision == 2

    reset_db_state()


def test_schedule_run_emits_context_compacted_event(monkeypatch, tmp_path) -> None:
    captured_events: list[dict[str, object]] = []

    def fake_run_agent_with_messages(*, messages, **kwargs):
        del messages, kwargs
        return _fake_run_result("scheduled decision")

    monkeypatch.setattr(llm_service, "run_agent_with_messages", fake_run_agent_with_messages)

    with create_test_client(monkeypatch, tmp_path):
        with session_scope() as db:
            settings = aniu_service.get_or_create_settings(db)
            settings.mx_api_key = "mx-key"
            settings.llm_base_url = "https://example.com/v1"
            settings.llm_api_key = "llm-key"
            settings.llm_model = "demo-model"
            settings.automation_recent_message_limit = 4
            schedule = _prepare_schedule(db, name="盘前分析", run_type="analysis")
            schedule_id = schedule.id

        for _ in range(4):
            aniu_service.execute_run(trigger_source="schedule", schedule_id=schedule_id)

        run = aniu_service.start_run_async(trigger_source="schedule", schedule_id=schedule_id)
        for event in event_bus.stream(run):
            captured_events.append(event)

        with session_scope() as db:
            session = db.query(ChatSession).filter(ChatSession.kind == "automation").one()
            messages = (
                db.query(ChatMessageRecord)
                .filter(ChatMessageRecord.session_id == session.id)
                .order_by(ChatMessageRecord.id.asc())
                .all()
            )

    compacted = [event for event in captured_events if event.get("type") == "context_compacted"]
    assert len(compacted) == 1
    assert str(compacted[0].get("content") or "").startswith("[上下文压缩摘要]\n")
    assert any(item.message_kind == "context_compaction" for item in messages)

    reset_db_state()
