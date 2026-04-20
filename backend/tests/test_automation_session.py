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
from app.services.llm_service import llm_service
from app.services.scheduler_service import scheduler_service
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
        assert any("[自动任务触发]" in str(msg.get("content") or "") for msg in second_history)
        assert any("[本轮执行摘要]" in str(msg.get("content") or "") for msg in second_history)

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


def test_manual_runs_do_not_read_or_write_automation_session(monkeypatch, tmp_path) -> None:
    schedule_messages: list[list[dict[str, object]]] = []
    manual_calls: list[tuple[str, str]] = []

    def fake_run_agent_with_messages(*, messages, **kwargs):
        del kwargs
        schedule_messages.append(messages)
        return _fake_run_result("scheduled decision")

    def fake_run_agent(settings, client, emit=None):
        del client, emit
        manual_calls.append((str(settings.run_type), str(settings.task_prompt)))
        return _fake_run_result("manual decision")

    monkeypatch.setattr(llm_service, "run_agent_with_messages", fake_run_agent_with_messages)
    monkeypatch.setattr(llm_service, "run_agent", fake_run_agent)

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
        assert manual_run.chat_session_id is None
        assert manual_run.prompt_message_id is None
        assert manual_run.response_message_id is None
        assert len(schedule_messages) == 1
        assert len(manual_calls) == 1
        assert "[自动任务触发]" not in manual_calls[0][1]

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
            assert len(messages) == 2
            assert all(record.run_id != manual_run.id for record in messages)

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
            assert "[本轮失败摘要]" in messages[1].content
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
