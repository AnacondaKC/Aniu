from datetime import datetime
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.core.config import get_settings
from app.db import database as database_module
from app.db.database import session_scope
from app.db.models import StrategyRun
from app.main import create_app
from app.services.scheduler_service import scheduler_service
from app.services.trading_calendar_service import trading_calendar_service


def create_test_client(monkeypatch, tmp_path) -> TestClient:
    from app.services.aniu_service import aniu_service

    monkeypatch.setenv("APP_LOGIN_PASSWORD", "release-pass")
    monkeypatch.setenv("SQLITE_DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setattr(trading_calendar_service, "ensure_years", lambda years: None)
    monkeypatch.setattr(scheduler_service, "start", lambda: None)
    monkeypatch.setattr(scheduler_service, "stop", lambda: None)
    get_settings.cache_clear()
    database_module._engine = None
    database_module._session_local = None
    aniu_service._account_overview_cache = None
    aniu_service._account_overview_cache_expires_at = None
    app = create_app()
    return TestClient(app)


def _auth_headers(client: TestClient) -> dict[str, str]:
    response = client.post(
        "/api/aniu/login",
        json={"password": "release-pass"},
    )
    payload = response.json()
    return {"Authorization": f"Bearer {payload['token']}"}


def test_login_endpoint_accepts_configured_credentials(monkeypatch, tmp_path) -> None:
    with create_test_client(monkeypatch, tmp_path) as client:
        response = client.post(
            "/api/aniu/login",
            json={"password": "release-pass"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["authenticated"] is True
    assert payload["token"]
    database_module._engine = None
    database_module._session_local = None
    get_settings.cache_clear()


def test_login_endpoint_rejects_invalid_credentials(monkeypatch, tmp_path) -> None:
    with create_test_client(monkeypatch, tmp_path) as client:
        response = client.post(
            "/api/aniu/login",
            json={"password": "wrong-password"},
        )

    assert response.status_code == 401
    database_module._engine = None
    database_module._session_local = None
    get_settings.cache_clear()


def test_app_startup_requires_current_year_trading_calendar(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("APP_LOGIN_PASSWORD", "release-pass")
    monkeypatch.setenv("SQLITE_DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setattr(
        trading_calendar_service,
        "ensure_years",
        lambda years: (_ for _ in ()).throw(RuntimeError("calendar unavailable"))
        if years == [2026]
        else None,
    )
    monkeypatch.setattr(scheduler_service, "start", lambda: None)
    monkeypatch.setattr(scheduler_service, "stop", lambda: None)
    monkeypatch.setattr("app.main.date", type("FakeDate", (), {"today": staticmethod(lambda: type("Today", (), {"year": 2026})())}))
    get_settings.cache_clear()
    database_module._engine = None
    database_module._session_local = None

    app = create_app()

    with pytest.raises(RuntimeError, match="calendar unavailable"):
        with TestClient(app):
            pass

    database_module._engine = None
    database_module._session_local = None
    get_settings.cache_clear()


def test_chat_endpoint_returns_assistant_message(monkeypatch, tmp_path) -> None:
    from app.services.aniu_service import aniu_service

    monkeypatch.setattr(
        aniu_service,
        "chat",
        lambda payload: {
            "message": {
                "role": "assistant",
                "content": "测试回复",
            },
            "context": {
                "include_system_prompt": True,
                "include_account_summary": False,
                "include_positions_orders": False,
                "include_latest_run_summary": False,
            },
        },
    )

    with create_test_client(monkeypatch, tmp_path) as client:
        headers = _auth_headers(client)
        response = client.post(
            "/api/aniu/chat",
            json={
                "messages": [
                    {"role": "user", "content": "你好"},
                ],
                "include_system_prompt": True,
                "include_account_summary": False,
                "include_positions_orders": False,
                "include_latest_run_summary": False,
            },
            headers=headers,
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["message"]["role"] == "assistant"
    assert payload["message"]["content"] == "测试回复"

    database_module._engine = None
    database_module._session_local = None
    get_settings.cache_clear()


def test_chat_endpoint_rejects_empty_messages(monkeypatch, tmp_path) -> None:
    with create_test_client(monkeypatch, tmp_path) as client:
        headers = _auth_headers(client)
        response = client.post(
            "/api/aniu/chat",
            json={
                "messages": [],
                "include_system_prompt": True,
                "include_account_summary": False,
                "include_positions_orders": False,
                "include_latest_run_summary": False,
            },
            headers=headers,
        )

    assert response.status_code == 422
    database_module._engine = None
    database_module._session_local = None
    get_settings.cache_clear()


def test_runs_endpoint_returns_lightweight_summary(monkeypatch, tmp_path) -> None:
    with create_test_client(monkeypatch, tmp_path) as client:
        with session_scope() as db:
            db.add(
                StrategyRun(
                    trigger_source="manual",
                    run_type="analysis",
                    schedule_name="盘前分析",
                    status="completed",
                    analysis_summary="摘要",
                    final_answer="详细输出",
                    decision_payload={
                        "tool_calls": [
                            {"name": "mx_query_market"},
                            {"name": "mx_moni_trade"},
                        ]
                    },
                    executed_actions=[{"action": "BUY", "symbol": "300059"}],
                    llm_response_payload={
                        "usage": {
                            "prompt_tokens": 11,
                            "completion_tokens": 22,
                            "total_tokens": 33,
                        }
                    },
                )
            )

        headers = _auth_headers(client)
        response = client.get("/api/aniu/runs?limit=20", headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    run = payload[0]
    assert run["analysis_summary"] == "摘要"
    assert run["api_call_count"] == 1
    assert run["executed_trade_count"] == 1
    assert run["input_tokens"] == 11
    assert run["output_tokens"] == 22
    assert run["total_tokens"] == 33
    assert "final_answer" not in run
    assert "decision_payload" not in run
    assert "executed_actions" not in run

    database_module._engine = None
    database_module._session_local = None
    get_settings.cache_clear()


def test_runs_endpoint_filters_by_date(monkeypatch, tmp_path) -> None:
    with create_test_client(monkeypatch, tmp_path) as client:
        with session_scope() as db:
            db.add_all(
                [
                    StrategyRun(
                        trigger_source="manual",
                        run_type="analysis",
                        status="completed",
                        analysis_summary="today",
                        started_at=datetime(2026, 4, 14, 8, 30, 0),
                    ),
                    StrategyRun(
                        trigger_source="manual",
                        run_type="analysis",
                        status="completed",
                        analysis_summary="yesterday",
                        started_at=datetime(2026, 4, 13, 8, 30, 0),
                    ),
                ]
            )

        headers = _auth_headers(client)
        response = client.get("/api/aniu/runs?date=2026-04-14&limit=20", headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["analysis_summary"] == "today"

    database_module._engine = None
    database_module._session_local = None
    get_settings.cache_clear()


def test_runs_feed_returns_pagination_metadata(monkeypatch, tmp_path) -> None:
    with create_test_client(monkeypatch, tmp_path) as client:
        with session_scope() as db:
            db.add_all(
                [
                    StrategyRun(
                        trigger_source="manual",
                        run_type="analysis",
                        status="completed",
                        analysis_summary=f"run-{index}",
                        started_at=datetime(2026, 4, 14, 8, index, 0),
                    )
                    for index in range(3)
                ]
            )

        headers = _auth_headers(client)
        response = client.get("/api/aniu/runs-feed?limit=2", headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 2
    assert payload["has_more"] is True
    assert payload["next_before_id"] is not None

    database_module._engine = None
    database_module._session_local = None
    get_settings.cache_clear()


def test_runtime_overview_endpoint_returns_aggregated_stats(monkeypatch, tmp_path) -> None:
    shanghai_now = datetime.now(ZoneInfo("Asia/Shanghai")).replace(
        hour=12,
        minute=0,
        second=0,
        microsecond=0,
    )

    with create_test_client(monkeypatch, tmp_path) as client:
        with session_scope() as db:
            db.add_all(
                [
                    StrategyRun(
                        trigger_source="manual",
                        run_type="analysis",
                        status="completed",
                        analysis_summary="today-1",
                        decision_payload={
                            "tool_calls": [
                                {"name": "mx_query_market"},
                                {"name": "mx_search_news"},
                                {"name": "mx_moni_trade"},
                            ]
                        },
                        executed_actions=[{"action": "BUY", "symbol": "300059"}],
                        llm_response_payload={
                            "usage": {
                                "prompt_tokens": 10,
                                "completion_tokens": 20,
                                "total_tokens": 30,
                            }
                        },
                        started_at=shanghai_now.replace(tzinfo=None),
                        finished_at=shanghai_now.replace(tzinfo=None),
                    ),
                    StrategyRun(
                        trigger_source="manual",
                        run_type="analysis",
                        status="failed",
                        analysis_summary="today-2",
                        decision_payload={
                            "tool_calls": [
                                {"name": "mx_get_balance"},
                            ]
                        },
                        llm_response_payload={
                            "usage": {
                                "prompt_tokens": 5,
                                "completion_tokens": 6,
                                "total_tokens": 11,
                            }
                        },
                        started_at=shanghai_now.replace(tzinfo=None),
                        finished_at=shanghai_now.replace(tzinfo=None),
                    ),
                ]
            )

        headers = _auth_headers(client)
        response = client.get("/api/aniu/runtime-overview", headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload["today"]["analysis_count"] == 2
    assert payload["today"]["api_calls"] == 3
    assert payload["today"]["trades"] == 1
    assert payload["today"]["success_rate"] == 50.0

    database_module._engine = None
    database_module._session_local = None
    get_settings.cache_clear()


def test_account_endpoint_excludes_raw_payloads_by_default(monkeypatch, tmp_path) -> None:
    from app.services.aniu_service import aniu_service

    monkeypatch.setattr(
        aniu_service,
        "get_account_overview",
        lambda **kwargs: {
            "open_date": None,
            "daily_profit_trade_date": None,
            "operating_days": None,
            "initial_capital": None,
            "total_assets": None,
            "total_market_value": None,
            "cash_balance": None,
            "total_position_ratio": None,
            "holding_profit": None,
            "total_return_ratio": None,
            "nav": None,
            "daily_profit": None,
            "daily_return_ratio": None,
            "positions": [],
            "orders": [],
            "trade_summaries": [],
            "errors": [],
        },
    )

    with create_test_client(monkeypatch, tmp_path) as client:
        headers = _auth_headers(client)
        response = client.get("/api/aniu/account", headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert "raw_balance" not in payload
    assert "raw_positions" not in payload
    assert "raw_orders" not in payload

    database_module._engine = None
    database_module._session_local = None
    get_settings.cache_clear()


def test_account_debug_endpoint_includes_raw_payloads(monkeypatch, tmp_path) -> None:
    from app.services.aniu_service import aniu_service

    monkeypatch.setattr(
        aniu_service,
        "get_account_overview",
        lambda **kwargs: {
            "open_date": None,
            "daily_profit_trade_date": None,
            "operating_days": None,
            "initial_capital": None,
            "total_assets": None,
            "total_market_value": None,
            "cash_balance": None,
            "total_position_ratio": None,
            "holding_profit": None,
            "total_return_ratio": None,
            "nav": None,
            "daily_profit": None,
            "daily_return_ratio": None,
            "positions": [],
            "orders": [],
            "trade_summaries": [],
            "raw_balance": {"a": 1},
            "raw_positions": {"b": 2},
            "raw_orders": {"c": 3},
            "errors": [],
        },
    )

    with create_test_client(monkeypatch, tmp_path) as client:
        headers = _auth_headers(client)
        response = client.get("/api/aniu/account/debug", headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload["raw_balance"] == {"a": 1}
    assert payload["raw_positions"] == {"b": 2}
    assert payload["raw_orders"] == {"c": 3}

    database_module._engine = None
    database_module._session_local = None
    get_settings.cache_clear()
