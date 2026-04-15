from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.db.database import init_db
from app.db.models import StrategySchedule
from app.services.aniu_service import aniu_service
from app.services.mx_skill_service import mx_skill_service
from app.services.llm_service import llm_service


def test_execute_run_rejects_unknown_schedule_id(monkeypatch, tmp_path) -> None:
    from app.core.config import get_settings
    from app.db import database as database_module
    from app.services.trading_calendar_service import trading_calendar_service

    monkeypatch.setenv("SQLITE_DB_PATH", str(tmp_path / "guards.db"))
    monkeypatch.setattr(trading_calendar_service, "ensure_years", lambda years: None)
    get_settings.cache_clear()
    database_module._engine = None
    database_module._session_local = None
    init_db()

    with pytest.raises(RuntimeError, match="指定的定时任务不存在"):
        aniu_service.execute_run(schedule_id=999999)

    database_module._engine = None
    database_module._session_local = None
    get_settings.cache_clear()


def test_moni_trade_requires_limit_price() -> None:
    with pytest.raises(RuntimeError, match="LIMIT 委托必须提供有效价格"):
        mx_skill_service._handle_moni_trade(
            client=None,
            app_settings=None,
            arguments={
                "action": "BUY",
                "symbol": "600519.SH",
                "quantity": 100,
                "price_type": "LIMIT",
            },
        )


def test_moni_trade_rejects_non_positive_limit_price() -> None:
    with pytest.raises(RuntimeError, match="LIMIT 委托价格必须大于 0"):
        mx_skill_service._handle_moni_trade(
            client=None,
            app_settings=None,
            arguments={
                "action": "BUY",
                "symbol": "600519.SH",
                "quantity": 100,
                "price_type": "LIMIT",
                "price": 0,
            },
        )


def test_resolve_run_type_maps_schedule_names() -> None:
    assert aniu_service._resolve_run_type(None) == "analysis"
    assert aniu_service._resolve_run_type(StrategySchedule(name="盘前分析", run_type="analysis")) == "analysis"
    assert aniu_service._resolve_run_type(StrategySchedule(name="午间复盘", run_type="analysis")) == "analysis"
    assert aniu_service._resolve_run_type(StrategySchedule(name="收盘分析", run_type="analysis")) == "analysis"
    assert aniu_service._resolve_run_type(StrategySchedule(name="上午运行1号", run_type="trade")) == "trade"
    assert aniu_service._resolve_run_type(StrategySchedule(name="下午运行2号", run_type="trade")) == "trade"


def test_resolve_run_type_falls_back_to_name_when_schedule_type_missing() -> None:
    assert aniu_service._resolve_run_type(StrategySchedule(name="上午运行1号", run_type="")) == "trade"
    assert aniu_service._resolve_run_type(StrategySchedule(name="收盘分析", run_type="")) == "analysis"


def test_infer_run_type_recovers_trade_runs_from_schedule_name() -> None:
    run = SimpleNamespace(
        schedule_name="上午运行1号",
        trade_orders=[],
        executed_actions=None,
        skill_payloads=None,
        decision_payload=None,
        run_type="analysis",
    )

    assert aniu_service._infer_run_type(run) == "trade"


def test_infer_run_type_recovers_trade_runs_from_actions() -> None:
    run = SimpleNamespace(
        schedule_name=None,
        trade_orders=[],
        executed_actions=[{"action": "BUY", "symbol": "300059"}],
        skill_payloads=None,
        decision_payload=None,
        run_type="analysis",
    )

    assert aniu_service._infer_run_type(run) == "trade"


def test_build_tools_excludes_trade_mutations_for_analysis_runs() -> None:
    tools = mx_skill_service.build_tools(run_type="analysis")
    names = {tool["function"]["name"] for tool in tools}

    assert "mx_moni_trade" not in names
    assert "mx_moni_cancel" not in names
    assert "mx_query_market" in names
    assert "mx_get_positions" in names


def test_build_tools_includes_trade_mutations_for_trade_runs() -> None:
    tools = mx_skill_service.build_tools(run_type="trade")
    names = {tool["function"]["name"] for tool in tools}

    assert "mx_moni_trade" in names
    assert "mx_moni_cancel" in names


def test_build_initial_request_payload_uses_run_type_tool_profile() -> None:
    app_settings = SimpleNamespace(
        llm_model="demo-model",
        system_prompt="system",
        task_prompt="task",
        run_type="analysis",
    )

    payload = llm_service.build_initial_request_payload(app_settings)
    names = {tool["function"]["name"] for tool in payload["tools"]}

    assert "mx_moni_trade" not in names
    assert "mx_query_market" in names


def test_execute_tool_adds_guidance_for_api_key_error() -> None:
    def boom(*, client, app_settings, arguments):
        del client, app_settings, arguments
        raise RuntimeError("401 Unauthorized / API密钥不存在")

    original_handler = mx_skill_service._handlers["mx_get_balance"]
    mx_skill_service._handlers["mx_get_balance"] = boom
    try:
        result = mx_skill_service.execute_tool(
            client=None,
            app_settings=None,
            tool_name="mx_get_balance",
            arguments={},
        )
    finally:
        mx_skill_service._handlers["mx_get_balance"] = original_handler

    assert result["ok"] is False
    assert "请检查 MX_APIKEY" in result["error"]


def test_screen_tool_returns_raw_result_without_normalized() -> None:
    class StubClient:
        def screen_stocks(self, query):
            del query
            return {
                "data": {
                    "data": {
                        "allResults": {
                            "result": {
                                "total": 1,
                                "columns": [
                                    {"key": "SECURITY_CODE", "title": "代码"},
                                    {"key": "SECURITY_SHORT_NAME", "title": "名称"},
                                    {"key": "NEWEST_PRICE", "title": "最新价"},
                                ],
                                "dataList": [
                                    {
                                        "SECURITY_CODE": "300059",
                                        "SECURITY_SHORT_NAME": "东方财富",
                                        "NEWEST_PRICE": "20.01",
                                    }
                                ],
                            }
                        }
                    }
                }
            }

    result = mx_skill_service._handle_screen_stocks(
        client=StubClient(),
        app_settings=SimpleNamespace(task_prompt=""),
        arguments={"query": "低估值股票"},
    )

    assert result["ok"] is True
    assert "normalized" not in result
    rows = result["result"]["data"]["data"]["allResults"]["result"]["dataList"]
    assert rows[0]["SECURITY_CODE"] == "300059"
