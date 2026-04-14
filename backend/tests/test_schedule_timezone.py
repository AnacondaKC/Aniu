from datetime import date, datetime, timezone
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.core.config import get_settings
from app.db import database as database_module
from app.db.database import init_db, session_scope
from app.db.models import StrategyRun, StrategySchedule
from app.services.aniu_service import aniu_service


def _use_temp_db(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("SQLITE_DB_PATH", str(tmp_path / "test.db"))
    get_settings.cache_clear()
    database_module._engine = None
    database_module._session_local = None


def _reset_db_state() -> None:
    database_module._engine = None
    database_module._session_local = None
    get_settings.cache_clear()


def test_compute_next_run_at_returns_utc() -> None:
    result = aniu_service._compute_next_run_at("15 7 * * 1-5")

    assert result is not None
    assert result.tzinfo == timezone.utc


def test_compute_next_run_at_recomputes_future_time_in_utc() -> None:
    shanghai = ZoneInfo("Asia/Shanghai")
    start = datetime(2026, 4, 12, 8, 46, tzinfo=shanghai)

    result = aniu_service._compute_next_run_at("45 8 * * 1-5", from_time=start)

    assert result is not None
    assert result.tzinfo == timezone.utc
    in_shanghai = result.astimezone(shanghai)
    assert in_shanghai.hour == 8
    assert in_shanghai.minute == 45
    assert in_shanghai.date().isoformat() == "2026-04-13"


def test_compute_next_run_at_preserves_utc_timezone() -> None:
    shanghai = ZoneInfo("Asia/Shanghai")
    start = datetime(2026, 4, 12, 7, 44, tzinfo=shanghai)

    result = aniu_service._compute_next_run_at("45 7 * * 1-5", from_time=start)

    assert result is not None
    assert result.tzinfo == timezone.utc


def test_schedule_datetimes_are_stored_as_utc() -> None:
    shanghai = ZoneInfo("Asia/Shanghai")
    result = aniu_service._compute_next_run_at(
        "15 7 * * 1-5",
        from_time=datetime(2026, 4, 12, 7, 0, tzinfo=shanghai),
    )

    assert result is not None
    assert result.isoformat().endswith("+00:00")
    in_shanghai = result.astimezone(shanghai)
    assert in_shanghai.hour == 7
    assert in_shanghai.minute == 15


def test_compute_next_run_at_skips_weekend_to_next_trading_day() -> None:
    shanghai = ZoneInfo("Asia/Shanghai")
    start = datetime(2026, 4, 11, 8, 0, tzinfo=shanghai)

    result = aniu_service._compute_next_run_at("0 8 * * 1-5", from_time=start)

    assert result is not None
    assert result.date().isoformat() == "2026-04-13"


def test_compute_next_run_at_skips_non_trading_holiday() -> None:
    from app.services import aniu_service as aniu_service_module

    shanghai = ZoneInfo("Asia/Shanghai")
    start = datetime(2026, 10, 1, 8, 0, tzinfo=shanghai)

    original_is_trading_day = aniu_service_module.trading_calendar_service.is_trading_day
    original_next_trading_day = (
        aniu_service_module.trading_calendar_service.next_trading_day
    )
    holiday_break = {
        "2026-10-01",
        "2026-10-02",
        "2026-10-05",
        "2026-10-06",
        "2026-10-07",
        "2026-10-08",
    }
    aniu_service_module.trading_calendar_service.is_trading_day = lambda current: (
        current.isoformat() not in holiday_break and current.weekday() < 5
    )
    aniu_service_module.trading_calendar_service.next_trading_day = lambda current: date(
        2026, 10, 9
    )

    try:
        result = aniu_service._compute_next_run_at("0 8 * * 1-5", from_time=start)
    finally:
        aniu_service_module.trading_calendar_service.is_trading_day = (
            original_is_trading_day
        )
        aniu_service_module.trading_calendar_service.next_trading_day = (
            original_next_trading_day
        )

    assert result is not None
    assert result.date().isoformat() == "2026-10-09"


def test_process_due_schedule_recomputes_non_trading_due_task_without_running(
    monkeypatch, tmp_path
) -> None:
    _use_temp_db(monkeypatch, tmp_path)
    init_db()
    shanghai = ZoneInfo("Asia/Shanghai")

    with session_scope() as db:
        schedule = StrategySchedule(
            name="测试非交易日任务",
            cron_expression="0 8 * * 1-5",
            task_prompt="test",
            timeout_seconds=1800,
            enabled=True,
            next_run_at=datetime(2026, 10, 1, 0, 0),
        )
        db.add(schedule)
        db.flush()
        schedule_id = schedule.id

    from app.services import aniu_service as aniu_service_module

    original_now_shanghai = aniu_service_module.now_shanghai
    original_is_trading_day = aniu_service_module.trading_calendar_service.is_trading_day
    original_next_trading_day = (
        aniu_service_module.trading_calendar_service.next_trading_day
    )
    holiday_break = {
        "2026-10-01",
        "2026-10-02",
        "2026-10-05",
        "2026-10-06",
        "2026-10-07",
        "2026-10-08",
    }
    aniu_service_module.now_shanghai = lambda: datetime(
        2026, 10, 1, 8, 1, tzinfo=shanghai
    )
    aniu_service_module.trading_calendar_service.is_trading_day = lambda current: (
        current.isoformat() not in holiday_break and current.weekday() < 5
    )
    aniu_service_module.trading_calendar_service.next_trading_day = lambda current: date(
        2026, 10, 9
    )
    try:
        aniu_service.process_due_schedule()
    finally:
        aniu_service_module.now_shanghai = original_now_shanghai
        aniu_service_module.trading_calendar_service.is_trading_day = (
            original_is_trading_day
        )
        aniu_service_module.trading_calendar_service.next_trading_day = (
            original_next_trading_day
        )
        _reset_db_state()

    with session_scope() as db:
        saved = db.get(StrategySchedule, schedule_id)
        assert saved is not None
        assert saved.last_run_at is None
        assert saved.next_run_at is not None
        assert saved.next_run_at.date().isoformat() == "2026-10-09"


def test_execute_run_failure_advances_schedule_window(monkeypatch, tmp_path) -> None:
    _use_temp_db(monkeypatch, tmp_path)
    init_db()
    shanghai = ZoneInfo("Asia/Shanghai")

    with session_scope() as db:
        schedule = StrategySchedule(
            name="收盘分析",
            cron_expression="30 15 * * 1-5",
            task_prompt="test",
            timeout_seconds=1800,
            enabled=True,
            next_run_at=datetime(2026, 4, 13, 7, 30, tzinfo=timezone.utc),
        )
        db.add(schedule)
        db.flush()
        schedule_id = schedule.id

    from app.services import aniu_service as aniu_service_module

    original_now_shanghai = aniu_service_module.now_shanghai
    monkeypatch.setattr(
        aniu_service_module,
        "now_shanghai",
        lambda: datetime(2026, 4, 13, 15, 31, tzinfo=shanghai),
    )
    monkeypatch.setattr(
        aniu_service_module.aniu_service,
        "get_or_create_settings",
        lambda db: type("StubSettings", (), {
            "id": 1,
            "mx_api_key": "demo-key",
            "llm_base_url": "https://example.com/v1",
            "llm_api_key": "token",
            "llm_model": "demo-model",
            "system_prompt": "prompt",
            "timeout_seconds": 1800,
        })(),
    )
    monkeypatch.setattr(
        aniu_service_module.llm_service,
        "run_agent",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        aniu_service_module,
        "now_utc",
        lambda: datetime(2026, 4, 13, 7, 31, tzinfo=timezone.utc),
    )

    try:
        try:
            aniu_service.execute_run(trigger_source="schedule", schedule_id=schedule_id)
        except RuntimeError as exc:
            assert str(exc) == "boom"
    finally:
        aniu_service_module.now_shanghai = original_now_shanghai
        _reset_db_state()

    with session_scope() as db:
        saved = db.get(StrategySchedule, schedule_id)
        assert saved is not None
        assert saved.last_run_at is not None
        assert saved.next_run_at is not None
        assert saved.retry_count == 1
        assert saved.retry_after_at is not None
        next_run = saved.next_run_at
        if next_run.tzinfo is None:
            next_run = next_run.replace(tzinfo=timezone.utc)
        retry_after = saved.retry_after_at
        if retry_after is not None and retry_after.tzinfo is None:
            retry_after = retry_after.replace(tzinfo=timezone.utc)
        next_run_shanghai = next_run.astimezone(shanghai)
        assert next_run_shanghai.date().isoformat() == "2026-04-14"
        assert next_run_shanghai.hour == 15
        assert next_run_shanghai.minute == 30
        assert retry_after is not None
        assert retry_after.isoformat() == "2026-04-13T07:36:00+00:00"


def test_execute_run_failure_stops_retry_after_third_retry(monkeypatch, tmp_path) -> None:
    _use_temp_db(monkeypatch, tmp_path)
    init_db()
    shanghai = ZoneInfo("Asia/Shanghai")

    with session_scope() as db:
        schedule = StrategySchedule(
            name="收盘分析",
            cron_expression="30 15 * * 1-5",
            task_prompt="test",
            timeout_seconds=1800,
            enabled=True,
            retry_count=3,
            retry_after_at=datetime(2026, 4, 13, 7, 20, tzinfo=timezone.utc),
            next_run_at=datetime(2026, 4, 13, 7, 30, tzinfo=timezone.utc),
        )
        db.add(schedule)
        db.flush()
        schedule_id = schedule.id

    from app.services import aniu_service as aniu_service_module

    original_now_shanghai = aniu_service_module.now_shanghai
    monkeypatch.setattr(
        aniu_service_module,
        "now_shanghai",
        lambda: datetime(2026, 4, 13, 15, 31, tzinfo=shanghai),
    )
    monkeypatch.setattr(
        aniu_service_module.aniu_service,
        "get_or_create_settings",
        lambda db: type("StubSettings", (), {
            "id": 1,
            "mx_api_key": "demo-key",
            "llm_base_url": "https://example.com/v1",
            "llm_api_key": "token",
            "llm_model": "demo-model",
            "system_prompt": "prompt",
            "timeout_seconds": 1800,
        })(),
    )
    monkeypatch.setattr(
        aniu_service_module.llm_service,
        "run_agent",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        aniu_service_module,
        "now_utc",
        lambda: datetime(2026, 4, 13, 7, 31, tzinfo=timezone.utc),
    )

    try:
        try:
            aniu_service.execute_run(trigger_source="schedule", schedule_id=schedule_id)
        except RuntimeError as exc:
            assert str(exc) == "boom"
    finally:
        aniu_service_module.now_shanghai = original_now_shanghai
        _reset_db_state()

    with session_scope() as db:
        saved = db.get(StrategySchedule, schedule_id)
        assert saved is not None
        assert saved.retry_count == 0
        assert saved.retry_after_at is None


def test_manual_failure_does_not_increment_retry_count(monkeypatch, tmp_path) -> None:
    _use_temp_db(monkeypatch, tmp_path)
    init_db()
    shanghai = ZoneInfo("Asia/Shanghai")

    with session_scope() as db:
        schedule = StrategySchedule(
            name="收盘分析",
            cron_expression="30 15 * * 1-5",
            task_prompt="test",
            timeout_seconds=1800,
            enabled=True,
            retry_count=2,
            retry_after_at=datetime(2026, 4, 13, 7, 20, tzinfo=timezone.utc),
            next_run_at=datetime(2026, 4, 13, 7, 30, tzinfo=timezone.utc),
        )
        db.add(schedule)
        db.flush()
        schedule_id = schedule.id

    from app.services import aniu_service as aniu_service_module

    original_now_shanghai = aniu_service_module.now_shanghai
    monkeypatch.setattr(
        aniu_service_module,
        "now_shanghai",
        lambda: datetime(2026, 4, 13, 15, 31, tzinfo=shanghai),
    )
    monkeypatch.setattr(
        aniu_service_module.aniu_service,
        "get_or_create_settings",
        lambda db: type("StubSettings", (), {
            "id": 1,
            "mx_api_key": "demo-key",
            "llm_base_url": "https://example.com/v1",
            "llm_api_key": "token",
            "llm_model": "demo-model",
            "system_prompt": "prompt",
            "timeout_seconds": 1800,
        })(),
    )
    monkeypatch.setattr(
        aniu_service_module.llm_service,
        "run_agent",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        aniu_service_module,
        "now_utc",
        lambda: datetime(2026, 4, 13, 7, 31, tzinfo=timezone.utc),
    )

    try:
        try:
            aniu_service.execute_run(trigger_source="manual", schedule_id=schedule_id)
        except RuntimeError as exc:
            assert str(exc) == "boom"
    finally:
        aniu_service_module.now_shanghai = original_now_shanghai
        _reset_db_state()

    with session_scope() as db:
        saved = db.get(StrategySchedule, schedule_id)
        assert saved is not None
        assert saved.retry_count == 2
        assert saved.retry_after_at is not None


def test_process_due_schedule_runs_due_retry_when_window_arrives(monkeypatch, tmp_path) -> None:
    _use_temp_db(monkeypatch, tmp_path)
    init_db()
    shanghai = ZoneInfo("Asia/Shanghai")

    with session_scope() as db:
        retry_schedule = StrategySchedule(
            name="收盘分析",
            cron_expression="30 15 * * 1-5",
            task_prompt="test",
            timeout_seconds=1800,
            enabled=True,
            retry_count=1,
            retry_after_at=datetime(2026, 4, 13, 7, 20, tzinfo=timezone.utc),
            next_run_at=datetime(2026, 4, 14, 7, 30, tzinfo=timezone.utc),
        )
        normal_schedule = StrategySchedule(
            name="盘前分析",
            cron_expression="0 8 * * 1-5",
            task_prompt="test",
            timeout_seconds=1800,
            enabled=True,
            next_run_at=datetime(2026, 4, 14, 0, 0, tzinfo=timezone.utc),
        )
        db.add(retry_schedule)
        db.add(normal_schedule)
        db.flush()
        retry_id = retry_schedule.id
        normal_id = normal_schedule.id

    from app.services import aniu_service as aniu_service_module

    original_now_shanghai = aniu_service_module.now_shanghai
    original_is_trading_day = aniu_service_module.trading_calendar_service.is_trading_day
    called: list[tuple[str, int | None]] = []

    monkeypatch.setattr(
        aniu_service_module,
        "now_shanghai",
        lambda: datetime(2026, 4, 13, 15, 31, tzinfo=shanghai),
    )
    monkeypatch.setattr(
        aniu_service_module.trading_calendar_service,
        "is_trading_day",
        lambda current: True,
    )
    monkeypatch.setattr(
        aniu_service_module.aniu_service,
        "execute_run",
        lambda trigger_source="manual", schedule_id=None: called.append((trigger_source, schedule_id)),
    )

    try:
        aniu_service.process_due_schedule()
    finally:
        aniu_service_module.now_shanghai = original_now_shanghai
        aniu_service_module.trading_calendar_service.is_trading_day = original_is_trading_day
        _reset_db_state()

    assert called == [("schedule", retry_id)]
    assert retry_id != normal_id


def test_trading_calendar_service_can_fill_missing_year(monkeypatch, tmp_path) -> None:
    from app.services.trading_calendar_service import TradingCalendarService

    service = TradingCalendarService()
    service._data_path = tmp_path / "trading_calendar.json"
    service._calendar = {"version": 1, "source": "codebuddy_trade_cal", "years": {}}

    monkeypatch.setattr(
        service,
        "_fetch_year",
        lambda year: ["2027-01-04", "2027-01-05"],
    )

    service.ensure_years([2027])

    assert service._calendar["years"]["2027"]["trading_days"] == [
        "2027-01-04",
        "2027-01-05",
    ]
    assert service._data_path.exists()


def test_trading_calendar_service_warms_current_year_and_tolerates_next_year_failure(
    monkeypatch, tmp_path
) -> None:
    from app.services.trading_calendar_service import TradingCalendarService

    service = TradingCalendarService()
    service._data_path = tmp_path / "trading_calendar.json"
    service._calendar = {"version": 1, "source": "codebuddy_trade_cal", "years": {}}

    def fake_query(year: int) -> list[str]:
        if year == 2026:
            return ["2026-01-05"]
        raise RuntimeError("next year unavailable")

    monkeypatch.setattr(service, "_fetch_year", fake_query)

    service.warm_up_years(2026)

    assert service._calendar["years"]["2026"]["trading_days"] == ["2026-01-05"]
    assert "2027" not in service._calendar["years"]


def test_trading_calendar_service_normalizes_http_trade_cal_rows(tmp_path) -> None:
    from app.services.trading_calendar_service import TradingCalendarService

    service = TradingCalendarService()
    service._data_path = tmp_path / "trading_calendar.json"
    rows = service._normalize_rows(
        ["exchange", "cal_date", "is_open", "pretrade_date"],
        [
            ["SSE", "20270102", 1, "20261231"],
            ["SSE", "20270103", 0, "20270102"],
            ["SSE", "20270104", "1", "20270102"],
        ],
    )

    trading_days = [
        service._normalize_calendar_date(str(row["cal_date"]))
        for row in rows
        if service._is_open_value(row.get("is_open"))
    ]

    assert trading_days == ["2027-01-02", "2027-01-04"]


def test_trading_calendar_service_retries_remote_fetch_until_success() -> None:
    from app.services.trading_calendar_service import TradingCalendarService

    service = TradingCalendarService()
    attempts: list[int] = []

    def fake_fetch_once(year: int) -> list[str]:
        attempts.append(year)
        if len(attempts) < 3:
            raise RuntimeError("temporary failure")
        return ["2027-01-04"]

    service._fetch_year_once = fake_fetch_once  # type: ignore[method-assign]

    result = service._fetch_year(2027)

    assert result == ["2027-01-04"]
    assert attempts == [2027, 2027, 2027]


def test_trading_calendar_service_raises_after_retry_limit() -> None:
    from app.services.trading_calendar_service import TradingCalendarService

    service = TradingCalendarService()
    attempts: list[int] = []

    def fake_fetch_once(year: int) -> list[str]:
        attempts.append(year)
        raise RuntimeError("temporary failure")

    service._fetch_year_once = fake_fetch_once  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="已重试 3 次仍失败"):
        service._fetch_year(2027)

    assert attempts == [2027, 2027, 2027, 2027]


def test_order_status_text_derives_from_fill_progress() -> None:
    from app.services.aniu_service import _order_status_text

    assert _order_status_text(2, order_quantity=200, filled_quantity=0) == "已报"
    assert _order_status_text("2", order_quantity=200, filled_quantity=100) == "部分成交"
    assert _order_status_text("2", order_quantity=200, filled_quantity=200) == "已成交"


def test_account_overview_prefers_live_positions_over_cached_snapshot(monkeypatch) -> None:
    from app.services import aniu_service as aniu_service_module

    live_balance = {
        "data": {
            "totalAsset": 120000,
            "stockMarketValue": 5000,
            "balanceActual": 115000,
        }
    }
    cached_balance = {
        "data": {
            "totalAsset": 100000,
            "stockMarketValue": 0,
            "balanceActual": 100000,
        }
    }
    live_positions = {
        "data": {
            "rows": [
                {
                    "stockCode": "300373",
                    "stockName": "扬杰科技",
                    "marketValue": 5000,
                    "count": 100,
                    "availCount": 100,
                }
            ]
        }
    }
    cached_positions = {"data": {"rows": []}}
    live_orders = {
        "data": {
            "rows": [
                {
                    "orderId": "1",
                    "stockCode": "300373",
                    "stockName": "扬杰科技",
                    "orderStatus": 2,
                    "orderDrt": 1,
                }
            ]
        }
    }

    class StubClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def get_balance(self) -> dict[str, object]:
            return live_balance

        def get_positions(self) -> dict[str, object]:
            return live_positions

        def get_orders(self) -> dict[str, object]:
            return live_orders

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        aniu_service_module.aniu_service,
        "get_or_create_settings",
        lambda db: type("StubSettings", (), {"mx_api_key": "demo-key"})(),
    )
    monkeypatch.setattr(
        aniu_service_module.aniu_service,
        "_get_recent_account_snapshot",
        lambda db: (cached_balance, cached_positions, None),
    )
    monkeypatch.setattr(aniu_service_module, "MXClient", StubClient)

    overview = aniu_service_module.aniu_service.get_account_overview(force_refresh=True)

    assert overview["positions"]
    assert overview["positions"][0]["name"] == "扬杰科技"
    assert overview["orders"][0]["status_text"] == "已报"
    assert overview["raw_positions"] == live_positions
    assert overview["raw_balance"] == live_balance


def test_account_overview_falls_back_to_cached_orders_when_live_orders_fail(
    monkeypatch,
) -> None:
    from app.services import aniu_service as aniu_service_module

    live_balance = {
        "data": {
            "totalAsset": 120000,
            "stockMarketValue": 5000,
            "balanceActual": 115000,
        }
    }
    live_positions = {
        "data": {
            "rows": [
                {
                    "stockCode": "300373",
                    "stockName": "扬杰科技",
                    "marketValue": 5000,
                    "count": 100,
                    "availCount": 100,
                }
            ]
        }
    }
    cached_orders = {
        "data": {
            "rows": [
                {
                    "orderId": "cached-order-1",
                    "stockCode": "300373",
                    "stockName": "扬杰科技",
                    "orderStatus": 2,
                    "orderDrt": 1,
                }
            ]
        }
    }

    class StubClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def get_balance(self) -> dict[str, object]:
            return live_balance

        def get_positions(self) -> dict[str, object]:
            return live_positions

        def get_orders(self) -> dict[str, object]:
            raise RuntimeError("orders boom")

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        aniu_service_module.aniu_service,
        "get_or_create_settings",
        lambda db: type("StubSettings", (), {"mx_api_key": "demo-key"})(),
    )
    monkeypatch.setattr(
        aniu_service_module.aniu_service,
        "_get_recent_account_snapshot",
        lambda db: (None, None, cached_orders),
    )
    monkeypatch.setattr(aniu_service_module, "MXClient", StubClient)

    overview = aniu_service_module.aniu_service.get_account_overview(force_refresh=True)

    assert overview["orders"]
    assert overview["orders"][0]["order_id"] == "cached-order-1"
    assert overview["raw_orders"] == cached_orders
    assert any("缓存的委托数据" in message for message in overview["errors"])


def test_recent_account_snapshot_merges_balance_positions_and_orders_from_runs(
    monkeypatch, tmp_path
) -> None:
    _use_temp_db(monkeypatch, tmp_path)
    init_db()

    with session_scope() as db:
        oldest = StrategyRun(
            status="completed",
            skill_payloads={
                "tool_calls": [
                    {
                        "name": "mx_get_orders",
                        "result": {
                            "ok": True,
                            "result": {"data": {"rows": [{"orderId": "A-1"}]}},
                        },
                    }
                ]
            },
        )
        middle = StrategyRun(
            status="completed",
            skill_payloads={
                "tool_calls": [
                    {
                        "name": "mx_get_positions",
                        "result": {
                            "ok": True,
                            "result": {"data": {"rows": [{"stockCode": "300373"}]}},
                        },
                    }
                ]
            },
        )
        latest = StrategyRun(
            status="completed",
            skill_payloads={
                "tool_calls": [
                    {
                        "name": "mx_get_balance",
                        "result": {
                            "ok": True,
                            "result": {"data": {"totalAsset": 100000}},
                        },
                    }
                ]
            },
        )
        db.add_all([oldest, middle, latest])

    with session_scope() as db:
        balance, positions, orders = aniu_service._get_recent_account_snapshot(db)

    _reset_db_state()

    assert balance == {"data": {"totalAsset": 100000}}
    assert positions == {"data": {"rows": [{"stockCode": "300373"}]}}
    assert orders == {"data": {"rows": [{"orderId": "A-1"}]}}


def test_execute_run_prefetches_account_triplet_before_agent(monkeypatch, tmp_path) -> None:
    _use_temp_db(monkeypatch, tmp_path)
    init_db()

    from app.services import aniu_service as aniu_service_module

    prefetched_names: list[str] = []
    captured_task_prompt: dict[str, str] = {}

    monkeypatch.setattr(
        aniu_service_module.aniu_service,
        "get_or_create_settings",
        lambda db: type(
            "StubSettings",
            (),
            {
                "id": 1,
                "mx_api_key": "demo-key",
                "llm_base_url": "https://example.com/v1",
                "llm_api_key": "token",
                "llm_model": "demo-model",
                "system_prompt": "prompt",
                "timeout_seconds": 1800,
                "task_prompt": "请分析当前账户。",
            },
        )(),
    )

    class StubClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def close(self) -> None:
            pass

    def fake_execute_tool(*, client, app_settings, tool_name, arguments):
        del client, app_settings, arguments
        prefetched_names.append(tool_name)
        if tool_name == "mx_get_balance":
            return {
                "ok": True,
                "tool_name": tool_name,
                "result": {"data": {"totalAsset": 100000, "balanceActual": 95000}},
            }
        if tool_name == "mx_get_positions":
            return {
                "ok": True,
                "tool_name": tool_name,
                "result": {
                    "data": {
                        "rows": [
                            {
                                "stockCode": "300373",
                                "stockName": "扬杰科技",
                                "marketValue": 5000,
                                "count": 100,
                            }
                        ]
                    }
                },
            }
        if tool_name == "mx_get_orders":
            return {
                "ok": True,
                "tool_name": tool_name,
                "result": {
                    "data": {
                        "rows": [
                            {
                                "orderId": "1",
                                "stockCode": "300373",
                                "stockName": "扬杰科技",
                                "orderStatus": 2,
                                "orderDrt": 1,
                            }
                        ]
                    }
                },
            }
        raise AssertionError(f"unexpected tool {tool_name}")

    def fake_run_agent(settings, client):
        del client
        captured_task_prompt["value"] = settings.task_prompt
        return (
            {
                "final_answer": "保持观察。",
                "tool_calls": [],
            },
            {"messages": []},
            {"responses": []},
            {"messages": []},
        )

    monkeypatch.setattr(aniu_service_module, "MXClient", StubClient)
    monkeypatch.setattr(
        aniu_service_module.mx_skill_service,
        "execute_tool",
        fake_execute_tool,
    )
    monkeypatch.setattr(aniu_service_module.llm_service, "run_agent", fake_run_agent)

    run = aniu_service.execute_run(trigger_source="manual")

    _reset_db_state()

    assert prefetched_names == ["mx_get_balance", "mx_get_positions", "mx_get_orders"]
    assert "系统已在本轮开始前预取账户快照" in captured_task_prompt["value"]
    assert run.skill_payloads is not None
    assert run.skill_payloads.get("prefetched_tool_calls")
    assert run.skill_payloads.get("prefetched_context")


def test_daily_profit_trade_date_falls_back_to_previous_trading_day_on_weekend(
    monkeypatch,
) -> None:
    from app.services import aniu_service as aniu_service_module

    shanghai = ZoneInfo("Asia/Shanghai")
    original_now_shanghai = aniu_service_module.now_shanghai
    aniu_service_module.now_shanghai = lambda: datetime(
        2026, 4, 12, 12, 0, tzinfo=shanghai
    )
    try:
        overview = aniu_service._build_account_overview(None, {"data": {"rows": []}})
    finally:
        aniu_service_module.now_shanghai = original_now_shanghai

    assert overview["daily_profit_trade_date"] == "2026-04-10"
