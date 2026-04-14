from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import httpx

_MAX_PROBE_DAYS = 30
_CALENDAR_FETCH_RETRIES = 3
_CALENDAR_SOURCE = "codebuddy_trade_cal"
_CALENDAR_API_URL = "https://www.codebuddy.cn/v2/tool/financedata"
_CALENDAR_EXCHANGE = "SSE"


class TradingCalendarService:
    def __init__(self) -> None:
        self._data_path = (
            Path(__file__).resolve().parents[1] / "data" / "trading_calendar.json"
        )
        self._calendar: dict[str, object] | None = None
        self._year_days_cache: dict[int, set[str]] = {}

    def _load_calendar(self) -> dict[str, object]:
        if self._calendar is None:
            if self._data_path.exists():
                payload = json.loads(self._data_path.read_text(encoding="utf-8"))
                if "years" not in payload:
                    trading_days = payload.get("trading_days", [])
                    by_year: dict[str, list[str]] = {}
                    for item in trading_days:
                        key = str(item)[:4]
                        by_year.setdefault(key, []).append(str(item))
                    payload = {
                        "version": 1,
                        "source": _CALENDAR_SOURCE,
                        "years": {
                            year: {"trading_days": days}
                            for year, days in by_year.items()
                        },
                    }
                self._calendar = payload
            else:
                self._calendar = {
                    "version": 1,
                    "source": _CALENDAR_SOURCE,
                    "years": {},
                }
        return self._calendar

    def _save_calendar(self) -> None:
        self._data_path.parent.mkdir(parents=True, exist_ok=True)
        self._data_path.write_text(
            json.dumps(self._calendar, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _fetch_year(self, year: int) -> list[str]:
        last_error: RuntimeError | None = None
        for attempt in range(_CALENDAR_FETCH_RETRIES + 1):
            try:
                return self._fetch_year_once(year)
            except RuntimeError as exc:
                last_error = exc
                if attempt == _CALENDAR_FETCH_RETRIES:
                    raise RuntimeError(
                        f"{exc}；已重试 {_CALENDAR_FETCH_RETRIES} 次仍失败"
                    ) from exc

        raise RuntimeError(
            f"missing trading calendar data for year {year}: {last_error}"
        )

    def _fetch_year_once(self, year: int) -> list[str]:
        payload = {
            "api_name": "trade_cal",
            "params": {
                "exchange": _CALENDAR_EXCHANGE,
                "start_date": f"{year}0101",
                "end_date": f"{year}1231",
            },
            "fields": "",
        }

        try:
            response = httpx.post(
                _CALENDAR_API_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30.0,
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise RuntimeError(
                f"交易日历远程接口返回错误 ({exc.response.status_code})"
            ) from exc
        except httpx.TimeoutException as exc:
            raise RuntimeError("交易日历远程接口请求超时") from exc
        except httpx.HTTPError as exc:
            raise RuntimeError(f"交易日历远程接口请求失败: {exc}") from exc

        try:
            result = response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError("交易日历远程接口返回了无效 JSON") from exc

        if not isinstance(result, dict):
            raise RuntimeError("交易日历远程接口返回结构异常")

        code = result.get("code")
        if code != 0:
            raise RuntimeError(
                f"交易日历远程接口返回失败: {result.get('msg') or code}"
            )

        data = result.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("交易日历远程接口缺少 data 字段")

        items = data.get("items")
        if not isinstance(items, list):
            raise RuntimeError("交易日历远程接口缺少 items 字段")

        fields = data.get("fields")
        rows = self._normalize_rows(fields, items)
        trading_days = [
            self._normalize_calendar_date(str(row["cal_date"]))
            for row in rows
            if self._is_open_value(row.get("is_open"))
        ]
        unique_days = sorted(set(trading_days))
        if not unique_days:
            raise RuntimeError(f"missing trading calendar data for year {year}")
        return unique_days

    def _normalize_rows(
        self, fields: Any, items: list[Any]
    ) -> list[dict[str, Any]]:
        if not isinstance(fields, list) or not all(
            isinstance(field, str) for field in fields
        ):
            raise RuntimeError("交易日历远程接口 fields 字段异常")

        rows: list[dict[str, Any]] = []
        for item in items:
            if isinstance(item, dict):
                rows.append(item)
                continue
            if isinstance(item, list) and len(item) == len(fields):
                rows.append(dict(zip(fields, item)))
                continue
            raise RuntimeError("交易日历远程接口 items 字段异常")
        return rows

    def _normalize_calendar_date(self, value: str) -> str:
        text = value.strip()
        if len(text) == 8 and text.isdigit():
            return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
        if len(text) == 10 and text[4] == "-" and text[7] == "-":
            return text
        raise RuntimeError(f"交易日历远程接口返回了非法日期: {value}")

    def _is_open_value(self, value: Any) -> bool:
        return str(value).strip() == "1"

    def ensure_years(self, years: list[int]) -> None:
        calendar = self._load_calendar()
        years_data = calendar.setdefault("years", {})
        if not isinstance(years_data, dict):
            raise RuntimeError("交易日历缓存结构异常")
        changed = False
        for year in years:
            key = str(year)
            if key in years_data:
                continue
            queried_days = self._fetch_year(year)
            if not queried_days:
                raise RuntimeError(f"missing trading calendar data for year {year}")
            years_data[key] = {"trading_days": queried_days}
            changed = True
        if changed:
            calendar["source"] = _CALENDAR_SOURCE
            self._save_calendar()
            self._year_days_cache.clear()

    def warm_up_years(self, current_year: int) -> None:
        self.ensure_years([current_year])
        try:
            self.ensure_years([current_year + 1])
        except Exception:
            # Missing next-year data should not block current-year scheduling.
            pass

    def _year_days(self, year: int) -> set[str]:
        if year in self._year_days_cache:
            return self._year_days_cache[year]
        self.ensure_years([year])
        calendar = self._load_calendar()
        years_data = calendar.get("years", {})
        year_payload = (
            years_data.get(str(year), {}) if isinstance(years_data, dict) else {}
        )
        if not isinstance(year_payload, dict):
            result: set[str] = set()
        else:
            trading_days = year_payload.get("trading_days", [])
            result = {str(item) for item in trading_days}
        self._year_days_cache[year] = result
        return result

    def is_trading_day(self, current: date) -> bool:
        return current.isoformat() in self._year_days(current.year)

    def next_trading_day(self, current: date) -> date:
        probe = current
        for _ in range(_MAX_PROBE_DAYS):
            if self.is_trading_day(probe):
                return probe
            probe += timedelta(days=1)
        raise RuntimeError(
            f"在 {current} 之后 {_MAX_PROBE_DAYS} 天内未找到交易日，请检查交易日历数据。"
        )


trading_calendar_service = TradingCalendarService()
