"""Load and validate versioned trading-calendar data files."""

from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Any

DATA_DIR = Path(__file__).resolve().parent / "data"
MANIFEST_NAME = "manifest.json"


class CalendarDataError(RuntimeError):
    """Raised when calendar data is missing or fails validation."""


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise CalendarDataError(f"calendar data file missing: {path}") from exc
    except json.JSONDecodeError as exc:
        raise CalendarDataError(
            f"calendar data file is not valid JSON: {path}"
        ) from exc


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _validate_year_payload(payload: dict[str, Any], *, expected_year: int) -> list[str]:
    if payload.get("exchange") != "XSHG":
        raise CalendarDataError(f"unexpected exchange in year {expected_year}")
    if payload.get("timezone") != "Asia/Shanghai":
        raise CalendarDataError(f"unexpected timezone in year {expected_year}")
    if payload.get("year") != expected_year:
        raise CalendarDataError(
            f"year field {payload.get('year')!r} does not match "
            f"file year {expected_year}"
        )
    days = payload.get("trading_days")
    if not isinstance(days, list) or not days:
        raise CalendarDataError(f"year {expected_year} has no trading_days")
    if any(not isinstance(item, str) for item in days):
        raise CalendarDataError(f"year {expected_year} contains non-string dates")
    if len(set(days)) != len(days):
        raise CalendarDataError(f"year {expected_year} contains duplicate dates")
    if days != sorted(days):
        raise CalendarDataError(f"year {expected_year} trading_days must be sorted")
    for item in days:
        if len(item) != 10 or item[4] != "-" or item[7] != "-":
            raise CalendarDataError(f"invalid date format: {item}")
        if not item.startswith(f"{expected_year}-"):
            raise CalendarDataError(
                f"date {item} does not belong to year {expected_year}"
            )
    return list(days)


@lru_cache(maxsize=1)
def load_trading_days_by_year() -> dict[int, frozenset[str]]:
    """Load all years declared by the calendar manifest.

    Validation is fail-closed: any missing/corrupt file raises CalendarDataError.
    """

    manifest_path = DATA_DIR / MANIFEST_NAME
    manifest = _read_json(manifest_path)
    if not isinstance(manifest, dict):
        raise CalendarDataError("calendar manifest must be an object")

    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise CalendarDataError("calendar manifest has no files")

    by_year: dict[int, frozenset[str]] = {}
    for entry in files:
        if not isinstance(entry, dict):
            raise CalendarDataError("calendar manifest file entry must be an object")
        year = entry.get("year")
        filename = entry.get("file")
        expected_hash = entry.get("sha256")
        if not isinstance(year, int) or not isinstance(filename, str):
            raise CalendarDataError("calendar manifest entry missing year/file")
        path = DATA_DIR / filename
        raw_text = path.read_text(encoding="utf-8")
        if isinstance(expected_hash, str) and expected_hash:
            actual = _sha256_text(raw_text)
            if actual != expected_hash:
                raise CalendarDataError(
                    f"hash mismatch for {filename}: "
                    f"expected {expected_hash}, got {actual}"
                )
        payload = json.loads(raw_text)
        if not isinstance(payload, dict):
            raise CalendarDataError(f"{filename} root must be an object")
        days = _validate_year_payload(payload, expected_year=year)
        by_year[year] = frozenset(days)

    covered = manifest.get("covered_years")
    if isinstance(covered, list):
        missing = [year for year in covered if year not in by_year]
        if missing:
            raise CalendarDataError(f"manifest covered years missing data: {missing}")

    return by_year


__all__ = ["CalendarDataError", "DATA_DIR", "load_trading_days_by_year"]
