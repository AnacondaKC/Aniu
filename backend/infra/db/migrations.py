"""Self-contained SQLite schema migrations."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from sqlalchemy import Connection, text

TWO_STAGE_SCHEMA_VERSION = 3

# These values are a snapshot of the legacy format's defaults.  Migrations must
# remain stable when the current settings domain changes.
_LEGACY_PROFILE_DEFAULTS: dict[str, object] = {
    "schema": "aniu.prompt-profile.v3",
    "name": "默认提示词配置",
    "description": "",
    "global_prompt": (
        "你是顶尖机构股票投资专家，具有A股完整牛熊周期和实战经验，"
        "你的唯一目标是实现账户收益最大化。"
    ),
}
_LEGACY_STAGE_PROMPTS: dict[str, str] = {
    "Run": (
        "你负责操作股票模拟账户进行交易，必须先研究市场和账户，再进行交易并总结经验。"
    ),
    "Summary": "请用简体中文紧凑地总结本次运行报告，突出关键结论、风险和交易结果。",
}


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
    else:
        decoded = value
    return decoded if isinstance(decoded, dict) else {}


def _stage_prompt(source: Mapping[str, Any], stage_id: str) -> str:
    for key in ("prompt", f"{stage_id.lower()}_prompt"):
        value = source.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return _LEGACY_STAGE_PROMPTS[stage_id]


def _serialized_length(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, str):
        return len(value)
    return len(
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
    )


def _strip_tool_results(trace: dict[str, Any]) -> bool:
    changed = False
    stages = trace.get("stages")
    if not isinstance(stages, list):
        return changed
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        steps = stage.get("steps")
        if not isinstance(steps, list):
            continue
        for step in steps:
            if not isinstance(step, dict) or step.get("type") != "tool":
                continue
            data = step.get("data")
            if isinstance(data, dict) and "result" in data:
                model_content_characters = data.get("model_content_characters")
                if not (
                    type(model_content_characters) is int
                    and model_content_characters >= 0
                ):
                    data["model_content_characters"] = _serialized_length(
                        data["result"]
                    )
                data.pop("result")
                changed = True
    return changed


def _trace_metrics(trace: dict[str, Any]) -> tuple[int, int, int, int]:
    tool_calls_count = 0
    thinking_count = 0
    token_characters = 0
    trade_count = 0
    stages = trace.get("stages")
    if not isinstance(stages, list):
        return 0, 0, 0, 0
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        if stage.get("key") == "run" and stage.get("status") == "completed":
            steps = stage.get("steps")
            if isinstance(steps, list):
                for step in reversed(steps):
                    if not isinstance(step, dict) or step.get("type") != "result":
                        continue
                    data = step.get("data")
                    count = data.get("trade_count") if isinstance(data, dict) else None
                    if type(count) is int and count >= 0:
                        trade_count += count
                        break
        steps = stage.get("steps")
        if not isinstance(steps, list):
            continue
        for step in steps:
            if not isinstance(step, dict):
                continue
            step_type = step.get("type")
            data = step.get("data")
            payload = data if isinstance(data, dict) else {}
            content = step.get("content")
            if step_type == "tool":
                tool_calls_count += 1
                token_characters += _serialized_length(payload.get("arguments"))
                model_content_characters = payload.get("model_content_characters")
                if (
                    type(model_content_characters) is int
                    and model_content_characters >= 0
                ):
                    token_characters += model_content_characters
                else:
                    token_characters += _serialized_length(payload.get("result"))
                token_characters += _serialized_length(payload.get("error"))
            elif step_type == "thinking":
                thinking_count += 1
                token_characters += len(content) if isinstance(content, str) else 0
            elif step_type == "prompt":
                prompt_text = "".join(
                    (
                        _serialized_text(payload.get("system_message")),
                        _serialized_text(payload.get("user_message")),
                    )
                )
                token_characters += len(
                    prompt_text or (content if isinstance(content, str) else "")
                )
            elif step_type == "result":
                if isinstance(content, str) and content:
                    token_characters += len(content)
                else:
                    token_characters += _serialized_length(payload.get("summary"))
    estimated_tokens = (
        max(1, (token_characters + 3) // 4) if token_characters > 0 else 0
    )
    return tool_calls_count, thinking_count, estimated_tokens, trade_count


def _serialized_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _backfill_run_metrics(connection: Connection) -> None:
    columns = {
        str(row[1])
        for row in connection.execute(text("PRAGMA table_info(strategy_runs)"))
    }
    required = {
        "trace_json",
        "tool_calls_count",
        "thinking_count",
        "total_tokens",
        "trade_count",
    }
    if not required.issubset(columns):
        return
    rows = connection.execute(
        text("SELECT id, trace_json FROM strategy_runs")
    ).mappings()
    for row in rows:
        metrics = _trace_metrics(_json_object(row["trace_json"]))
        connection.execute(
            text(
                "UPDATE strategy_runs SET tool_calls_count = :tool_calls_count, "
                "thinking_count = :thinking_count, total_tokens = :total_tokens, "
                "trade_count = :trade_count WHERE id = :id"
            ),
            {
                "id": row["id"],
                "tool_calls_count": metrics[0],
                "thinking_count": metrics[1],
                "total_tokens": metrics[2],
                "trade_count": metrics[3],
            },
        )


def _upgrade_legacy_pipeline(connection: Connection) -> None:
    rows = connection.execute(
        text("SELECT id, prompt_profile_json, stage_settings_json FROM app_settings")
    ).mappings()
    for row in rows:
        profile = _json_object(row["prompt_profile_json"])
        stages = _json_object(row["stage_settings_json"])
        migrated_profile = dict(_LEGACY_PROFILE_DEFAULTS)
        migrated_profile["name"] = (
            profile.get("name") or _LEGACY_PROFILE_DEFAULTS["name"]
        )
        migrated_profile["description"] = profile.get("description") or ""
        migrated_profile["global_prompt"] = (
            profile.get("global_prompt") or _LEGACY_PROFILE_DEFAULTS["global_prompt"]
        )

        source_by_stage = {
            "Run": stages.get("Research") or stages.get("Run") or {},
            "Summary": stages.get("Summary") or {},
        }
        migrated_stages: dict[str, dict[str, object]] = {}
        for stage_id, raw_source in source_by_stage.items():
            source = raw_source if isinstance(raw_source, dict) else {}
            migrated_stages[stage_id] = {
                "stage_id": stage_id,
                "model_selected_model_id": source.get("model_selected_model_id"),
                "temperature": source.get("temperature", 0.0),
                "top_p": source.get("top_p", 1.0),
                "thinking_effort": source.get("thinking_effort"),
                "prompt": _stage_prompt(source, stage_id),
            }

        connection.execute(
            text(
                "UPDATE app_settings SET prompt_profile_json = :profile, "
                "stage_settings_json = :stages WHERE id = :id"
            ),
            {
                "id": row["id"],
                "profile": json.dumps(migrated_profile, ensure_ascii=False),
                "stages": json.dumps(migrated_stages, ensure_ascii=False),
            },
        )

    # Run history is intentionally disposable for this pre-release local schema.
    connection.execute(text("DELETE FROM tool_invocations"))
    connection.execute(text("DELETE FROM run_jobs"))
    connection.execute(text("DELETE FROM strategy_runs"))


def _remove_persisted_tool_results(connection: Connection) -> None:
    columns = {
        str(row[1])
        for row in connection.execute(text("PRAGMA table_info(strategy_runs)"))
    }
    if "trace_json" not in columns:
        return
    rows = connection.execute(
        text("SELECT id, trace_json FROM strategy_runs")
    ).mappings()
    for row in rows:
        trace = _json_object(row["trace_json"])
        if not _strip_tool_results(trace):
            continue
        connection.execute(
            text("UPDATE strategy_runs SET trace_json = :trace WHERE id = :id"),
            {
                "id": row["id"],
                "trace": json.dumps(trace, ensure_ascii=False, separators=(",", ":")),
            },
        )


def upgrade_two_stage_pipeline(connection: Connection) -> None:
    """Upgrade persisted local data without importing business code."""

    current_version = int(connection.execute(text("PRAGMA user_version")).scalar() or 0)
    if current_version >= TWO_STAGE_SCHEMA_VERSION:
        return
    if current_version < 1:
        _upgrade_legacy_pipeline(connection)
    if current_version < 2:
        _remove_persisted_tool_results(connection)
    if current_version < 3:
        _backfill_run_metrics(connection)
    connection.execute(text(f"PRAGMA user_version = {TWO_STAGE_SCHEMA_VERSION}"))


__all__ = ["TWO_STAGE_SCHEMA_VERSION", "upgrade_two_stage_pipeline"]
