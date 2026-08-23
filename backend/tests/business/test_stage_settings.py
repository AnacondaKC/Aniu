"""Tests for fixed two-stage model and prompt settings."""

import pytest

from backend.business.runs import StrategySnapshot
from backend.business.settings import (
    STAGE_IDS,
    AniuAgentPrompt,
    AppSettings,
    StageSettings,
)


def test_stage_settings_normalize_optional_thinking_effort() -> None:
    settings = StageSettings.from_mapping(
        {
            "stage_id": "Run",
            "model_selected_model_id": 11,
            "temperature": 0.2,
            "top_p": 0.8,
            "thinking_effort": " HIGH ",
            "prompt": "执行提示词",
        }
    )

    assert settings.thinking_effort == "high"
    assert settings.as_dict()["thinking_effort"] == "high"


def test_stage_settings_contract_has_no_render_or_tool_loop_switches() -> None:
    settings = StageSettings(
        stage_id="Summary",
        model_selected_model_id=11,
        temperature=0.2,
        top_p=0.8,
        prompt="总结提示词",
    )

    assert settings.model_selected_model_id == 11
    assert set(settings.as_dict()) == {
        "stage_id",
        "model_selected_model_id",
        "temperature",
        "top_p",
        "thinking_effort",
        "prompt",
    }
    assert not hasattr(settings, "render_mode")
    assert not hasattr(settings, "html_prompt")
    assert not hasattr(settings, "max_tool_rounds")


def test_configured_stage_ids_include_dream() -> None:
    assert STAGE_IDS == ("Run", "Summary", "Dream")


def test_old_settings_inherit_run_model_for_missing_dream_stage() -> None:
    settings = AppSettings(
        stage_settings={
            "Run": StageSettings("Run", 11, 0.0, 1.0, "运行提示词"),
            "Summary": StageSettings("Summary", 11, 0.0, 1.0, "总结提示词"),
        }
    )

    assert settings.stage_settings["Dream"].model_selected_model_id == 11
    assert settings.stage_settings["Dream"].prompt

    with pytest.raises(ValueError, match="unknown stage_id"):
        StageSettings(
            stage_id="Research",
            model_selected_model_id=1,
            temperature=0,
            top_p=1,
            prompt="legacy",
        )


def test_snapshot_composes_global_and_stage_prompt_at_runtime() -> None:
    snapshot = StrategySnapshot(
        prompt_version="v3",
        risk_rules_version="v1",
        prompt_profile=AniuAgentPrompt(global_prompt="全局约束"),
        stage_settings={
            "Run": StageSettings(
                stage_id="Run",
                model_selected_model_id=1,
                temperature=0,
                top_p=1,
                prompt="执行提示词",
            )
        },
    )

    assert tuple(snapshot.stage_settings) == STAGE_IDS
    assert snapshot.stage_settings["Run"].prompt == "执行提示词"
    assert snapshot.settings_for_stage("Run").prompt == "全局约束\n\n执行提示词"


def test_stage_settings_accepts_zero_top_p() -> None:
    settings = StageSettings(
        stage_id="Run",
        model_selected_model_id=1,
        temperature=0,
        top_p=0,
        prompt="分析输入",
    )

    assert settings.top_p == 0


def test_mapping_ignores_non_domain_storage_fields() -> None:
    settings = StageSettings.from_mapping(
        {
            "stage_id": "Summary",
            "model_selected_model_id": 1,
            "temperature": 0,
            "top_p": 1,
            "prompt": "生成完整总结",
            "max_output_tokens": 1_000_000,
        }
    )

    assert "max_output_tokens" not in settings.as_dict()
