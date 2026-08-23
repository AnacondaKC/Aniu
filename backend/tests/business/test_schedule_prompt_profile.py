"""Tests for two-stage prompt profile defaults."""

from __future__ import annotations

from backend.business.settings import (
    PROMPT_PROFILE_PROMPT_FIELDS,
    PROMPT_PROFILE_SCHEMA,
    AniuAgentPrompt,
)


def test_prompt_profile_has_run_summary_and_dream_prompts() -> None:
    profile = AniuAgentPrompt()
    payload = profile.as_dict()
    prompt_fields = {"global_prompt", "run_prompt", "summary_prompt", "dream_prompt"}

    assert PROMPT_PROFILE_SCHEMA == "aniu.prompt-profile.v3"
    assert PROMPT_PROFILE_PROMPT_FIELDS == prompt_fields
    for field_name in prompt_fields:
        assert profile.prompt_text(field_name)

    assert set(payload) == {"schema", "name", "description", *prompt_fields}
    assert "research_prompt" not in payload
    assert "decision_prompt" not in payload
    assert "trade_prompt" not in payload


def test_default_prompts_define_institutional_trading_and_visual_rules() -> None:
    profile = AniuAgentPrompt()

    assert "顶尖机构股票投资专家" in profile.global_prompt
    assert "账户收益最大化" in profile.global_prompt
    assert "市场环境判断" in profile.run_prompt
    assert "当前账户的持仓情况" in profile.run_prompt
    assert "投资经验和投资哲学" in profile.run_prompt
    assert profile.dream_prompt == (
        "整理指定日期的运行报告和投资经验，决定保留、合并、更新或删除经验。"
        "先阅读报告和记忆，再进行经验操作。要从投资哲学和投资理念的角度出发去进行整理，"
        "不要保存短期行情、单次噪声或重复内容。更新或删除经验时必须使用最近读取结果"
        "中的 id 和 version。删除只能用于重复经验，不要删除仍然有独立价值或"
        "彼此冲突的经验，最后总结本次梦境做了什么。"
    )
    assert "标题从 ## 起" in profile.summary_prompt
    assert "纯内联样式" in profile.summary_prompt
    assert "html-visual" in profile.summary_prompt
    assert "黑白灰等克制色" in profile.summary_prompt
