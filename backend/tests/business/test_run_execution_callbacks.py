"""Trace-slimming rules for two-stage completion result steps."""

from backend.business.runs.callbacks import _slim_result_data


def test_slim_result_data_drops_activity_and_content_duplicates() -> None:
    payload = {
        "tool_activity": [{"tool_name": "query_market_data", "content": "巨大输出"}],
        "run_content": "完整 Markdown 运行报告正文",
        "content": "重复报告正文",
        "run_summary": "一句话摘要",
        "tool_calls_count": 9,
        "tool_failure_count": 0,
        "duration_ms": 1234,
        "trade_count": 0,
    }

    assert _slim_result_data(payload) == {
        "run_summary": "一句话摘要",
        "tool_calls_count": 9,
        "tool_failure_count": 0,
        "duration_ms": 1234,
        "trade_count": 0,
    }


def test_slim_result_data_can_drop_summary_for_summary_stage() -> None:
    payload = {"summary": "整份 HTML 总结", "duration_ms": 5}
    assert _slim_result_data(payload, drop_summary=True) == {"duration_ms": 5}
    assert _slim_result_data(payload) == payload
