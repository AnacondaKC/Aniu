"""Protocol driver tests for production text probes and registry behavior."""

from __future__ import annotations

import pytest

from backend.llm import LLMErrorCode, LLMIntegrationError, ModelProtocol
from backend.llm.registry import default_provider_registry

get_protocol_driver = default_provider_registry().require


def test_openai_text_probe_builds_and_parses() -> None:
    driver = get_protocol_driver(ModelProtocol.OPENAI_CHAT_COMPLETIONS)
    request = driver.build_text_request(
        api_key="k",
        model="gpt-test",
        system_prompt="sys",
        user_prompt="hello",
        temperature=0.1,
        top_p=1.0,
        max_output_tokens=65_536,
    )

    assert request["url"] == "/chat/completions"
    assert request["json"]["model"] == "gpt-test"
    assert request["json"]["max_tokens"] == 65_536
    assert (
        driver.parse_text_response(
            {"choices": [{"finish_reason": "stop", "message": {"content": "world"}}]}
        )
        == "world"
    )


def test_claude_text_probe_builds_and_parses() -> None:
    driver = get_protocol_driver(ModelProtocol.CLAUDE_API)
    request = driver.build_text_request(
        api_key="k",
        model="claude-test",
        system_prompt="sys",
        user_prompt="hello",
        temperature=0.3,
        top_p=0.8,
        max_output_tokens=65_536,
    )

    assert request["url"] == "/messages"
    assert request["json"]["system"] == "sys"
    assert request["json"]["top_p"] == 0.8
    assert "temperature" not in request["json"]
    assert (
        driver.parse_text_response(
            {
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "world"}],
            }
        )
        == "world"
    )


def test_claude_text_probe_preserves_context_overflow() -> None:
    driver = get_protocol_driver(ModelProtocol.CLAUDE_API)

    with pytest.raises(LLMIntegrationError) as exc_info:
        driver.parse_text_response(
            {
                "stop_reason": "model_context_window_exceeded",
                "content": [],
            }
        )

    assert exc_info.value.error_code is LLMErrorCode.CONTEXT_OVERFLOW


@pytest.mark.parametrize(
    ("protocol", "payload", "error"),
    [
        (
            ModelProtocol.OPENAI_CHAT_COMPLETIONS,
            {"choices": [{"message": {"content": "partial"}}]},
            "finish_reason",
        ),
        (
            ModelProtocol.CLAUDE_API,
            {"content": [{"type": "text", "text": "partial"}]},
            "stop_reason",
        ),
    ],
)
def test_text_probe_requires_terminal_reason(
    protocol: ModelProtocol,
    payload: dict[str, object],
    error: str,
) -> None:
    driver = get_protocol_driver(protocol)
    with pytest.raises(LLMIntegrationError, match=error):
        driver.parse_text_response(payload)


def test_unknown_protocol_raises() -> None:
    with pytest.raises(ValueError, match="unsupported model protocol"):
        get_protocol_driver("not-a-protocol")  # type: ignore[arg-type]
