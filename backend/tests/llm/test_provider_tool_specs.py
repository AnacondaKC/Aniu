from typing import cast

from backend.llm.contracts import LLMToolDefinition
from backend.llm.providers.extract import _claude_tools_spec, _openai_tool_spec


def _tool() -> LLMToolDefinition:
    return cast(
        LLMToolDefinition,
        {
            "name": "stock_example",
            "description": "test tool",
            "parameters": {
                "type": "object",
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "action": {"const": "stocks"},
                            "limit": {"type": "integer", "default": 20},
                        },
                        "required": ["action"],
                        "additionalProperties": False,
                    },
                    {
                        "type": "object",
                        "properties": {
                            "action": {"const": "sectors"},
                            "sector_type": {
                                "type": "string",
                                "default": "industry",
                            },
                        },
                        "required": ["action"],
                        "additionalProperties": False,
                    },
                ],
            },
        },
    )


def test_openai_tool_spec_preserves_action_schema_without_internal_controls() -> None:
    tool = _tool()
    spec = _openai_tool_spec(tool)
    assert spec["function"]["name"] == tool["name"]
    assert spec["function"]["description"] == tool["description"]
    assert spec["function"]["parameters"] == tool["parameters"]
    assert "source" not in spec["function"]["parameters"]
    assert "endpoint" not in spec["function"]["parameters"]


def test_claude_tools_spec_preserves_action_schema_without_internal_controls() -> None:
    tool = _tool()
    spec = _claude_tools_spec([tool])[0]
    assert spec["name"] == tool["name"]
    assert spec["description"] == tool["description"]
    assert spec["input_schema"] == tool["parameters"]
    assert "source" not in spec["input_schema"]
    assert "endpoint" not in spec["input_schema"]
