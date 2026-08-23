"""Projection of registered tools to provider-facing definitions."""

from __future__ import annotations

from backend.llm import ToolDefinition


def list_tool_definitions(registry: object | None) -> list[ToolDefinition]:
    if registry is None:
        return []
    list_tools = getattr(registry, "list_tools", None)
    if not callable(list_tools):
        return []
    definitions: list[ToolDefinition] = []
    for tool in list_tools():
        to_definition = getattr(tool, "to_tool_definition", None)
        if not callable(to_definition):
            continue
        definition = to_definition()
        if not isinstance(definition, dict):
            continue
        name = definition.get("name")
        description = definition.get("description")
        parameters = definition.get("parameters")
        if not isinstance(name, str) or not name.strip():
            continue
        if not isinstance(description, str) or not isinstance(parameters, dict):
            continue
        definitions.append(
            ToolDefinition(
                name=name,
                description=description,
                parameters=parameters,
            )
        )
    return definitions


__all__ = ["list_tool_definitions"]
