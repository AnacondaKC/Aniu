"""Unicode and JSON sanitization for provider request boundaries."""

from __future__ import annotations

from math import isfinite

from backend.llm.errors import LLMConfigurationError

type SanitizedJsonValue = (
    None
    | bool
    | int
    | float
    | str
    | list[SanitizedJsonValue]
    | dict[str, SanitizedJsonValue]
)


def sanitize_unicode(value: str) -> str:
    """Replace only unpaired UTF-16 surrogates, preserving valid characters."""

    return value.encode("utf-16", "surrogatepass").decode("utf-16", "replace")


def sanitize_json(value: object) -> SanitizedJsonValue:
    if isinstance(value, float) and not isfinite(value):
        raise LLMConfigurationError("provider JSON numbers must be finite")
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return sanitize_unicode(value)
    if isinstance(value, list):
        return [sanitize_json(item) for item in value]
    if isinstance(value, dict):
        sanitized: dict[str, SanitizedJsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise LLMConfigurationError("provider JSON object keys must be text")
            sanitized[sanitize_unicode(key)] = sanitize_json(item)
        return sanitized
    raise LLMConfigurationError(
        f"provider JSON value has unsupported type: {type(value).__name__}"
    )


__all__ = ["sanitize_json", "sanitize_unicode"]
