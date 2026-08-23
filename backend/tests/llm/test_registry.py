"""Provider registry coverage and ownership tests."""

from __future__ import annotations

import subprocess
import sys

import pytest

from backend.llm import ModelProtocol, ProviderRegistry, default_provider_registry
from backend.llm.providers.openai_chat import OpenAIChatDriver


def test_default_registry_covers_every_supported_protocol() -> None:
    registry = default_provider_registry()

    assert registry.protocols() == frozenset(ModelProtocol)


def test_default_registry_does_not_eagerly_import_provider_sdks() -> None:
    script = """
import sys
from backend.llm.registry import default_provider_registry

default_provider_registry()
assert "openai" not in sys.modules
assert "anthropic" not in sys.modules
"""

    subprocess.run([sys.executable, "-c", script], check=True)


def test_registry_rejects_duplicate_protocol_driver() -> None:
    registry = ProviderRegistry((OpenAIChatDriver(),))

    with pytest.raises(ValueError, match="already registered"):
        registry.register(OpenAIChatDriver())


def test_registry_rejects_unregistered_protocol() -> None:
    registry = ProviderRegistry()

    with pytest.raises(ValueError, match="unsupported model protocol"):
        registry.require(ModelProtocol.CLAUDE_API)
