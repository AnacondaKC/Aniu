"""Explicit registry for model protocol drivers."""

from __future__ import annotations

from collections.abc import Iterable

from backend.llm.contracts import ModelProtocol
from backend.llm.providers.anthropic_messages import ClaudeMessagesDriver
from backend.llm.providers.openai_chat import OpenAIChatDriver
from backend.llm.providers.types import ModelProtocolDriver


class ProviderRegistry:
    def __init__(self, drivers: Iterable[ModelProtocolDriver] = ()) -> None:
        self._drivers: dict[ModelProtocol, ModelProtocolDriver] = {}
        for driver in drivers:
            self.register(driver)

    def register(self, driver: ModelProtocolDriver) -> None:
        if driver.protocol in self._drivers:
            raise ValueError(f"model protocol already registered: {driver.protocol}")
        self._drivers[driver.protocol] = driver

    def require(self, protocol: ModelProtocol) -> ModelProtocolDriver:
        try:
            return self._drivers[protocol]
        except KeyError as exc:
            raise ValueError(f"unsupported model protocol: {protocol}") from exc

    def protocols(self) -> frozenset[ModelProtocol]:
        return frozenset(self._drivers)


def default_provider_registry() -> ProviderRegistry:
    return ProviderRegistry((OpenAIChatDriver(), ClaudeMessagesDriver()))


__all__ = ["ProviderRegistry", "default_provider_registry"]
