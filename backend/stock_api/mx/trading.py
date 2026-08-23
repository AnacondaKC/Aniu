"""MX paper-trading write client."""

from __future__ import annotations

from dataclasses import dataclass, field

from backend.stock_api.mx.cache import MxReadCache
from backend.stock_api.mx.http import MxHttpTransport
from backend.stock_api.mx.trading_parser import parse_trade_instruction


@dataclass(slots=True)
class MxPaperTradingClient:
    transport: MxHttpTransport
    read_cache: MxReadCache = field(default_factory=MxReadCache)

    async def trade(self, instruction: str) -> dict[str, object]:
        intent = parse_trade_instruction(instruction)
        if intent.name != "trade":
            raise ValueError("trade only accepts buy or sell instructions")
        result = await self.transport.post_envelope(
            "/api/claw/mockTrading/trade",
            intent.payload,
        )
        self.read_cache.clear()
        return result

    async def cancel(self, instruction: str) -> dict[str, object]:
        intent = parse_trade_instruction(instruction)
        if intent.name == "cancel":
            payload = {"type": "order", **intent.payload}
        elif intent.name == "cancel_all":
            payload = {"type": "all"}
        else:
            raise ValueError("cancel only accepts cancel instructions")
        result = await self.transport.post_envelope(
            "/api/claw/mockTrading/cancel",
            payload,
        )
        self.read_cache.clear()
        return result


__all__ = ["MxPaperTradingClient"]
