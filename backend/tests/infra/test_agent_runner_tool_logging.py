"""Tool-level logging behavior around upstream StockApi request details."""

from __future__ import annotations

import pytest

from backend.agent.tools import ToolRegistry
from backend.business.stock_api_logs.models import StockApiToolCall
from backend.infra.integrations.agent_runner import _StageToolRegistry
from backend.infra.integrations.tool_policy import SideEffectLevel
from backend.stock_api.models import StockApiCall, emit_stock_api_call_log
from backend.stock_api.public import UnsupportedStockRequest


class StockQuoteTool:
    name = "stock_quote"
    enabled_stages = ("Run",)
    side_effect_level = SideEffectLevel.READ
    execution_mode = "parallel"

    async def run(self, symbols: list[str]) -> object:
        for provider, status in (("tencent", "failed"), ("sina", "success")):
            await emit_stock_api_call_log(
                None,
                StockApiCall(
                    provider=provider,
                    operation_id="quote.snapshot",
                    endpoint="internal",
                    method="GET",
                    parameters={},
                    status=status,
                    status_code=None,
                    duration_ms=1,
                ),
            )
        return {"status": "ok", "symbols": symbols}


class QueryKlineLogTool:
    name = "query_kline"
    enabled_stages = ("Run",)
    side_effect_level = SideEffectLevel.READ
    execution_mode = "parallel"

    def stock_api_log_parameters(
        self, parameters: dict[str, object]
    ) -> dict[str, object]:
        return {
            **parameters,
            "instrument_type": "index",
            "resolved_instrument": "000001.SH",
            "data_source": "public",
        }

    async def run(self, instrument: str) -> object:
        return {"status": "ok", "instrument": instrument}


class UnsupportedKlineTool(QueryKlineLogTool):
    async def run(self, instrument: str) -> object:
        del instrument
        raise UnsupportedStockRequest("K 线不支持行业板块。")


@pytest.mark.asyncio
async def test_tool_call_log_is_one_final_record_despite_upstream_fallbacks() -> None:
    source = ToolRegistry()
    source.register(StockQuoteTool())
    persisted: list[StockApiToolCall] = []

    async def record(call: StockApiToolCall) -> None:
        persisted.append(call)

    registry = _StageToolRegistry(
        source,
        "Run",
        run_id=20260816001,
        invocation_session_factory=None,
        stock_api_tool_call_logger=record,
    )

    result = await registry.call_idempotently(
        "stock_quote",
        tool_call_id="quote-1",
        abort_signal=None,
        symbols=["600519.SH"],
    )

    assert result == {"status": "ok", "symbols": ["600519.SH"]}
    upstream_providers = [
        item["provider"] for item in registry.stock_api_call_details("quote-1")
    ]
    assert upstream_providers == [
        "tencent",
        "sina",
    ]
    assert len(persisted) == 1
    assert persisted[0].tool_source == "public"
    assert persisted[0].tool_id == "stock_quote"
    assert persisted[0].parameters == {"symbols": ["600519.SH"]}
    assert persisted[0].status == "success"
    assert persisted[0].response_characters == len(
        '{"status":"ok","symbols":["600519.SH"]}'
    )


@pytest.mark.asyncio
async def test_tool_call_log_uses_tool_enriched_routing_parameters() -> None:
    source = ToolRegistry()
    source.register(QueryKlineLogTool())
    persisted: list[StockApiToolCall] = []

    async def record(call: StockApiToolCall) -> None:
        persisted.append(call)

    registry = _StageToolRegistry(
        source,
        "Run",
        run_id=20260816001,
        invocation_session_factory=None,
        stock_api_tool_call_logger=record,
    )

    await registry.call_idempotently(
        "query_kline",
        tool_call_id="kline-1",
        abort_signal=None,
        instrument="000001.SH",
    )

    assert len(persisted) == 1
    assert persisted[0].tool_id == "query_kline"
    assert persisted[0].parameters == {
        "instrument": "000001.SH",
        "instrument_type": "index",
        "resolved_instrument": "000001.SH",
        "data_source": "public",
    }


@pytest.mark.asyncio
async def test_tool_call_log_classifies_domain_rejection_as_business_failure() -> None:
    source = ToolRegistry()
    source.register(UnsupportedKlineTool())
    persisted: list[StockApiToolCall] = []

    async def record(call: StockApiToolCall) -> None:
        persisted.append(call)

    registry = _StageToolRegistry(
        source,
        "Run",
        run_id=20260816001,
        invocation_session_factory=None,
        stock_api_tool_call_logger=record,
    )

    with pytest.raises(UnsupportedStockRequest, match="不支持行业板块"):
        await registry.call_idempotently(
            "query_kline",
            tool_call_id="kline-unsupported",
            abort_signal=None,
            instrument="000001.SH",
        )

    assert len(persisted) == 1
    assert persisted[0].status == "failed"
    assert persisted[0].response_characters is None
    assert persisted[0].error_category == "business_failure"
