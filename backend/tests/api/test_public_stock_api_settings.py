"""API coverage for the unified data-source settings surface."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from backend.infra.db.models import StockApiCallLogModel
from backend.infra.repositories import StockApiCallLogRecord, StockApiCallLogRepository


@pytest.mark.asyncio
async def test_stock_api_settings_expose_catalog_and_tool_invocation_logs(
    api_client: AsyncClient,
    session_factory,
) -> None:
    async with session_factory() as session:
        repository = StockApiCallLogRepository(session)
        await repository.append(
            StockApiCallLogRecord(
                tool_source="mx",
                tool_id="data",
                parameters={"query": "贵州茅台"},
                status="success",
                duration_ms=20,
                response_characters=321,
            )
        )
        await repository.append(
            StockApiCallLogRecord(
                tool_source="public",
                tool_id="stock_quote",
                parameters={"symbols": ["600519.SH"]},
                status="failed",
                duration_ms=24,
                error_message="公开数据暂时不可用。",
            )
        )
        await repository.append(
            StockApiCallLogRecord(
                tool_source="aggregate",
                tool_id="market_snapshot",
                parameters={},
                status="success",
                duration_ms=36,
                response_characters=13_259,
            )
        )
        # Legacy rows represent upstream requests and must not leak into the
        # user-facing tool invocation log after the logging-granularity change.
        session.add(
            StockApiCallLogModel(
                source="run",
                provider="eastmoney",
                operation_id="quote.snapshot",
                parameters_json='{"secid":"1.600519","url":"https://private.example.com"}',
                status="success",
                duration_ms=8,
                response_characters=128,
            )
        )
        session.add(
            StockApiCallLogModel(
                source="run",
                provider="mx",
                operation_id="query_market_data",
                parameters_json='{"query":"历史上游调用"}',
                status="success",
                duration_ms=8,
                response_characters=16,
            )
        )
        await session.commit()

    settings_response = await api_client.get("/api/aniu/settings/stock-api")
    assert settings_response.status_code == 200
    catalog = settings_response.json()["public_stock"]
    assert catalog["name"] == "公开数据"
    assert catalog["providers"] == ["tencent", "sina", "eastmoney"]
    assert [item["tool_name"] for item in catalog["tools"]] == [
        "stock_quote",
        "query_kline",
        "stock_intraday",
        "stock_ranking",
        "stock_money_flow",
        "stock_fundamentals",
        "stock_research",
        "stock_news",
        "market_snapshot",
        "portfolio_stock_snapshot",
        "stock_analysis",
        "industry_snapshot",
    ]
    assert catalog["tools"][0]["providers"] == ["tencent", "sina", "eastmoney"]
    assert catalog["tools"][1]["providers"] == [
        "tencent",
        "sina",
        "eastmoney",
    ]
    assert catalog["tools"][2]["providers"] == ["tencent", "eastmoney"]
    assert catalog["tools"][-1]["providers"] == ["eastmoney"]

    response = await api_client.get("/api/aniu/settings/stock-api/logs")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert {
        (item["tool_source"], item["tool_id"], item["tool_name"])
        for item in body["items"]
    } == {
        ("aggregate", "market_snapshot", "行情查询"),
        ("mx", "data", "金融数据查询"),
        ("public", "stock_quote", "实时行情"),
    }
    assert all("provider" not in item for item in body["items"])
    response_characters = {
        item["tool_id"]: item["response_characters"] for item in body["items"]
    }
    assert response_characters == {
        "data": 321,
        "stock_quote": None,
        "market_snapshot": 13_259,
    }
    assert "private.example.com" not in response.text
    assert "1.600519" not in response.text
    assert "历史上游调用" not in response.text

    paged = await api_client.get(
        "/api/aniu/settings/stock-api/logs",
        params={"limit": 1, "offset": 1},
    )
    assert paged.status_code == 200
    assert paged.json()["total"] == 3
    assert [item["tool_id"] for item in paged.json()["items"]] == ["stock_quote"]

    filtered = await api_client.get(
        "/api/aniu/settings/stock-api/logs",
        params={"tool_source": "public"},
    )
    assert filtered.status_code == 200
    assert filtered.json()["total"] == 1
    assert filtered.json()["items"][0]["tool_id"] == "stock_quote"

    aggregate_filtered = await api_client.get(
        "/api/aniu/settings/stock-api/logs",
        params={"tool_source": "aggregate"},
    )
    assert aggregate_filtered.status_code == 200
    assert aggregate_filtered.json()["total"] == 1
    assert aggregate_filtered.json()["items"][0]["tool_id"] == "market_snapshot"

    invalid = await api_client.get(
        "/api/aniu/settings/stock-api/logs",
        params={"tool_source": "unknown"},
    )
    assert invalid.status_code == 422
