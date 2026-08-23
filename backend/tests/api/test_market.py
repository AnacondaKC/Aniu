"""API tests for the public market overview endpoint."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from backend.api.deps import get_market_overview_service
from backend.main import app


class FakeMarketOverviewService:
    async def get_market_indices(self) -> dict[str, object]:
        return {
            "generated_at": "2026-07-31T08:00:00Z",
            "indices": [],
            "trends": [],
            "errors": [
                {"resource": "index", "item_id": None, "message": "指数源暂时不可用"}
            ],
        }

    async def get_market_details(self) -> dict[str, object]:
        return {
            "generated_at": "2026-07-31T08:00:00Z",
            "turnover": {"today_amount": None},
            "breadth": None,
            "rankings": {
                "gainers": [],
                "losers": [],
                "net_inflow": [],
                "net_outflow": [],
            },
            "hotspots": {"industry": [], "concept": []},
            "headlines": [],
            "flash_news": [],
            "errors": [
                {
                    "resource": "details",
                    "item_id": None,
                    "message": "其余行情源暂时不可用",
                }
            ],
        }

    async def get_market_overview(self) -> dict[str, object]:
        return {
            "generated_at": "2026-07-31T08:00:00Z",
            "indices": [],
            "trends": [],
            "turnover": {"today_amount": None},
            "breadth": None,
            "rankings": {
                "gainers": [],
                "losers": [],
                "net_inflow": [],
                "net_outflow": [],
            },
            "hotspots": {"industry": [], "concept": []},
            "headlines": [],
            "flash_news": [],
            "errors": [
                {"resource": "index", "item_id": None, "message": "行情源暂时不可用"}
            ],
        }


@pytest.mark.asyncio
async def test_market_overview_endpoint_returns_partial_snapshot(
    api_client: AsyncClient,
) -> None:
    app.dependency_overrides[get_market_overview_service] = FakeMarketOverviewService

    response = await api_client.get("/api/aniu/market/overview")

    assert response.status_code == 200
    body = response.json()
    assert body["breadth"] is None
    assert body["turnover"]["today_amount"] is None
    assert body["errors"] == [
        {"resource": "index", "item_id": None, "message": "行情源暂时不可用"}
    ]


@pytest.mark.asyncio
async def test_split_market_endpoints_return_independent_payloads(
    api_client: AsyncClient,
) -> None:
    app.dependency_overrides[get_market_overview_service] = FakeMarketOverviewService

    indices_response = await api_client.get("/api/aniu/market/overview/indices")
    details_response = await api_client.get("/api/aniu/market/overview/details")

    assert indices_response.status_code == 200
    assert indices_response.json()["errors"] == [
        {"resource": "index", "item_id": None, "message": "指数源暂时不可用"}
    ]
    assert details_response.status_code == 200
    assert details_response.json()["errors"] == [
        {"resource": "details", "item_id": None, "message": "其余行情源暂时不可用"}
    ]
