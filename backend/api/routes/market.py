"""Public market overview routes."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from backend.api.deps import get_market_overview_service
from backend.api.schemas.error import error_responses
from backend.api.schemas.market import (
    MarketDetailsResponse,
    MarketIndicesResponse,
    MarketOverviewResponse,
)
from backend.api.security import require_authenticated
from backend.business.market import MarketOverviewQueryPort

router = APIRouter(
    prefix="/api/aniu/market",
    tags=["Market"],
    dependencies=[Depends(require_authenticated)],
    responses=error_responses(401, 403, 429, 502, 503),
)


@router.get("/overview/indices", response_model=MarketIndicesResponse)
async def get_market_indices(
    service: Annotated[MarketOverviewQueryPort, Depends(get_market_overview_service)],
) -> dict[str, object]:
    return await service.get_market_indices()


@router.get("/overview/details", response_model=MarketDetailsResponse)
async def get_market_details(
    service: Annotated[MarketOverviewQueryPort, Depends(get_market_overview_service)],
) -> dict[str, object]:
    return await service.get_market_details()


@router.get("/overview", response_model=MarketOverviewResponse)
async def get_market_overview(
    service: Annotated[MarketOverviewQueryPort, Depends(get_market_overview_service)],
) -> dict[str, object]:
    return await service.get_market_overview()
