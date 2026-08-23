"""Market overview API schemas."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from backend.api.schemas.common import ApiModel


class MarketResourceErrorResponse(ApiModel):
    resource: str
    item_id: str | None
    message: str


class MarketIndicesResponse(ApiModel):
    generated_at: datetime
    indices: list[dict[str, Any]]
    trends: list[dict[str, Any]]
    errors: list[MarketResourceErrorResponse]


class MarketDetailsResponse(ApiModel):
    generated_at: datetime
    turnover: dict[str, Any]
    breadth: dict[str, Any] | None = None
    rankings: dict[str, list[dict[str, Any]]]
    hotspots: dict[str, list[dict[str, Any]]]
    headlines: list[dict[str, Any]]
    flash_news: list[dict[str, Any]]
    errors: list[MarketResourceErrorResponse]


class MarketOverviewResponse(ApiModel):
    generated_at: datetime
    indices: list[dict[str, Any]]
    trends: list[dict[str, Any]]
    turnover: dict[str, Any]
    breadth: dict[str, Any] | None = None
    rankings: dict[str, list[dict[str, Any]]]
    hotspots: dict[str, list[dict[str, Any]]]
    headlines: list[dict[str, Any]]
    flash_news: list[dict[str, Any]]
    errors: list[MarketResourceErrorResponse]
