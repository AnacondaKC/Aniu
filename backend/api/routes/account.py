"""Market and portfolio account routes."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from backend.api.deps import get_account_app_service
from backend.api.schemas.account import (
    AccountDashboardResponse,
    AccountRefreshResultResponse,
)
from backend.api.schemas.error import error_responses
from backend.api.security import require_authenticated
from backend.business.account.dto import (
    AccountDashboardDTO,
    AccountRefreshResultDTO,
)
from backend.business.account.service import AccountAppService

router = APIRouter(
    prefix="/api/aniu/account",
    tags=["Account"],
    dependencies=[Depends(require_authenticated)],
    responses=error_responses(401, 403, 422, 429, 502, 503),
)


@router.get("/dashboard", response_model=AccountDashboardResponse)
async def get_account_dashboard(
    service: Annotated[AccountAppService, Depends(get_account_app_service)],
) -> AccountDashboardDTO:
    return await service.get_account_dashboard()


@router.post(
    "/refresh",
    response_model=AccountRefreshResultResponse,
)
async def refresh_account_cache(
    service: Annotated[AccountAppService, Depends(get_account_app_service)],
) -> AccountRefreshResultDTO:
    return await service.refresh_account_cache()
