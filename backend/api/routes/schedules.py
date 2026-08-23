"""Schedule routes."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Depends, status

from backend.api.deps import get_schedule_service
from backend.api.schemas.error import error_responses
from backend.api.schemas.schedule import (
    CreateScheduleRequest,
    StrategyScheduleResponse,
    UpdateScheduleRequest,
)
from backend.api.security import require_authenticated
from backend.business.schedules.commands import (
    CreateScheduleCommand,
    UpdateScheduleCommand,
)
from backend.business.schedules.dto import StrategyScheduleDTO
from backend.business.schedules.service import ScheduleAppService

router = APIRouter(
    prefix="/api/aniu/schedules",
    tags=["Schedules"],
    dependencies=[Depends(require_authenticated)],
    responses=error_responses(401, 403, 404, 409, 422),
)


@router.get("", response_model=list[StrategyScheduleResponse])
async def list_schedules(
    service: Annotated[ScheduleAppService, Depends(get_schedule_service)],
) -> list[StrategyScheduleDTO]:
    return await service.list_schedules()


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=StrategyScheduleResponse,
)
async def create_schedule(
    service: Annotated[ScheduleAppService, Depends(get_schedule_service)],
    payload: CreateScheduleRequest = Body(...),
) -> StrategyScheduleDTO:
    return await service.create_schedule(CreateScheduleCommand(**payload.model_dump()))


@router.put(
    "/{schedule_id}",
    response_model=StrategyScheduleResponse,
)
async def update_schedule(
    schedule_id: int,
    service: Annotated[ScheduleAppService, Depends(get_schedule_service)],
    payload: UpdateScheduleRequest = Body(...),
) -> StrategyScheduleDTO:
    return await service.update_schedule(
        UpdateScheduleCommand(schedule_id=schedule_id, **payload.model_dump())
    )
