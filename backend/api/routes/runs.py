"""Run orchestration routes."""

from __future__ import annotations

from datetime import date
from typing import Annotated, Protocol

from fastapi import APIRouter, Body, Depends, Query, status
from pydantic import BaseModel, ConfigDict

from backend.api.deps import get_run_service, get_run_worker
from backend.api.schemas.error import error_responses
from backend.api.schemas.run import (
    AbortRunResponse,
    RunDetailResponse,
    RunSummaryResponse,
)
from backend.api.security import require_authenticated
from backend.business.runs.commands import StartRunCommand
from backend.business.runs.dto import RunDetailDTO, RunSummaryDTO
from backend.business.runs.queries import GetRunDetailQuery, ListRunsQuery
from backend.business.runs.service import RunService
from backend.business.shared import TriggerSource


class RunSubmitter(Protocol):
    async def submit(self, run_id: int) -> None: ...


router = APIRouter(
    prefix="/api/aniu/runs",
    tags=["Runs"],
    dependencies=[Depends(require_authenticated)],
    responses=error_responses(401, 403, 404, 409, 422, 503),
)


class StartRunRequest(BaseModel):
    """HTTP request body for starting a strategy run."""

    model_config = ConfigDict(extra="forbid")

    trigger_source: TriggerSource = TriggerSource.MANUAL
    schedule_id: int | None = None
    prompt_version: str = "m1-bootstrap"
    risk_rules_version: str = "m1-st-only"
    model_name: str | None = None


class AbortRunRequest(BaseModel):
    """HTTP request body for aborting the active strategy run."""

    model_config = ConfigDict(extra="forbid")

    reason: str = "user_requested"


@router.post(
    "/start",
    status_code=status.HTTP_201_CREATED,
    response_model=RunDetailResponse,
)
async def start_run(
    service: Annotated[RunService, Depends(get_run_service)],
    run_worker: Annotated[RunSubmitter, Depends(get_run_worker)],
    payload: StartRunRequest | None = Body(default=None),
) -> RunDetailDTO:
    """Create one strategy run and queue it on the process-local worker."""

    request = payload or StartRunRequest()
    result = await service.create_run(
        StartRunCommand(
            trigger_source=request.trigger_source,
            schedule_id=request.schedule_id,
            prompt_version=request.prompt_version,
            risk_rules_version=request.risk_rules_version,
            model_name=request.model_name,
        )
    )
    await run_worker.submit(result.run_id)
    return result


@router.get("", response_model=list[RunSummaryResponse])
async def list_runs(
    service: Annotated[RunService, Depends(get_run_service)],
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    started_date: Annotated[date | None, Query()] = None,
) -> list[RunSummaryDTO]:
    """List strategy runs."""

    return await service.list_runs(
        ListRunsQuery(limit=limit, offset=offset, started_date=started_date)
    )


@router.get("/{run_id}", response_model=RunDetailResponse)
async def get_run_detail(
    run_id: int,
    service: Annotated[RunService, Depends(get_run_service)],
) -> RunDetailDTO:
    """Return one run detail record."""

    return await service.get_run_detail(GetRunDetailQuery(run_id=run_id))


@router.delete(
    "/{run_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_run(
    run_id: int,
    service: Annotated[RunService, Depends(get_run_service)],
) -> None:
    """Delete one completed or failed run snapshot."""

    await service.delete_run(run_id)


@router.post(
    "/{run_id}/abort",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=AbortRunResponse,
)
async def abort_run(
    run_id: int,
    service: Annotated[RunService, Depends(get_run_service)],
    payload: AbortRunRequest | None = Body(default=None),
) -> AbortRunResponse:
    """Request cooperative cancellation for the active run."""

    request = payload or AbortRunRequest()
    await service.abort_run(run_id, reason=request.reason)
    return AbortRunResponse(run_id=run_id, status="abort_requested")
