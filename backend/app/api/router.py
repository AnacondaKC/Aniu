from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core.auth import get_current_user
from app.db.database import get_db
from app.schemas.aniu import (
    AccountOverviewDebugRead,
    AccountOverviewRead,
    AppSettingsRead,
    AppSettingsUpdate,
    ChatRequest,
    ChatResponse,
    LoginRequest,
    LoginResponse,
    RunDetailRead,
    RunSummaryRead,
    RunSummaryPageRead,
    RuntimeOverviewRead,
    ScheduleRead,
    ScheduleUpdate,
)
from app.services.aniu_service import aniu_service

router = APIRouter(prefix="/api/aniu", tags=["aniu"])


@router.post("/login", response_model=LoginResponse)
def login(payload: LoginRequest) -> LoginResponse:
    try:
        return aniu_service.authenticate_login(payload.username, payload.password)
    except RuntimeError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc


@router.get("/settings", response_model=AppSettingsRead)
def get_settings(
    db: Session = Depends(get_db),
    _user: str = Depends(get_current_user),
) -> AppSettingsRead:
    return aniu_service.get_or_create_settings(db)


@router.put("/settings", response_model=AppSettingsRead)
def update_settings(
    payload: AppSettingsUpdate,
    db: Session = Depends(get_db),
    _user: str = Depends(get_current_user),
) -> AppSettingsRead:
    return aniu_service.update_settings(db, payload)


@router.get("/schedule", response_model=list[ScheduleRead])
def get_schedule(
    db: Session = Depends(get_db),
    _user: str = Depends(get_current_user),
) -> list[ScheduleRead]:
    return aniu_service.list_schedules(db)


@router.put("/schedule", response_model=list[ScheduleRead])
def update_schedule(
    payload: list[ScheduleUpdate],
    db: Session = Depends(get_db),
    _user: str = Depends(get_current_user),
) -> list[ScheduleRead]:
    return aniu_service.replace_schedules(db, payload)


@router.post("/run", response_model=RunDetailRead)
def run_once(
    schedule_id: int | None = Query(default=None, ge=1),
    _user: str = Depends(get_current_user),
) -> RunDetailRead:
    try:
        return aniu_service.execute_run(trigger_source="manual", schedule_id=schedule_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/runs", response_model=list[RunSummaryRead])
def list_runs(
    limit: int = Query(default=20, ge=1, le=100),
    run_date: date | None = Query(default=None, alias="date"),
    status: str | None = Query(default=None),
    before_id: int | None = Query(default=None, ge=1),
    db: Session = Depends(get_db),
    _user: str = Depends(get_current_user),
) -> list[RunSummaryRead]:
    return aniu_service.list_runs(
        db,
        limit=limit,
        run_date=run_date,
        status=status,
        before_id=before_id,
    )


@router.get("/runs-feed", response_model=RunSummaryPageRead)
def list_runs_feed(
    limit: int = Query(default=20, ge=1, le=100),
    run_date: date | None = Query(default=None, alias="date"),
    status: str | None = Query(default=None),
    before_id: int | None = Query(default=None, ge=1),
    db: Session = Depends(get_db),
    _user: str = Depends(get_current_user),
) -> RunSummaryPageRead:
    return aniu_service.list_runs_page(
        db,
        limit=limit,
        run_date=run_date,
        status=status,
        before_id=before_id,
    )


@router.get("/runs/{run_id}", response_model=RunDetailRead)
def get_run(
    run_id: int,
    db: Session = Depends(get_db),
    _user: str = Depends(get_current_user),
) -> RunDetailRead:
    run = aniu_service.get_run(db, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="运行记录不存在。")
    return run


@router.get("/runtime-overview", response_model=RuntimeOverviewRead)
def get_runtime_overview(
    db: Session = Depends(get_db),
    _user: str = Depends(get_current_user),
) -> RuntimeOverviewRead:
    return aniu_service.get_runtime_overview(db)


@router.get("/account", response_model=AccountOverviewRead)
def get_account(
    force_refresh: bool = Query(default=False),
    _user: str = Depends(get_current_user),
) -> AccountOverviewRead:
    try:
        return aniu_service.get_account_overview(force_refresh=force_refresh)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/account/debug", response_model=AccountOverviewDebugRead)
def get_account_debug(
    force_refresh: bool = Query(default=False),
    _user: str = Depends(get_current_user),
) -> AccountOverviewDebugRead:
    try:
        return aniu_service.get_account_overview(
            include_raw=True,
            force_refresh=force_refresh,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/chat", response_model=ChatResponse)
def chat(
    payload: ChatRequest,
    _user: str = Depends(get_current_user),
) -> ChatResponse:
    try:
        return aniu_service.chat(payload)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
