"""API routes for nightly memory dreams."""

from __future__ import annotations

from typing import Annotated, Protocol

from fastapi import APIRouter, Depends, HTTPException, Query, status

from backend.api.deps import get_dream_service, get_dream_worker, get_memory_service
from backend.api.schemas.error import error_responses
from backend.api.schemas.memory_dream import (
    MemoryDreamDetailResponse,
    MemoryDreamListResponse,
    MemoryDreamResponse,
)
from backend.api.security import require_authenticated
from backend.business.dreams import DreamStatus
from backend.business.dreams.service import DreamService
from backend.business.memories.service import MemoryService


class DreamSubmitter(Protocol):
    async def submit(self, task_id: int) -> None: ...


router = APIRouter(
    prefix="/api/aniu/memory-dreams",
    tags=["MemoryDreams"],
    dependencies=[Depends(require_authenticated)],
    responses=error_responses(401, 403, 422),
)


@router.post(
    "/run",
    response_model=MemoryDreamResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def run_memory_dream(
    service: Annotated[DreamService, Depends(get_dream_service)],
    worker: Annotated[DreamSubmitter, Depends(get_dream_worker)],
) -> object:
    dream = await service.prepare_manual_run()
    if dream.status is DreamStatus.PENDING:
        await worker.submit(dream.task_id)
    return dream


@router.get("", response_model=MemoryDreamListResponse)
async def list_memory_dreams(
    service: Annotated[DreamService, Depends(get_dream_service)],
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> object:
    items, total = await service.list_recent(limit=limit, offset=offset)
    latest_items, _ = await service.list_recent(limit=1, offset=0)
    return {
        "items": items,
        "total": total,
        "latest": latest_items[0] if latest_items else None,
    }


@router.get(
    "/{task_id}",
    response_model=MemoryDreamDetailResponse,
    responses=error_responses(404),
)
async def get_memory_dream(
    task_id: int,
    dream_service: Annotated[DreamService, Depends(get_dream_service)],
    memory_service: Annotated[MemoryService, Depends(get_memory_service)],
) -> object:
    dream = await dream_service.get_by_id(task_id)
    if dream is None:
        raise HTTPException(status_code=404, detail="梦境任务不存在")
    activities, activity_total = await memory_service.activities(
        task_id=task_id,
        limit=100,
    )
    return {
        "dream": dream,
        "activities": activities,
        "activity_total": activity_total,
    }


@router.delete(
    "/{task_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=error_responses(404),
)
async def delete_memory_dream(
    task_id: int,
    service: Annotated[DreamService, Depends(get_dream_service)],
) -> None:
    deleted = await service.delete(task_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="梦境任务不存在")
