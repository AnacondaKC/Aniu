from typing import Annotated, Literal

from fastapi import APIRouter, Body, Depends, Query, status

from backend.api.deps import get_memory_service
from backend.api.schemas.error import error_responses
from backend.api.schemas.memory import (
    CreateMemoryRequest,
    DeleteMemoryRequest,
    MemoryItemResponse,
    MemoryOverviewResponse,
    UpdateMemoryRequest,
)
from backend.api.security import require_authenticated
from backend.business.memories import MemoryOperation, MemoryService, MemoryWriteCommand

router = APIRouter(
    prefix="/api/aniu/memories",
    tags=["Memories"],
    dependencies=[Depends(require_authenticated)],
    responses=error_responses(401, 403, 422),
)


@router.get("", response_model=MemoryOverviewResponse)
async def get_memory_overview(
    service: Annotated[MemoryService, Depends(get_memory_service)],
    activity_limit: Annotated[int, Query(ge=1, le=100)] = 20,
    activity_offset: Annotated[int, Query(ge=0)] = 0,
    activity_task_id: Annotated[int | None, Query(ge=1)] = None,
    activity_operation: Annotated[
        Literal["read", "create", "update", "delete"] | None,
        Query(),
    ] = None,
    item_limit: Annotated[int, Query(ge=1, le=100)] = 20,
    item_offset: Annotated[int, Query(ge=0)] = 0,
    item_keywords: Annotated[str, Query(max_length=200)] = "",
) -> object:
    return await service.overview(
        activity_limit=activity_limit,
        activity_offset=activity_offset,
        activity_task_id=activity_task_id,
        activity_operation=activity_operation,
        item_limit=item_limit,
        item_offset=item_offset,
        item_keywords=item_keywords,
    )


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=MemoryItemResponse,
)
async def create_memory(
    service: Annotated[MemoryService, Depends(get_memory_service)],
    payload: CreateMemoryRequest = Body(...),
) -> object:
    command = MemoryWriteCommand(
        operation=MemoryOperation.CREATE,
        task_id=0,
        content=payload.content,
        reason=payload.reason,
    )
    return await service.write(command)


@router.put(
    "/{memory_id}",
    response_model=MemoryItemResponse,
)
async def update_memory(
    memory_id: int,
    service: Annotated[MemoryService, Depends(get_memory_service)],
    payload: UpdateMemoryRequest = Body(...),
) -> object:
    command = MemoryWriteCommand(
        operation=MemoryOperation.UPDATE,
        task_id=0,
        memory_id=memory_id,
        content=payload.content,
        reason=payload.reason,
        expected_version=payload.expected_version,
    )
    return await service.write(command)


@router.delete(
    "/{memory_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_memory(
    memory_id: int,
    service: Annotated[MemoryService, Depends(get_memory_service)],
    payload: DeleteMemoryRequest = Body(...),
) -> None:
    command = MemoryWriteCommand(
        operation=MemoryOperation.DELETE,
        task_id=0,
        memory_id=memory_id,
        expected_version=payload.expected_version,
    )
    await service.write(command)
