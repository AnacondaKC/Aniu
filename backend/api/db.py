"""Request-scoped database session dependencies."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated, Any, cast

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


def get_session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    runtime: Any = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise RuntimeError("application runtime is not configured")
    return cast(async_sessionmaker[AsyncSession], runtime.require_session_factory())


async def get_db_session(
    session_factory: Annotated[
        async_sessionmaker[AsyncSession], Depends(get_session_factory)
    ],
) -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
