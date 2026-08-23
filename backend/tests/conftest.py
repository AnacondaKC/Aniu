"""Pytest fixtures shared across backend tests."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
async def engine(tmp_path: Path):
    # Database dependencies stay lazy so isolated unit-test packages do not load Infra.
    from sqlalchemy.ext.asyncio import close_all_sessions, create_async_engine

    from backend.infra.db.models import Base

    db_path = tmp_path / "test.sqlite3"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    engine = create_async_engine(database_url)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    yield engine

    await close_all_sessions()
    await engine.dispose()


@pytest.fixture
def session_factory(engine):
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


@pytest.fixture
async def session(session_factory):
    async with session_factory() as db_session:
        yield db_session
