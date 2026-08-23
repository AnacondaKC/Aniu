"""Persistence tests for simple task-sourced memories."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from backend.business.memories import (
    MemoryMatchMode,
    MemoryOperation,
    MemoryService,
    MemoryWriteCommand,
)
from backend.infra.db.models import MemoryActivityModel, MemoryItemModel
from backend.infra.repositories.memory_repo import MemoryRepository


def _create_command() -> MemoryWriteCommand:
    return MemoryWriteCommand(
        operation=MemoryOperation.CREATE,
        task_id=101,
        content="弱市缩量反弹时不要追高，等待成交量确认。",
        reason="多次观察到追高后出现回撤。",
    )


@pytest.mark.asyncio
async def test_memory_create_update_soft_delete_and_search(
    session, session_factory
) -> None:
    service = MemoryService(MemoryRepository(session))
    created = await service.write(_create_command())
    await session.commit()

    assert created.id > 0
    assert created.version == 1
    assert created.content.startswith("弱市缩量")
    assert created.reason == "多次观察到追高后出现回撤。"
    assert created.created_task_id == 101
    assert created.updated_task_id == 101
    assert created.deleted_at is None

    async with session_factory() as verify_session:
        verifier = MemoryService(MemoryRepository(verify_session))
        matches = await verifier.read(keywords="弱市 追高", limit=5)
        assert [item.id for item in matches] == [created.id]

        updated = await verifier.write(
            MemoryWriteCommand(
                operation=MemoryOperation.UPDATE,
                task_id=102,
                memory_id=created.id,
                expected_version=1,
                content="弱市缩量反弹时不要追高，等待成交量和趋势确认。",
                reason="补充趋势确认条件。",
            )
        )
        await verify_session.commit()

    assert updated.version == 2
    assert updated.updated_task_id == 102
    assert "趋势确认" in updated.content

    async with session_factory() as delete_session:
        deleter = MemoryService(MemoryRepository(delete_session))
        deleted = await deleter.write(
            MemoryWriteCommand(
                operation=MemoryOperation.DELETE,
                task_id=103,
                memory_id=created.id,
                expected_version=2,
            )
        )
        await delete_session.commit()

    assert deleted.version == 3
    assert deleted.deleted_at is not None
    assert deleted.updated_task_id == 103

    async with session_factory() as verify_session:
        verifier = MemoryService(MemoryRepository(verify_session))
        assert await verifier.read(keywords="追高", limit=5) == []
        assert (
            await MemoryRepository(verify_session).list_items(limit=100, offset=0) == []
        )


@pytest.mark.asyncio
async def test_memory_version_check_is_atomic(session_factory) -> None:
    async with session_factory() as seed_session:
        created = await MemoryService(MemoryRepository(seed_session)).write(
            _create_command()
        )
        await seed_session.commit()

    async with session_factory() as first_session, session_factory() as second_session:
        first_row = await first_session.get(MemoryItemModel, created.id)
        second_row = await second_session.get(MemoryItemModel, created.id)
        assert first_row is not None
        assert second_row is not None
        assert first_row.version == second_row.version == 1

        await MemoryRepository(first_session).write(
            MemoryWriteCommand(
                operation=MemoryOperation.UPDATE,
                task_id=401,
                memory_id=created.id,
                expected_version=1,
                content="第一份并发更新。",
                reason="先提交的版本。",
            )
        )
        await first_session.commit()

        with pytest.raises(ValueError, match="memory version conflict"):
            await MemoryRepository(second_session).write(
                MemoryWriteCommand(
                    operation=MemoryOperation.UPDATE,
                    task_id=402,
                    memory_id=created.id,
                    expected_version=1,
                    content="第二份并发更新。",
                    reason="过期版本。",
                )
            )
        await second_session.rollback()

    async with session_factory() as verify_session:
        items = await MemoryRepository(verify_session).list_items(limit=100, offset=0)
        assert len(items) == 1
        assert items[0].content == "第一份并发更新。"
        assert items[0].version == 2


@pytest.mark.asyncio
async def test_memory_search_supports_and_or_modes(session) -> None:
    service = MemoryService(MemoryRepository(session))
    await service.write(
        MemoryWriteCommand(
            operation=MemoryOperation.CREATE,
            task_id=301,
            content="半导体仓位控制",
            reason="单一行业需要分散风险。",
        )
    )
    await service.write(
        MemoryWriteCommand(
            operation=MemoryOperation.CREATE,
            task_id=302,
            content="农业轮动策略",
            reason="防守板块适合低吸。",
        )
    )
    await session.commit()

    repository = MemoryRepository(session)
    assert await repository.count_items() == 2
    assert await repository.count_items(keywords="农业") == 1
    first_page = await repository.list_items(limit=1, offset=0)
    second_page = await repository.list_items(limit=1, offset=1)
    filtered_page = await repository.list_items(limit=10, offset=0, keywords="分散风险")
    assert [item.content for item in first_page + second_page] == [
        "农业轮动策略",
        "半导体仓位控制",
    ]
    assert [item.content for item in filtered_page] == ["半导体仓位控制"]

    and_matches = await service.read(
        keywords="半导体 农业",
        match_mode=MemoryMatchMode.AND,
        limit=10,
    )
    or_matches = await service.read(
        keywords="半导体 农业",
        match_mode=MemoryMatchMode.OR,
        limit=10,
    )

    assert and_matches == []
    assert {item.content for item in or_matches} == {"半导体仓位控制", "农业轮动策略"}


@pytest.mark.asyncio
async def test_memory_search_escapes_like_wildcards(session) -> None:
    service = MemoryService(MemoryRepository(session))
    await service.write(
        MemoryWriteCommand(
            operation=MemoryOperation.CREATE,
            task_id=501,
            content="收益率 10%",
            reason="百分号是内容的一部分。",
        )
    )
    await service.write(
        MemoryWriteCommand(
            operation=MemoryOperation.CREATE,
            task_id=502,
            content="收益率 100",
            reason="没有百分号。",
        )
    )
    await service.write(
        MemoryWriteCommand(
            operation=MemoryOperation.CREATE,
            task_id=503,
            content="A_B 形态",
            reason="下划线是内容的一部分。",
        )
    )
    await service.write(
        MemoryWriteCommand(
            operation=MemoryOperation.CREATE,
            task_id=504,
            content="ACB 形态",
            reason="没有下划线。",
        )
    )
    await session.commit()

    repository = MemoryRepository(session)
    percent_items = await repository.list_items(limit=10, offset=0, keywords="%")
    underscore_items = await repository.list_items(limit=10, offset=0, keywords="_")
    assert [item.content for item in percent_items] == ["收益率 10%"]
    assert [item.content for item in underscore_items] == ["A_B 形态"]
    assert await repository.count_items(keywords="%") == 1
    assert await repository.count_items(keywords="_") == 1
    assert [item.content for item in await service.read(keywords="%", limit=10)] == [
        "收益率 10%"
    ]


@pytest.mark.asyncio
async def test_memory_read_activity_keeps_keywords_and_result_count(session) -> None:
    service = MemoryService(MemoryRepository(session))
    await service.record_read(keywords="弱市 追高", result_count=2, task_id=150)
    await session.commit()

    repository = MemoryRepository(session)
    activities = await repository.list_activities(limit=1, offset=0)

    assert await repository.count_activities() == 1
    assert len(activities) == 1
    assert activities[0].operation.value == "read"
    assert activities[0].content == "弱市 追高"
    assert activities[0].result_count == 2
    assert activities[0].task_id == 150


@pytest.mark.asyncio
async def test_memory_overview_includes_activity_history(session) -> None:
    session.add_all(
        [
            MemoryActivityModel(
                operation="read",
                content="历史活动",
                result_count=1,
                task_id=201,
                created_at="2026-08-17T08:00:00+00:00",
            ),
            MemoryActivityModel(
                operation="create",
                content="当前活动",
                memory_id=1,
                task_id=202,
                created_at="2026-08-18T08:00:00+00:00",
            ),
        ]
    )
    await session.commit()

    service = MemoryService(
        MemoryRepository(session),
        now_provider=lambda: datetime(2026, 8, 18, 12, tzinfo=UTC),
    )
    overview = await service.overview(activity_limit=1, activity_offset=0)
    older_overview = await service.overview(activity_limit=1, activity_offset=1)

    assert overview.activity_total == 2
    assert [activity.content for activity in overview.activities] == ["当前活动"]
    assert [activity.content for activity in older_overview.activities] == ["历史活动"]


@pytest.mark.asyncio
async def test_memory_rejects_invalid_write_commands(session) -> None:
    service = MemoryService(MemoryRepository(session))

    with pytest.raises(ValueError, match="requires content"):
        await service.write(
            MemoryWriteCommand(operation=MemoryOperation.CREATE, task_id=1)
        )

    with pytest.raises(ValueError, match="requires memory_id"):
        await service.write(
            MemoryWriteCommand(
                operation=MemoryOperation.UPDATE,
                task_id=1,
                content="新内容",
            )
        )

    with pytest.raises(ValueError, match="requires memory_id"):
        await service.write(
            MemoryWriteCommand(operation=MemoryOperation.DELETE, task_id=1)
        )

    # reason 现在对所有操作必填
    with pytest.raises(ValueError, match="requires reason"):
        await service.write(
            MemoryWriteCommand(
                operation=MemoryOperation.CREATE,
                task_id=1,
                content="有内容但没有原因。",
            )
        )

    with pytest.raises(ValueError, match="requires reason"):
        await service.write(
            MemoryWriteCommand(
                operation=MemoryOperation.UPDATE,
                task_id=1,
                memory_id=1,
                expected_version=1,
                content="新内容",
            )
        )
