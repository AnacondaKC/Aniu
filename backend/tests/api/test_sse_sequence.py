"""SSE schema v3 sequencing, replay, and full-content tests."""

from __future__ import annotations

import asyncio

import pytest

from backend.api.sse import StreamHub, encode_sse_message, wrap_checkpoint


@pytest.mark.asyncio
async def test_stream_hub_assigns_monotonic_event_seq() -> None:
    hub = StreamHub()
    subscription = await hub.subscribe_run(42)

    await hub.publish_snapshot(42, {"run_id": 42, "status": "RUNNING"})
    await hub.publish_trace_step_delta(
        42,
        stage_id="s1",
        step_id="result",
        channel="text",
        delta="hello",
    )
    await hub.publish_trace_step_delta(
        42,
        stage_id="s1",
        step_id="result",
        channel="text",
        delta=" world",
    )

    first = subscription.queue.get_nowait()
    second = subscription.queue.get_nowait()
    third = subscription.queue.get_nowait()

    assert first["kind"] == "checkpoint"
    assert first["event_seq"] == 1
    assert second["kind"] == "trace_delta"
    assert second["event_seq"] == 2
    assert third["event_seq"] == 3
    assert {first["stream_id"], second["stream_id"], third["stream_id"]} == {
        subscription.stream_id
    }
    assert hub.current_seq(42) == 3


@pytest.mark.asyncio
async def test_terminal_snapshot_and_finish_run_publish_only_once() -> None:
    hub = StreamHub()
    subscription = await hub.subscribe_run(101)

    await hub.publish_snapshot(101, {"run_id": 101, "status": "COMPLETED"})
    published = subscription.queue.get_nowait()
    finished = await hub.finish_run(
        101,
        snapshot={"run_id": 101, "status": "COMPLETED"},
    )

    assert finished == published
    assert subscription.queue.empty()
    assert hub.current_seq(101) == 1
    await hub.unsubscribe_run(101, subscription.queue)
    assert hub.current_seq(101) == 0


@pytest.mark.asyncio
async def test_stream_hub_skips_unobserved_run_deltas() -> None:
    hub = StreamHub()

    await hub.publish_trace_step_delta(
        99,
        stage_id="s1",
        step_id="result",
        channel="text",
        delta="not observed",
    )

    assert hub.current_seq(99) == 0


@pytest.mark.asyncio
async def test_stream_hub_releases_terminal_run_replay_state() -> None:
    hub = StreamHub()
    subscription = await hub.subscribe_run(98)
    await hub.publish_trace_step_delta(
        98,
        stage_id="s1",
        step_id="result",
        channel="text",
        delta="done",
    )

    await hub.publish_snapshot(98, {"run_id": 98, "status": "COMPLETED"})

    assert hub.current_seq(98) == 2
    await hub.unsubscribe_run(98, subscription.queue)
    assert hub.current_seq(98) == 0

    resumed = await hub.subscribe_run(
        98,
        stream_id=subscription.stream_id,
        after_seq=0,
    )
    assert resumed.stream_id != subscription.stream_id
    assert resumed.event_seq == 0
    assert resumed.replay == ()


@pytest.mark.asyncio
async def test_terminal_checkpoint_follows_replay_with_a_new_sequence() -> None:
    hub = StreamHub()
    initial = await hub.subscribe_run(97)
    await hub.publish_trace_step_delta(
        97,
        stage_id="s1",
        step_id="result",
        channel="text",
        delta="first",
    )
    await hub.publish_trace_step_delta(
        97,
        stage_id="s1",
        step_id="result",
        channel="text",
        delta="second",
    )
    await hub.unsubscribe_run(97, initial.queue)
    resumed = await hub.subscribe_run(
        97,
        stream_id=initial.stream_id,
        after_seq=0,
    )

    checkpoint = await hub.finish_run(
        97, snapshot={"run_id": 97, "status": "COMPLETED"}
    )

    assert [message["event_seq"] for message in resumed.replay] == [1, 2]
    assert checkpoint is not None
    assert checkpoint["stream_id"] == resumed.stream_id
    assert checkpoint["event_seq"] == 3


@pytest.mark.asyncio
async def test_finish_run_broadcasts_terminal_checkpoint_to_existing_subscribers(
) -> None:
    hub = StreamHub()
    first = await hub.subscribe_run(96)
    second = await hub.subscribe_run(96)

    checkpoint = await hub.finish_run(
        96,
        snapshot={"run_id": 96, "status": "COMPLETED"},
    )

    assert checkpoint is not None
    assert first.queue.get_nowait() == checkpoint
    assert second.queue.get_nowait() == checkpoint


@pytest.mark.asyncio
async def test_history_eviction_starts_a_new_run_stream_generation() -> None:
    hub = StreamHub(history_run_maxsize=1)
    first = await hub.subscribe_run(95)
    await hub.publish_trace_step_delta(
        95,
        stage_id="s1",
        step_id="result",
        channel="text",
        delta="old",
    )
    await hub.unsubscribe_run(95, first.queue)

    second = await hub.subscribe_run(96)
    await hub.unsubscribe_run(96, second.queue)
    resumed = await hub.subscribe_run(95, stream_id=first.stream_id, after_seq=1)

    assert resumed.stream_id != first.stream_id
    assert resumed.event_seq == 0
    assert resumed.replay == ()


@pytest.mark.asyncio
async def test_history_eviction_keeps_the_newer_run_when_capacity_is_exceeded() -> None:
    hub = StreamHub(history_run_maxsize=2)
    first = await hub.subscribe_run(91)
    await hub.unsubscribe_run(91, first.queue)
    second = await hub.subscribe_run(92)
    await hub.unsubscribe_run(92, second.queue)
    third = await hub.subscribe_run(90)
    await hub.unsubscribe_run(90, third.queue)

    resumed = await hub.subscribe_run(92, stream_id=second.stream_id, after_seq=0)

    assert resumed.stream_id == second.stream_id


@pytest.mark.asyncio
async def test_history_cap_evicts_a_run_after_its_last_subscriber_leaves() -> None:
    hub = StreamHub(history_run_maxsize=1)
    first = await hub.subscribe_run(93)
    second = await hub.subscribe_run(94)

    await hub.unsubscribe_run(93, first.queue)
    resumed = await hub.subscribe_run(93, stream_id=first.stream_id, after_seq=0)

    assert resumed.stream_id != first.stream_id
    await hub.unsubscribe_run(94, second.queue)


@pytest.mark.asyncio
async def test_stream_hub_preserves_large_utf8_delta() -> None:
    hub = StreamHub()
    subscription = await hub.subscribe_run(43)
    delta = "牛" * 100_000

    await hub.publish_trace_step_delta(
        43,
        stage_id="s1",
        step_id="result",
        channel="text",
        delta=delta,
    )

    assert subscription.queue.get_nowait()["delta"] == delta


@pytest.mark.asyncio
async def test_concurrent_publish_keeps_queue_in_sequence_order() -> None:
    hub = StreamHub(queue_maxsize=128)
    subscription = await hub.subscribe_run(44)

    await asyncio.gather(
        *(
            hub.publish_trace_step_delta(
                44,
                stage_id="s1",
                step_id="result",
                channel="text",
                delta=str(index),
            )
            for index in range(100)
        )
    )

    received = [subscription.queue.get_nowait()["event_seq"] for _ in range(100)]
    assert received == list(range(1, 101))


@pytest.mark.asyncio
async def test_resume_replays_every_frame_after_client_watermark() -> None:
    hub = StreamHub(queue_maxsize=2, history_maxsize=16)
    initial = await hub.subscribe_run(45)
    for index in range(4):
        await hub.publish_trace_step_delta(
            45,
            stage_id="s1",
            step_id="result",
            channel="text",
            delta=str(index),
        )

    resumed = await hub.subscribe_run(
        45,
        stream_id=initial.stream_id,
        after_seq=1,
    )

    assert [message["event_seq"] for message in resumed.replay] == [2, 3, 4]
    assert "".join(str(message["delta"]) for message in resumed.replay) == "123"


@pytest.mark.asyncio
async def test_new_hub_uses_a_new_stream_id() -> None:
    first = await StreamHub().subscribe_run(46)
    second = await StreamHub().subscribe_run(46)

    assert first.stream_id != second.stream_id


def test_checkpoint_preserves_all_bodies_and_steps() -> None:
    steps = [
        {
            "step_id": f"result-{index}",
            "type": "result",
            "content": "内容" * 10_000,
            "data": {"result": "数据" * 10_000},
        }
        for index in range(30)
    ]
    steps.insert(
        0,
        {
            "step_id": "prompt",
            "type": "prompt",
            "content": "完整提示词",
            "data": {"user_message": "完整提示词"},
        },
    )
    snapshot = {
        "run_id": 42,
        "status": "RUNNING",
        "trace": {"stages": [{"stage_id": "research:na", "steps": steps}]},
    }

    checkpoint = wrap_checkpoint(42, snapshot, stream_id="stream-1", event_seq=3)
    frame = encode_sse_message(checkpoint)
    checkpoint_steps = checkpoint["snapshot"]["trace"]["stages"][0]["steps"]

    assert len(checkpoint_steps) == 31
    assert checkpoint_steps[0]["content"] == "完整提示词"
    assert checkpoint_steps[0]["data"] == {"user_message": "完整提示词"}
    assert checkpoint_steps[-1]["content"] == "内容" * 10_000
    assert "完整提示词" in frame
