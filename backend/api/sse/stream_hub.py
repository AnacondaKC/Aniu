"""SSE hub and routes for run checkpoints and text deltas (schema v3)."""

from __future__ import annotations

import asyncio
import json
from collections import OrderedDict, deque
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, cast
from uuid import uuid4

from fastapi import APIRouter, Depends, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse

from backend.api.schemas.error import error_responses
from backend.api.security import require_stream_authenticated
from backend.business.runs.queries import GetRunDetailQuery

SSE_SCHEMA_VERSION = 3


@dataclass(frozen=True, slots=True)
class RunStreamSubscription:
    queue: asyncio.Queue[dict[str, Any]]
    stream_id: str
    event_seq: int
    replay: tuple[dict[str, Any], ...] = ()


router = APIRouter(
    prefix="/api/aniu/sse",
    tags=["SSE"],
    dependencies=[Depends(require_stream_authenticated)],
    responses=error_responses(401, 403, 404, 422),
)


class StreamHub:
    """In-memory fan-out hub for ordered, resumable SSE run streams."""

    def __init__(
        self,
        queue_maxsize: int = 64,
        history_maxsize: int = 1024,
        history_run_maxsize: int = 32,
    ) -> None:
        self._queue_maxsize = queue_maxsize
        self._history_maxsize = max(history_maxsize, queue_maxsize)
        self._history_run_maxsize = max(1, history_run_maxsize)
        self._run_streams: dict[int, set[asyncio.Queue[dict[str, Any]]]] = {}
        self._stream_ids: dict[int, str] = {}
        self._event_seq: dict[int, int] = {}
        self._history: dict[int, deque[dict[str, Any]]] = {}
        self._replay_runs: OrderedDict[int, None] = OrderedDict()
        self._terminal_messages: dict[int, dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def subscribe_run(
        self,
        run_id: int,
        *,
        stream_id: str | None = None,
        after_seq: int | None = None,
    ) -> RunStreamSubscription:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=self._queue_maxsize
        )
        async with self._lock:
            self._run_streams.setdefault(run_id, set()).add(queue)
            self._track_run_locked(run_id)
            current_stream_id = self._stream_ids[run_id]
            event_seq = self._event_seq.setdefault(run_id, 0)
            replay = self._replay_locked(
                run_id,
                stream_id=stream_id,
                current_stream_id=current_stream_id,
                after_seq=after_seq,
                event_seq=event_seq,
            )
            return RunStreamSubscription(
                queue=queue,
                stream_id=current_stream_id,
                event_seq=event_seq,
                replay=replay,
            )

    async def unsubscribe_run(
        self,
        run_id: int,
        queue: asyncio.Queue[dict[str, Any]],
        *,
        clear_if_empty: bool = False,
    ) -> None:
        async with self._lock:
            subscribers = self._run_streams.get(run_id)
            if subscribers is None:
                return
            subscribers.discard(queue)
            if not subscribers:
                self._run_streams.pop(run_id, None)
                if clear_if_empty:
                    self._clear_replay_locked(run_id)
                elif (
                    run_id in self._replay_runs
                    and len(self._replay_runs) > self._history_run_maxsize
                ):
                    self._clear_replay_locked(run_id)
                elif run_id not in self._replay_runs:
                    self._clear_generation_locked(run_id)

    async def finish_run(
        self, run_id: int, *, snapshot: object | None = None
    ) -> dict[str, Any] | None:
        """Release replay data and optionally build one final checkpoint."""

        async with self._lock:
            existing = self._terminal_messages.get(run_id)
            if existing is not None:
                self._clear_replay_locked(run_id)
                return existing
            message: dict[str, Any] | None = None
            stream_id = self._stream_ids.get(run_id)
            if snapshot is not None and stream_id is not None:
                message = wrap_checkpoint(
                    run_id,
                    snapshot,
                    stream_id=stream_id,
                    event_seq=self._next_seq_locked(run_id),
                )
            if message is not None:
                self._terminal_messages[run_id] = message
                self._publish_locked(run_id, message)
            self._clear_replay_locked(run_id)
            return message

    def has_subscribers(self, run_id: int) -> bool:
        """Synchronously report whether any client is streaming this run."""

        return bool(self._run_streams.get(run_id))

    def current_seq(self, run_id: int) -> int:
        """Return the last published event_seq for a run (0 if none)."""

        return self._event_seq.get(run_id, 0)

    async def publish_snapshot(self, run_id: int, snapshot: object) -> None:
        """Publish a complete checkpoint."""

        status = (
            snapshot.get("status")
            if isinstance(snapshot, dict)
            else getattr(snapshot, "status", None)
        )
        is_terminal = status not in {None, "RUNNING"}
        async with self._lock:
            if run_id in self._terminal_messages:
                self._clear_replay_locked(run_id)
                return
            if run_id in self._replay_runs:
                event_seq = self._next_seq_locked(run_id)
                message = wrap_checkpoint(
                    run_id,
                    snapshot,
                    stream_id=self._stream_ids[run_id],
                    event_seq=event_seq,
                )
                self._publish_locked(run_id, message)
                if is_terminal:
                    self._terminal_messages[run_id] = message
            if is_terminal:
                self._clear_replay_locked(run_id)

    async def publish_trace_step_delta(
        self,
        run_id: int,
        *,
        stage_id: str,
        step_id: str,
        channel: str,
        delta: str,
    ) -> None:
        if not delta:
            return
        async with self._lock:
            if run_id not in self._replay_runs:
                return
            event_seq = self._next_seq_locked(run_id)
            self._publish_locked(
                run_id,
                {
                    "schema_version": SSE_SCHEMA_VERSION,
                    "kind": "trace_delta",
                    "stream_id": self._stream_ids[run_id],
                    "event_seq": event_seq,
                    "run_id": run_id,
                    "stage_id": stage_id,
                    "step_id": step_id,
                    "channel": channel,
                    "delta": delta,
                },
            )

    def _track_run_locked(self, run_id: int) -> None:
        self._stream_ids.setdefault(run_id, uuid4().hex)
        if run_id in self._terminal_messages:
            return
        self._replay_runs[run_id] = None
        self._replay_runs.move_to_end(run_id)
        if len(self._replay_runs) <= self._history_run_maxsize:
            return
        for candidate in tuple(self._replay_runs):
            if candidate != run_id and not self._run_streams.get(candidate):
                self._clear_replay_locked(candidate)
                if len(self._replay_runs) <= self._history_run_maxsize:
                    break

    def _clear_replay_locked(self, run_id: int) -> None:
        self._history.pop(run_id, None)
        self._replay_runs.pop(run_id, None)
        if not self._run_streams.get(run_id):
            self._clear_generation_locked(run_id)

    def _clear_generation_locked(self, run_id: int) -> None:
        self._stream_ids.pop(run_id, None)
        self._event_seq.pop(run_id, None)
        self._terminal_messages.pop(run_id, None)

    def _next_seq_locked(self, run_id: int) -> int:
        current = self._event_seq.get(run_id, 0) + 1
        self._event_seq[run_id] = current
        return current

    def _publish_locked(self, run_id: int, message: dict[str, Any]) -> None:
        history = self._history.setdefault(
            run_id,
            deque(maxlen=self._history_maxsize),
        )
        history.append(message)
        for queue in tuple(self._run_streams.get(run_id, set())):
            while queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                continue

    def _replay_locked(
        self,
        run_id: int,
        *,
        stream_id: str | None,
        current_stream_id: str,
        after_seq: int | None,
        event_seq: int,
    ) -> tuple[dict[str, Any], ...]:
        if (
            stream_id != current_stream_id
            or after_seq is None
            or after_seq >= event_seq
        ):
            return ()
        replay = tuple(
            message
            for message in self._history.get(run_id, ())
            if int(message["event_seq"]) > after_seq
        )
        if not replay:
            return ()
        expected = list(range(after_seq + 1, event_seq + 1))
        actual = [int(message["event_seq"]) for message in replay]
        return replay if actual == expected else ()


def _json_bytes(value: object) -> bytes:
    return json.dumps(
        jsonable_encoder(value),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _checkpoint_snapshot(snapshot: object) -> object:
    """Encode a complete live snapshot without removing bodies or steps."""

    return jsonable_encoder(snapshot)


def encode_sse_message(message: object) -> str:
    payload = _json_bytes(message)
    return (b"data: " + payload + b"\n\n").decode("utf-8")


def keepalive_frame() -> str:
    return ": keep-alive\n\n"


def wrap_checkpoint(
    run_id: int,
    snapshot: object,
    *,
    stream_id: str,
    event_seq: int = 0,
) -> dict[str, Any]:
    return {
        "schema_version": SSE_SCHEMA_VERSION,
        "kind": "checkpoint",
        "stream_id": stream_id,
        "event_seq": event_seq,
        "run_id": run_id,
        "snapshot": _checkpoint_snapshot(snapshot),
    }


@router.get(
    "/run/{run_id}",
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "Server-sent run checkpoint and trace events",
            "content": {"text/event-stream": {"schema": {"type": "string"}}},
        }
    },
)
async def stream_run_snapshot(
    run_id: int,
    request: Request,
) -> StreamingResponse:
    runtime = request.app.state.runtime
    stream_hub = cast(StreamHub, runtime.require_stream_hub())
    requested_stream_id = request.query_params.get("stream_id")
    try:
        requested_after_seq = int(request.query_params["after_seq"])
    except (KeyError, ValueError):
        requested_after_seq = None
    subscription = await stream_hub.subscribe_run(
        run_id,
        stream_id=requested_stream_id,
        after_seq=requested_after_seq,
    )

    try:
        session_factory = runtime.require_session_factory()
        async with session_factory() as session:
            initial_snapshot = await runtime.run_service(session).get_run_detail(
                GetRunDetailQuery(run_id=run_id)
            )
    except Exception:
        await stream_hub.unsubscribe_run(
            run_id,
            subscription.queue,
            clear_if_empty=True,
        )
        raise

    terminal_checkpoint = (
        await stream_hub.finish_run(run_id, snapshot=initial_snapshot)
        if initial_snapshot.status != "RUNNING"
        else None
    )
    initial_checkpoint = terminal_checkpoint or wrap_checkpoint(
        run_id,
        initial_snapshot,
        stream_id=subscription.stream_id,
        event_seq=subscription.event_seq,
    )

    async def event_source() -> AsyncIterator[str]:
        try:
            if subscription.replay:
                for message in subscription.replay:
                    yield encode_sse_message(message)
            else:
                yield encode_sse_message(initial_checkpoint)
            if terminal_checkpoint is not None:
                if subscription.replay:
                    yield encode_sse_message(terminal_checkpoint)
                return
            while True:
                if await request.is_disconnected():
                    break
                try:
                    message = await asyncio.wait_for(
                        subscription.queue.get(),
                        timeout=15.0,
                    )
                except TimeoutError:
                    yield keepalive_frame()
                    continue
                yield encode_sse_message(message)
        finally:
            await stream_hub.unsubscribe_run(run_id, subscription.queue)

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
