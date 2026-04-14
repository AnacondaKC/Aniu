from __future__ import annotations

import logging
from threading import Event, Thread

from app.core.config import get_settings
from app.services.aniu_service import aniu_service

logger = logging.getLogger(__name__)


class SchedulerService:
    def __init__(self) -> None:
        self._stop_event = Event()
        self._thread: Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = Thread(target=self._run_loop, name="aniu-scheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)

    def _run_loop(self) -> None:
        poll_seconds = max(5, get_settings().scheduler_poll_seconds)
        while not self._stop_event.is_set():
            try:
                aniu_service.process_due_schedule()
            except Exception as exc:
                logger.exception("scheduler loop error: %s", exc)
            self._stop_event.wait(poll_seconds)


scheduler_service = SchedulerService()
