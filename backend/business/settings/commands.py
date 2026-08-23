"""Settings commands."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

UNSET: object = object()


@dataclass(frozen=True, slots=True)
class UpdateSettingsCommand:
    """Partial settings update. Use UNSET for omitted fields."""

    mx_api_key: Any = field(default=UNSET)
    prompt_profile: Any = field(default=UNSET)
    stage_settings: Any = field(default=UNSET)
    dream_schedule_time: Any = field(default=UNSET)
    expected_revision: int | None = None

    def provided(self, name: str) -> bool:
        return getattr(self, name) is not UNSET
