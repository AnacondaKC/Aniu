"""Repository for application settings."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.business.settings import (
    AniuAgentPrompt,
    AppSettings,
    normalize_stage_settings,
)
from backend.business.shared import ConfigurationConflictError
from backend.infra.db.models import AppSettingsModel
from backend.infra.repositories.secret_store_repo import SecretStoreRepository
from backend.infra.security import SecretCodec


class SettingsRepository:
    """Persistence adapter for the single app settings row."""

    def __init__(
        self,
        session: AsyncSession,
        secret_codec: SecretCodec | None = None,
    ):
        self._session = session
        codec = secret_codec or SecretCodec()
        self._secret_store = SecretStoreRepository(session, secret_codec=codec)

    async def get(self) -> AppSettings | None:
        statement = (
            select(AppSettingsModel).order_by(AppSettingsModel.id.asc()).limit(1)
        )
        model = (await self._session.scalars(statement)).first()
        return None if model is None else await self._to_domain(model)

    async def save(self, settings: AppSettings) -> AppSettings:
        statement = (
            select(AppSettingsModel).order_by(AppSettingsModel.id.asc()).limit(1)
        )
        model = (await self._session.scalars(statement)).first()
        now = datetime.now(tz=UTC)
        if model is None:
            if settings.revision not in {0, 1}:
                raise ConfigurationConflictError("app_settings", settings.revision, 0)
            model = AppSettingsModel(
                prompt_profile_json=settings.prompt_profile.as_dict(),
                stage_settings_json={
                    stage_id: item.as_dict()
                    for stage_id, item in settings.stage_settings.items()
                },
                dream_schedule_time=settings.dream_schedule_time,
                revision=1,
                created_at=settings.created_at.isoformat(),
                updated_at=now.isoformat(),
            )
            self._session.add(model)
        else:
            if settings.revision not in {0, model.revision}:
                raise ConfigurationConflictError(
                    "app_settings", settings.revision, model.revision
                )
            model.prompt_profile_json = settings.prompt_profile.as_dict()
            model.stage_settings_json = {
                stage_id: item.as_dict()
                for stage_id, item in settings.stage_settings.items()
            }
            model.dream_schedule_time = settings.dream_schedule_time
            model.updated_at = now.isoformat()

        await self._session.flush()
        await self._secret_store.set_secret(
            "app_settings",
            str(model.id),
            "mx_api_key",
            settings.mx_api_key,
        )
        return await self._to_domain(model)

    async def _to_domain(self, model: AppSettingsModel) -> AppSettings:
        mx_api_key = await self._secret_store.get_secret(
            "app_settings",
            str(model.id),
            "mx_api_key",
        )
        return AppSettings(
            mx_api_key=mx_api_key,
            prompt_profile=AniuAgentPrompt.from_mapping(model.prompt_profile_json),
            stage_settings=normalize_stage_settings(model.stage_settings_json),
            dream_schedule_time=model.dream_schedule_time,
            revision=model.revision,
            created_at=datetime.fromisoformat(model.created_at),
            updated_at=datetime.fromisoformat(model.updated_at),
        )
