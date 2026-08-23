"""Repository for saved model profiles."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.business.settings import ModelProfile
from backend.business.shared import ConfigurationConflictError
from backend.infra.db.models import ModelProfileModel
from backend.infra.repositories.secret_store_repo import SecretStoreRepository
from backend.infra.security import SecretCodec
from backend.llm import ModelProtocol, ModelProviderConfig


class ModelProfileRepository:
    """Persistence adapter for model profiles."""

    def __init__(
        self,
        session: AsyncSession,
        secret_codec: SecretCodec | None = None,
    ):
        self._session = session
        codec = secret_codec or SecretCodec()
        self._secret_store = SecretStoreRepository(session, secret_codec=codec)

    async def list_profiles(self) -> list[ModelProfile]:
        statement = select(ModelProfileModel).order_by(
            ModelProfileModel.sort_order.asc(),
            ModelProfileModel.id.asc(),
        )
        models = list((await self._session.scalars(statement)).all())
        secrets = await self._secret_store.value_map(
            "model_profile",
            ignore_invalid=False,
        )
        return [
            self._model_to_profile(
                model,
                secrets.get(str(model.id), {}).get("api_key"),
            )
            for model in models
        ]

    async def get_by_id(self, profile_id: int) -> ModelProfile | None:
        model = await self._session.get(ModelProfileModel, profile_id)
        if model is None:
            return None
        return await self._to_profile(model)

    async def get_by_ids(self, profile_ids: set[int]) -> dict[int, ModelProfile]:
        if not profile_ids:
            return {}
        statement = select(ModelProfileModel).where(
            ModelProfileModel.id.in_(profile_ids)
        )
        models = list((await self._session.scalars(statement)).all())
        secrets = await self._secret_store.value_map(
            "model_profile",
            ignore_invalid=False,
        )
        return {
            model.id: self._model_to_profile(
                model,
                secrets.get(str(model.id), {}).get("api_key"),
            )
            for model in models
        }

    async def create(self, profile: ModelProfile) -> ModelProfile:
        if profile.profile_id != 0 or profile.revision != 0:
            raise ValueError("new model profiles must not have an id or revision")
        now = datetime.now(tz=UTC)
        model = ModelProfileModel(
            name=profile.name,
            protocol=profile.protocol.value,
            model_name=profile.model_name,
            base_url=profile.base_url,
            provider_config_json=profile.provider_config.as_dict(),
            enabled=profile.enabled,
            sort_order=profile.sort_order,
            revision=1,
            created_at=profile.created_at.isoformat(),
            updated_at=now.isoformat(),
        )
        self._session.add(model)
        await self._session.flush()
        await self._store_api_key(model.id, profile.api_key)
        return await self._to_profile(model)

    async def update(self, profile: ModelProfile) -> ModelProfile:
        if profile.profile_id <= 0 or profile.revision <= 0:
            raise ValueError(
                "existing model profiles require a positive id and revision"
            )
        model = await self._session.get(ModelProfileModel, profile.profile_id)
        if model is None:
            raise ValueError("model profile does not exist")
        if profile.revision != model.revision:
            raise ConfigurationConflictError(
                "model_profile", profile.revision, model.revision
            )
        model.name = profile.name
        model.protocol = profile.protocol.value
        model.model_name = profile.model_name
        model.base_url = profile.base_url
        model.provider_config_json = profile.provider_config.as_dict()
        model.enabled = profile.enabled
        model.sort_order = profile.sort_order
        model.updated_at = datetime.now(tz=UTC).isoformat()
        await self._session.flush()
        await self._store_api_key(model.id, profile.api_key)
        return await self._to_profile(model)

    async def _store_api_key(self, profile_id: int, api_key: str | None) -> None:
        await self._secret_store.set_secret(
            "model_profile",
            str(profile_id),
            "api_key",
            api_key,
        )

    async def delete(self, profile_id: int) -> None:
        model = await self._session.get(ModelProfileModel, profile_id)
        if model is not None:
            await self._secret_store.delete_owner("model_profile", str(profile_id))
            await self._session.delete(model)

    async def _to_profile(self, model: ModelProfileModel) -> ModelProfile:
        api_key = await self._secret_store.get_secret(
            "model_profile",
            str(model.id),
            "api_key",
        )
        return self._model_to_profile(model, api_key)

    @staticmethod
    def _model_to_profile(
        model: ModelProfileModel,
        api_key: str | None,
    ) -> ModelProfile:
        return ModelProfile(
            profile_id=model.id,
            name=model.name,
            protocol=ModelProtocol(model.protocol),
            model_name=model.model_name,
            base_url=model.base_url,
            api_key=api_key,
            provider_config=ModelProviderConfig.from_mapping(
                model.provider_config_json
            ),
            enabled=model.enabled,
            sort_order=model.sort_order,
            revision=model.revision,
            created_at=datetime.fromisoformat(model.created_at),
            updated_at=datetime.fromisoformat(model.updated_at),
        )
