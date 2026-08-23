"""Repository for selected channel models."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.business.settings import SelectedModel
from backend.infra.db.models import SelectedModelModel
from backend.llm import normalize_thinking_efforts


class SelectedModelRepository:
    """Persistence adapter for selected models."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def list_all(self) -> list[SelectedModel]:
        statement = select(SelectedModelModel).order_by(
            SelectedModelModel.channel_profile_id.asc(),
            SelectedModelModel.sort_order.asc(),
            SelectedModelModel.id.asc(),
        )
        models = list((await self._session.scalars(statement)).all())
        return [self._to_selected_model(model) for model in models]

    async def list_by_channel(self, channel_profile_id: int) -> list[SelectedModel]:
        statement = (
            select(SelectedModelModel)
            .where(SelectedModelModel.channel_profile_id == channel_profile_id)
            .order_by(SelectedModelModel.sort_order.asc(), SelectedModelModel.id.asc())
        )
        models = list((await self._session.scalars(statement)).all())
        return [self._to_selected_model(model) for model in models]

    async def get_by_id(self, selected_model_id: int) -> SelectedModel | None:
        model = await self._session.get(SelectedModelModel, selected_model_id)
        if model is None:
            return None
        return self._to_selected_model(model)

    async def get_by_ids(
        self, selected_model_ids: set[int]
    ) -> dict[int, SelectedModel]:
        if not selected_model_ids:
            return {}
        statement = select(SelectedModelModel).where(
            SelectedModelModel.id.in_(selected_model_ids)
        )
        models = list((await self._session.scalars(statement)).all())
        return {model.id: self._to_selected_model(model) for model in models}

    async def replace_for_channel(
        self,
        channel_profile_id: int,
        models: list[SelectedModel],
    ) -> list[SelectedModel]:
        """Replace one channel catalogue while preserving stable model identities.

        Stage settings persist ``selected_model_id`` values. Recreating every row on
        each editor save would silently retarget or invalidate those references, so
        existing rows are matched by provider identity (falling back to model name)
        and updated in place.
        """

        existing_rows = list(
            (
                await self._session.scalars(
                    select(SelectedModelModel).where(
                        SelectedModelModel.channel_profile_id == channel_profile_id
                    )
                )
            ).all()
        )
        existing_by_identity = {
            self._identity(row.provider_id, row.model_name): row
            for row in existing_rows
        }
        incoming_identities: set[tuple[str, str]] = set()
        stored: list[SelectedModel] = []
        now = datetime.now(tz=UTC)

        for model in models:
            identity = self._identity(model.provider_id, model.model_name)
            if identity in incoming_identities:
                raise ValueError(f"duplicate selected model identity: {identity[1]}")
            incoming_identities.add(identity)

            row = existing_by_identity.pop(identity, None)
            if row is None:
                row = SelectedModelModel(
                    channel_profile_id=channel_profile_id,
                    created_at=model.created_at.isoformat(),
                )
                self._session.add(row)

            row.model_name = model.model_name
            row.label = model.label
            row.provider_id = model.provider_id
            row.context_window_tokens = model.context_window_tokens
            row.max_output_tokens = model.max_output_tokens
            row.input_price_per_million = model.input_price_per_million
            row.output_price_per_million = model.output_price_per_million
            row.cache_read_price_per_million = model.cache_read_price_per_million
            row.cache_write_price_per_million = model.cache_write_price_per_million
            row.thinking_efforts_json = list(model.thinking_efforts)
            row.sort_order = model.sort_order
            row.updated_at = now.isoformat()
            await self._session.flush()
            stored.append(self._to_selected_model(row))

        for obsolete in existing_by_identity.values():
            await self._session.delete(obsolete)
        await self._session.flush()
        return stored

    async def delete_by_channel(self, channel_profile_id: int) -> None:
        await self._session.execute(
            delete(SelectedModelModel).where(
                SelectedModelModel.channel_profile_id == channel_profile_id
            )
        )

    @staticmethod
    def _identity(provider_id: str | None, model_name: str) -> tuple[str, str]:
        normalized_provider = (provider_id or "").strip()
        if normalized_provider:
            return ("provider", normalized_provider)
        return ("model", model_name.strip())

    def _to_selected_model(self, model: SelectedModelModel) -> SelectedModel:
        return SelectedModel(
            selected_model_id=model.id,
            channel_profile_id=model.channel_profile_id,
            model_name=model.model_name,
            label=model.label,
            provider_id=model.provider_id,
            context_window_tokens=model.context_window_tokens,
            max_output_tokens=model.max_output_tokens,
            input_price_per_million=model.input_price_per_million,
            output_price_per_million=model.output_price_per_million,
            cache_read_price_per_million=model.cache_read_price_per_million,
            cache_write_price_per_million=model.cache_write_price_per_million,
            thinking_efforts=normalize_thinking_efforts(model.thinking_efforts_json),
            sort_order=model.sort_order,
            created_at=datetime.fromisoformat(model.created_at),
            updated_at=datetime.fromisoformat(model.updated_at),
        )
