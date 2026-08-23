"""Unified encrypted secret store repository."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.infra.db.models import SecretStoreModel
from backend.infra.security import SecretCodec


class SecretStoreRepository:
    """Persistence adapter for installation-local encrypted secrets.

    Secrets are addressed by a stable namespace/owner/name triple so business
    tables can expose only non-secret configuration and last-four metadata.
    """

    def __init__(
        self,
        session: AsyncSession,
        secret_codec: SecretCodec | None = None,
    ) -> None:
        self._session = session
        self._secret_codec = secret_codec or SecretCodec()

    async def get_secret(
        self,
        namespace: str,
        owner_id: str,
        name: str,
    ) -> str | None:
        model = await self._get_model(namespace, owner_id, name)
        if model is None:
            return None
        return self._secret_codec.decrypt(model.encrypted_value)

    async def set_secret(
        self,
        namespace: str,
        owner_id: str,
        name: str,
        value: str | None,
    ) -> None:
        normalized = None if value is None else str(value).strip()
        if not normalized:
            await self.delete_secret(namespace, owner_id, name)
            return
        model = await self._get_model(namespace, owner_id, name)
        encrypted = self._secret_codec.encrypt(normalized)
        now = datetime.now(tz=UTC).isoformat()
        if model is None:
            self._session.add(
                SecretStoreModel(
                    namespace=namespace,
                    owner_id=owner_id,
                    secret_name=name,
                    encrypted_value=encrypted,
                    key_version="local-v1",
                    created_at=now,
                    updated_at=now,
                )
            )
        else:
            model.encrypted_value = encrypted
            model.key_version = "local-v1"
            model.updated_at = now
        await self._session.flush()

    async def delete_secret(
        self,
        namespace: str,
        owner_id: str,
        name: str,
    ) -> None:
        await self._session.execute(
            delete(SecretStoreModel).where(
                SecretStoreModel.namespace == namespace,
                SecretStoreModel.owner_id == owner_id,
                SecretStoreModel.secret_name == name,
            )
        )
        await self._session.flush()

    async def delete_owner(self, namespace: str, owner_id: str) -> None:
        await self._session.execute(
            delete(SecretStoreModel).where(
                SecretStoreModel.namespace == namespace,
                SecretStoreModel.owner_id == owner_id,
            )
        )
        await self._session.flush()

    async def value_map(
        self,
        namespace: str,
        owner_id: str | None = None,
        *,
        ignore_invalid: bool = True,
    ) -> dict[str, dict[str, str]]:
        statement = select(SecretStoreModel).where(
            SecretStoreModel.namespace == namespace
        )
        if owner_id is not None:
            statement = statement.where(SecretStoreModel.owner_id == owner_id)
        rows = list((await self._session.scalars(statement)).all())
        values: dict[str, dict[str, str]] = {}
        for row in rows:
            try:
                values.setdefault(row.owner_id, {})[row.secret_name] = (
                    self._secret_codec.decrypt(row.encrypted_value)
                )
            except ValueError:
                if not ignore_invalid:
                    raise
                continue
        return values

    async def rotate_all(self, *, key_version: str) -> int:
        """Re-encrypt every row with the codec's active key.

        Configure the former key in ``ANIU_MASTER_SECRET_KEY_PREVIOUS`` while
        this runs. The enclosing unit of work decides whether to commit, making
        rotation atomic with respect to the database transaction.
        """

        normalized_version = key_version.strip()
        if not normalized_version:
            raise ValueError("key_version must not be empty")
        rows = list((await self._session.scalars(select(SecretStoreModel))).all())
        now = datetime.now(tz=UTC).isoformat()
        for row in rows:
            plaintext = self._secret_codec.decrypt(row.encrypted_value)
            row.encrypted_value = self._secret_codec.encrypt(plaintext)
            row.key_version = normalized_version
            row.updated_at = now
        await self._session.flush()
        return len(rows)

    async def _get_model(
        self,
        namespace: str,
        owner_id: str,
        name: str,
    ) -> SecretStoreModel | None:
        statement = select(SecretStoreModel).where(
            SecretStoreModel.namespace == namespace,
            SecretStoreModel.owner_id == owner_id,
            SecretStoreModel.secret_name == name,
        )
        return (await self._session.scalars(statement)).first()
