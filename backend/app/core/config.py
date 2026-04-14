from __future__ import annotations

import secrets
from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        enable_decoding=False,
        extra="ignore",
    )

    app_name: str = "Aniu"
    api_prefix: str = "/api/aniu"
    sqlite_db_path: Path = Field(
        default=Path("./data/aniu.db"), alias="SQLITE_DB_PATH"
    )

    mx_apikey: str | None = Field(default=None, alias="MX_APIKEY")
    mx_api_url: str = Field(
        default="https://mkapi2.dfcfs.com/finskillshub", alias="MX_API_URL"
    )

    openai_base_url: str | None = Field(default=None, alias="OPENAI_BASE_URL")
    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4o-mini", alias="OPENAI_MODEL")

    account_overview_cache_ttl_seconds: int = Field(
        default=30, alias="ACCOUNT_OVERVIEW_CACHE_TTL_SECONDS"
    )

    scheduler_poll_seconds: int = Field(default=15, alias="SCHEDULER_POLL_SECONDS")
    app_login_username: str | None = Field(default=None, alias="APP_LOGIN_USERNAME")
    app_login_password: str | None = Field(default=None, alias="APP_LOGIN_PASSWORD")
    jwt_secret: str = Field(
        default_factory=lambda: secrets.token_urlsafe(32), alias="JWT_SECRET"
    )
    jwt_expire_hours: int = Field(default=24, alias="JWT_EXPIRE_HOURS")
    cors_allow_origins: list[str] = Field(
        default_factory=lambda: ["*"], alias="CORS_ALLOW_ORIGINS"
    )

    @field_validator(
        "mx_apikey",
        "openai_base_url",
        "openai_api_key",
        "app_login_username",
        "app_login_password",
        mode="before",
    )
    @classmethod
    def empty_str_to_none(cls, value: object) -> str | None:
        """Normalize empty / whitespace-only env vars to None."""
        if isinstance(value, str) and not value.strip():
            return None
        return value  # type: ignore[return-value]

    @field_validator("jwt_secret", mode="before")
    @classmethod
    def ensure_jwt_secret(cls, value: object) -> str:
        """Auto-generate secret when env var is empty or missing."""
        if not value or (isinstance(value, str) and not value.strip()):
            return secrets.token_urlsafe(32)
        return str(value)

    @field_validator("cors_allow_origins", mode="before")
    @classmethod
    def parse_origins(cls, value: object) -> list[str]:
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        if isinstance(value, list):
            return [str(item) for item in value]
        return ["*"]


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    if not settings.sqlite_db_path.is_absolute():
        settings.sqlite_db_path = Path.cwd() / settings.sqlite_db_path
    return settings
