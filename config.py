from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    postgres_async_url: str = Field(
        default='postgresql+asyncpg://app_db:app_db@127.0.0.1:5432/app_db',
        alias='POSTGRES_ASYNC_URL',
    )
    redis_url: str = Field(
        default='redis://127.0.0.1:6379/0',
        alias='REDIS_URL',
    )


settings = Settings()