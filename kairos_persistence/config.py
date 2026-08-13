"""Persistence settings."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PersistenceSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAIROS_PERSISTENCE_", extra="ignore")

    database_url: str = Field(
        default="postgresql://kairos:kairos@localhost:5432/kairos",
        description="PostgreSQL/TimescaleDB DSN. Override in every deployed environment.",
    )
    pool_min_size: int = Field(default=1, ge=1)
    pool_max_size: int = Field(default=10, ge=1)
    command_timeout_s: float = Field(default=30.0, gt=0)
