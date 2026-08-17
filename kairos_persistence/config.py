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
    inbox_lease_s: float = Field(default=180.0, gt=0)
    outbox_poll_s: float = Field(default=0.25, gt=0)
    outbox_lease_s: float = Field(default=30.0, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)
    outbox_max_attempts: int = Field(default=20, ge=1)
    outbox_retry_base_s: float = Field(default=0.5, gt=0)
    outbox_retry_max_s: float = Field(default=60.0, gt=0)
    shutdown_timeout_s: float = Field(default=5.0, ge=0)
