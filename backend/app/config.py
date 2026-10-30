"""Typed, environment-driven configuration (pydantic-settings)."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Persistence
    database_url: str = "postgresql+asyncpg://monitor:monitor@localhost:5433/monitor"
    sql_echo: bool = False

    # Health-check worker
    request_timeout_seconds: float = Field(default=10.0, ge=0.5)
    max_concurrent_checks: int = Field(default=50, ge=1)
    min_check_interval_seconds: int = Field(default=5, ge=1)
    max_check_interval_seconds: int = Field(default=86_400, ge=1)
    scheduler_initial_jitter_seconds: float = Field(default=5.0, ge=0)

    # Data lifecycle (raw retention must be >= 2 so "yesterday" can finish its rollup)
    raw_log_retention_days: int = Field(default=14, ge=2)
    aggregate_retention_days: int = Field(default=30, ge=1)
    maintenance_interval_seconds: int = Field(default=300, ge=1)

    # Developer experience
    seed_demo_targets: bool = True
    seed_history_days: int = Field(default=7, ge=0, le=14)
    log_level: str = "INFO"
    cors_origins: str = "http://localhost:5173,http://localhost:3000"

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
