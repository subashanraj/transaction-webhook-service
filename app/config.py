from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All runtime configuration, read from environment variables / .env."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://postgres@localhost:5432/postgres"
    # asyncpg prepared-statement cache; must be 0 behind pgbouncer (Supabase port 6543)
    db_statement_cache_size: int = 100
    db_api_pool_size: int = 10
    db_worker_pool_size: int = 3
    db_write_timeout_ms: int = 400
    auto_migrate: bool = True

    processing_delay_seconds: float = 30.0
    worker_concurrency: int = 100
    poll_interval_seconds: float = 1.0
    lease_seconds: float = 90.0
    max_attempts: int = 5
    retry_base_seconds: float = 2.0
    retry_max_seconds: float = 300.0
    simulated_failure_rate: float = 0.0

    webhook_secret: str = ""
    max_body_bytes: int = 64 * 1024
    log_level: str = "INFO"

    @model_validator(mode="after")
    def _check(self) -> "Settings":
        # A lease shorter than the processing time would let a second worker
        # steal a job that is still running.
        if self.lease_seconds < self.processing_delay_seconds + 5:
            raise ValueError("LEASE_SECONDS must be at least PROCESSING_DELAY_SECONDS + 5")
        if not 0.0 <= self.simulated_failure_rate <= 1.0:
            raise ValueError("SIMULATED_FAILURE_RATE must be between 0 and 1")
        if self.worker_concurrency < 1 or self.max_attempts < 1:
            raise ValueError("WORKER_CONCURRENCY and MAX_ATTEMPTS must be >= 1")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
