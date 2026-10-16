"""Runtime configuration."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings for flowforge.

    Defaults assume Kafka is running on localhost:9092 (which is what the
    local Kafka install gives you). DuckDB is embedded — no server needed.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="FLOWFORGE_",
        case_sensitive=False,
        extra="ignore",
    )

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_trades: str = "crypto.trades.raw"
    kafka_group_id: str = "flowforge"
    kafka_auto_offset_reset: str = "earliest"

    duckdb_path: str = Field(default="flowforge.duckdb")

    consumer_max_messages: int = Field(default=500, gt=0)
    consumer_timeout_ms: int = Field(default=8000, gt=0)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
