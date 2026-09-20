"""Runtime configuration, driven entirely by environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """Connection and tuning settings for the CDC pipeline."""

    bootstrap_servers: str = os.environ.get("CDC_BOOTSTRAP_SERVERS", "localhost:9092")
    connect_url: str = os.environ.get("CDC_CONNECT_URL", "http://localhost:8083")
    topic_prefix: str = os.environ.get("CDC_TOPIC_PREFIX", "shopdb")
    tables: tuple[str, ...] = tuple(
        table.strip()
        for table in os.environ.get("CDC_TABLES", "customers,orders,order_items").split(",")
        if table.strip()
    )
    dlq_topic: str = os.environ.get("CDC_DLQ_TOPIC", "cdc.dlq")
    consumer_group: str = os.environ.get("CDC_CONSUMER_GROUP", "cdc-clickhouse-sink")
    auto_offset_reset: str = os.environ.get("CDC_AUTO_OFFSET_RESET", "earliest")
    batch_size: int = int(os.environ.get("CDC_BATCH_SIZE", "200"))
    flush_interval_seconds: float = float(os.environ.get("CDC_FLUSH_INTERVAL", "5"))
    poll_timeout_seconds: float = float(os.environ.get("CDC_POLL_TIMEOUT", "1.0"))
    metrics_interval_seconds: float = float(os.environ.get("CDC_METRICS_INTERVAL", "10"))
    idle_timeout_seconds: float = float(os.environ.get("CDC_IDLE_TIMEOUT", "15"))

    clickhouse_host: str = os.environ.get("CLICKHOUSE_HOST", "localhost")
    clickhouse_port: int = int(os.environ.get("CLICKHOUSE_PORT", "8124"))
    clickhouse_user: str = os.environ.get("CLICKHOUSE_USER", "cdc")
    clickhouse_password: str = os.environ.get("CLICKHOUSE_PASSWORD", "cdc_pw")
    clickhouse_database: str = os.environ.get("CLICKHOUSE_DATABASE", "cdc")

    data_dir: str = os.environ.get("CDC_DATA_DIR", "data")

    @property
    def source_topics(self) -> tuple[str, ...]:
        return tuple(f"{self.topic_prefix}.public.{table}" for table in self.tables)

    @property
    def dlq_topic_name(self) -> str:
        return self.dlq_topic


def get_settings(**overrides: object) -> Settings:
    """Build settings, applying keyword overrides on top of the defaults."""

    if not overrides:
        return Settings()
    return Settings(**{**Settings().__dict__, **overrides})
