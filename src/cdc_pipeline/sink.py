"""Idempotent ClickHouse sink built on ReplacingMergeTree."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import clickhouse_connect

from cdc_pipeline.config import Settings
from cdc_pipeline.transforms import SPECS

SUPPORTED_TABLES = {spec.target for spec in SPECS.values()}


class ClickHouseSink:
    """Write change rows so replays and duplicates converge to one truth.

    Every row carries a monotonic ``_version`` (the source change timestamp) and a
    soft-delete flag. ReplacingMergeTree keeps the highest ``_version`` per key, so
    re-processing the same Kafka message is harmless: at-least-once delivery plus an
    idempotent merge behaves like effectively-once.
    """

    def __init__(self, settings: Settings, client: Any | None = None) -> None:
        self.settings = settings
        self._client = client or clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )

    @property
    def client(self) -> Any:
        return self._client

    def write(self, target: str, rows: Sequence[dict]) -> int:
        """Insert rows into a target table; returns the number of rows sent."""

        if not rows:
            return 0
        if target not in SUPPORTED_TABLES:
            raise ValueError(f"unsupported sink target: {target}")

        columns = list(_column_order(target))
        payload = [[row.get(column) for column in columns] for row in rows]
        self._client.insert(target, payload, column_names=columns)
        return len(rows)

    def count(self, target: str, final: bool = True) -> int:
        """Count rows, using FINAL so pending merges are collapsed before counting."""

        suffix = " FINAL" if final else ""
        result = self._client.query(f"SELECT count(*) FROM {target}{suffix}")
        return int(result.result_rows[0][0])

    def active_count(self, target: str) -> int:
        """Count non-deleted rows after collapsing merges."""

        result = self._client.query(
            f"SELECT count(*) FROM {target} FINAL WHERE is_deleted = 0"
        )
        return int(result.result_rows[0][0])

    def optimize(self, target: str) -> None:
        self._client.command(f"OPTIMIZE TABLE {target} FINAL")

    def record_metrics(self, metrics: dict) -> None:
        self._client.insert(
            "cdc.consumer_metrics",
            [
                [
                    metrics["run_id"],
                    metrics["topic"],
                    metrics["partition"],
                    metrics["event_lag"],
                    metrics["processed"],
                    metrics["written"],
                    metrics["decode_errors"],
                    metrics["dlq_routed"],
                ]
            ],
            column_names=[
                "run_id",
                "topic",
                "partition",
                "event_lag",
                "processed",
                "written",
                "decode_errors",
                "dlq_routed",
            ],
        )


def _column_order(target: str) -> tuple[str, ...]:
    for spec in SPECS.values():
        if spec.target == target:
            return (*spec.columns, "op", "source_ts_ms", "is_deleted", "_version")
    raise ValueError(f"unsupported sink target: {target}")
