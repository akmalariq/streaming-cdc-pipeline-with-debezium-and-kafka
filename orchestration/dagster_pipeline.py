"""Dagster orchestration for the CDC consumer.

Install the optional dependency before running:

    uv sync --extra dagster
    uv run dagster dev -f orchestration/dagster_pipeline.py
"""

from __future__ import annotations

from pathlib import Path

from dagster import (
    Definitions,
    MaterializeResult,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from cdc_pipeline.config import get_settings
from cdc_pipeline.consumer import CDCConsumer
from cdc_pipeline.sink import ClickHouseSink
from cdc_pipeline.transforms import SPECS

DRAIN_MESSAGES = 200


@asset(retry_policy=RetryPolicy(max_retries=3, delay=10))
def cdc_batch_drain() -> MaterializeResult:
    """Drain a bounded batch of change events into ClickHouse.

    The consumer normally runs as a long-lived process. This asset runs a bounded
    drain so orchestration owns the retry policy, alerting, and run metadata.
    """

    settings = get_settings()
    sink = ClickHouseSink(settings)
    consumer = CDCConsumer(settings, sink)
    metrics = consumer.run(max_messages=DRAIN_MESSAGES)
    report_path = metrics.write(Path(settings.data_dir))
    return MaterializeResult(
        metadata={
            "run_id": metrics.run_id,
            "processed": metrics.processed,
            "written": metrics.written,
            "decode_errors": metrics.decode_errors,
            "dlq_routed": metrics.dlq_routed,
            "max_lag": metrics.max_lag,
            "report": str(report_path),
        }
    )


@asset
def cdc_sink_health() -> MaterializeResult:
    """Report sink row counts so a dashboard can alert on stalls or drift."""

    settings = get_settings()
    sink = ClickHouseSink(settings)
    counts = {spec.target: sink.count(spec.target) for spec in SPECS.values()}
    active = {spec.target: sink.active_count(spec.target) for spec in SPECS.values()}
    return MaterializeResult(metadata={"rows": counts, "active_rows": active})


cdc_job = define_asset_job("cdc_job", selection=[cdc_batch_drain, cdc_sink_health])

cdc_schedule = ScheduleDefinition(
    job=cdc_job,
    cron_schedule="*/15 * * * *",
    execution_timezone="Asia/Jakarta",
)

defs = Definitions(
    assets=[cdc_batch_drain, cdc_sink_health],
    jobs=[cdc_job],
    schedules=[cdc_schedule],
)
