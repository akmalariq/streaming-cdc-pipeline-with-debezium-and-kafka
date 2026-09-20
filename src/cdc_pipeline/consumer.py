"""Kafka consumer that decodes change events and lands them in ClickHouse."""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Any, Protocol

from cdc_pipeline.config import Settings
from cdc_pipeline.events import DecodeError, decode_debezium
from cdc_pipeline.monitoring import JsonLogger, RunMetrics, new_run_id
from cdc_pipeline.transforms import SPECS, UnknownEntityError, to_row


class Sink(Protocol):
    def write(self, target: str, rows: Sequence[dict]) -> int: ...


class CDCConsumer:
    """Consume change events, write in batches, and commit only after a write.

    Ordering of guarantees: the sink write happens before the offset commit, so a
    crash between the two replays at most one batch. Replays are harmless because the
    ClickHouse sink merges by version, which is what makes this effectively-once.
    """

    def __init__(
        self,
        settings: Settings,
        sink: Sink,
        consumer: Any | None = None,
        dlq_producer: Any | None = None,
        logger: JsonLogger | None = None,
        clock=time.monotonic,
    ) -> None:
        self.settings = settings
        self.sink = sink
        self._consumer = consumer
        self._dlq_producer = dlq_producer
        self._clock = clock
        self.run_id = new_run_id()
        self.log = logger or JsonLogger(self.run_id)
        self.buffers: dict[str, list[dict]] = {spec.target: [] for spec in SPECS.values()}
        self.metrics = RunMetrics(run_id=self.run_id, started_at=_now_iso())
        self._last_flush = clock()

    def _ensure_consumer(self) -> Any:
        if self._consumer is None:
            from confluent_kafka import Consumer

            self._consumer = Consumer(
                {
                    "bootstrap.servers": self.settings.bootstrap_servers,
                    "group.id": self.settings.consumer_group,
                    "auto.offset.reset": self.settings.auto_offset_reset,
                    "enable.auto.commit": False,
                }
            )
            self._consumer.subscribe(list(self.settings.source_topics))
            self.log.info("subscribed", topics=list(self.settings.source_topics))
        return self._consumer

    def _ensure_dlq(self) -> Any:
        if self._dlq_producer is None:
            from confluent_kafka import Producer

            self._dlq_producer = Producer(
                {"bootstrap.servers": self.settings.bootstrap_servers}
            )
        return self._dlq_producer

    def buffered(self) -> int:
        return sum(len(rows) for rows in self.buffers.values())

    def flush(self) -> int:
        """Write every buffered batch and commit offsets afterward."""

        written = 0
        for target, rows in self.buffers.items():
            if not rows:
                continue
            written += self.sink.write(target, rows)
            rows.clear()

        if written:
            consumer = self._ensure_consumer()
            commit = getattr(consumer, "commit", None)
            if callable(commit):
                commit(asynchronous=False)
                self.metrics.committed += 1
            self.log.info("batch_flushed", written=written, buffered=self.buffered())

        self._last_flush = self._clock()
        self.metrics.written += written
        return written

    def handle_message(self, message: Any) -> None:
        topic = message.topic()
        self.metrics.topic = topic
        self.metrics.partition = getattr(message, "partition", lambda: -1)()

        if getattr(message, "error", None) and message.error():
            self.log.warning("kafka_error", error=str(message.error()))
            return

        self.metrics.processed += 1

        try:
            event = decode_debezium(message.value(), topic, message.key())
            row = to_row(event)
        except (DecodeError, UnknownEntityError) as exc:
            self.metrics.decode_errors += 1
            self.metrics.record_error(topic, str(exc))
            self.log.error("decode_failed", topic=topic, error=str(exc))
            self._route_to_dlq(message, str(exc))
            return

        self.buffers[SPECS[event.table].target].append(row)

    def _route_to_dlq(self, message: Any, reason: str) -> None:
        try:
            producer = self._ensure_dlq()
            producer.produce(
                self.settings.dlq_topic_name,
                key=message.key(),
                value=message.value(),
                headers={"x-error": reason, "x-origin-topic": message.topic()},
            )
            flush = getattr(producer, "flush", None)
            if callable(flush):
                flush(5)
            self.metrics.dlq_routed += 1
        except Exception as exc:
            self.log.error("dlq_routing_failed", error=str(exc))

    def observe_lag(self) -> None:
        """Sample consumer lag for the assigned partitions."""

        consumer = self._consumer
        if consumer is None:
            return
        try:
            assignment = consumer.assignment()
            for topic_partition in assignment:
                position = consumer.position([topic_partition])[0].offset
                low, high = consumer.get_watermark_offsets(topic_partition, cached=False)
                if high < 0 or position < 0:
                    continue
                self.metrics.observe_lag(max(0, high - position))
        except Exception:
            return

    def run(self, max_messages: int | None = None) -> RunMetrics:
        """Poll until ``max_messages`` is reached, the topic goes idle, or interrupted.

        A bounded run stops after ``max_messages`` OR after ``idle_timeout_seconds``
        with no new message, whichever comes first. Without the idle guard a bounded
        drain would poll forever whenever fewer messages exist than requested.
        """

        consumer = self._ensure_consumer()
        last_metrics = self._clock()
        self._last_message_at = self._clock()
        idle_timeout = self.settings.idle_timeout_seconds

        try:
            while True:
                if max_messages is not None and self.metrics.processed >= max_messages:
                    break

                message = consumer.poll(self.settings.poll_timeout_seconds)
                now = self._clock()

                if message is not None:
                    self._last_message_at = now
                    self.handle_message(message)
                elif max_messages is not None and (now - self._last_message_at) >= idle_timeout:
                    self.log.info(
                        "idle_timeout_reached", idle_seconds=round(now - self._last_message_at, 1)
                    )
                    break

                should_flush = self.buffered() >= self.settings.batch_size or (
                    self.buffered() > 0
                    and (now - self._last_flush) >= self.settings.flush_interval_seconds
                )
                if should_flush:
                    self.flush()

                if (now - last_metrics) >= self.settings.metrics_interval_seconds:
                    self.observe_lag()
                    last_metrics = now
        except KeyboardInterrupt:
            self.log.warning("interrupted")
        finally:
            self.flush()
            self.observe_lag()
            self.metrics.finish()
            close = getattr(consumer, "close", None)
            if callable(close):
                close()
            self.log.info(
                "consumer_stopped",
                processed=self.metrics.processed,
                written=self.metrics.written,
                decode_errors=self.metrics.decode_errors,
                dlq_routed=self.metrics.dlq_routed,
                max_lag=self.metrics.max_lag,
            )

        return self.metrics


def _now_iso() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat()
