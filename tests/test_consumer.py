import io
import json

from cdc_pipeline.config import Settings
from cdc_pipeline.consumer import CDCConsumer
from cdc_pipeline.monitoring import JsonLogger

TS_MS = 1_700_000_000_000


def make_settings(**overrides) -> Settings:
    base = {
        "bootstrap_servers": "ignored:9092",
        "batch_size": 10,
        "flush_interval_seconds": 999,
        "poll_timeout_seconds": 0.0,
        "metrics_interval_seconds": 999,
    }
    base.update(overrides)
    return Settings(**base)


def silent_logger() -> JsonLogger:
    return JsonLogger("test-run", stream=io.StringIO())


def create_payload(table: str = "customers", key_value: int = 1, ts_ms: int = TS_MS) -> bytes:
    row = {
        "customers": {"customer_id": key_value, "name": "Ada", "email": "a@example.com",
                      "city": "Jakarta", "updated_at": ts_ms * 1000},
        "orders": {"order_id": key_value, "customer_id": 1, "status": "paid",
                   "amount": 100.0, "updated_at": ts_ms * 1000},
        "order_items": {"order_item_id": key_value, "order_id": 1, "sku": "SKU-LATTE",
                        "quantity": 1, "unit_price": 10.0, "updated_at": ts_ms * 1000},
    }[table]
    envelope = {
        "before": None,
        "after": row,
        "source": {"db": "shopdb", "schema": "public", "table": table, "ts_ms": ts_ms},
        "op": "c",
        "ts_ms": ts_ms,
    }
    return json.dumps(envelope).encode("utf-8")


class FakeMessage:
    def __init__(self, topic: str, value: bytes, key: bytes = b'{"customer_id":1}') -> None:
        self._topic = topic
        self._value = value
        self._key = key

    def topic(self) -> str:
        return self._topic

    def value(self) -> bytes:
        return self._value

    def key(self) -> bytes:
        return self._key

    def error(self):
        return None

    def partition(self) -> int:
        return 0


class FakeSink:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.writes: list[tuple[str, list[dict]]] = []

    def write(self, target: str, rows) -> int:
        self.events.append("write")
        self.writes.append((target, list(rows)))
        return len(rows)


class FakeConsumer:
    def __init__(self, messages: list[FakeMessage], events: list[str]) -> None:
        self._messages = list(messages)
        self.events = events
        self.commits = 0
        self.closed = False

    def poll(self, timeout: float):
        return self._messages.pop(0) if self._messages else None

    def commit(self, asynchronous: bool = False) -> None:
        self.commits += 1
        self.events.append("commit")

    def close(self) -> None:
        self.closed = True

    def assignment(self) -> list:
        return []


class FakeProducer:
    def __init__(self) -> None:
        self.produced: list[dict] = []

    def produce(self, topic: str, key=None, value=None, headers=None) -> None:
        self.produced.append({"topic": topic, "key": key, "value": value, "headers": headers})

    def flush(self, timeout=None) -> int:
        return 0


def test_handle_message_buffers_transformed_row():
    events: list[str] = []
    consumer = CDCConsumer(
        make_settings(),
        FakeSink(events),
        consumer=FakeConsumer([], events),
        logger=silent_logger(),
    )

    consumer.handle_message(FakeMessage("shopdb.public.customers", create_payload()))

    assert consumer.buffered() == 1
    buffered_row = consumer.buffers["cdc.customers"][0]
    assert buffered_row["op"] == "c"
    assert buffered_row["_version"] == TS_MS


def test_flush_writes_before_committing_offsets():
    events: list[str] = []
    sink = FakeSink(events)
    kafka_consumer = FakeConsumer([], events)
    consumer = CDCConsumer(
        make_settings(), sink, consumer=kafka_consumer, logger=silent_logger()
    )
    consumer.handle_message(FakeMessage("shopdb.public.customers", create_payload()))

    written = consumer.flush()

    assert written == 1
    assert events == ["write", "commit"]
    assert kafka_consumer.commits == 1
    assert sink.writes[0][0] == "cdc.customers"
    assert consumer.buffered() == 0


def test_decode_failure_routes_message_to_dlq():
    events: list[str] = []
    dlq = FakeProducer()
    consumer = CDCConsumer(
        make_settings(),
        FakeSink(events),
        consumer=FakeConsumer([], events),
        dlq_producer=dlq,
        logger=silent_logger(),
    )

    consumer.handle_message(FakeMessage("shopdb.public.customers", b"not-json"))

    assert consumer.metrics.decode_errors == 1
    assert consumer.metrics.dlq_routed == 1
    assert consumer.buffered() == 0
    assert dlq.produced[0]["topic"] == "cdc.dlq"
    assert dlq.produced[0]["headers"]["x-origin-topic"] == "shopdb.public.customers"


def test_run_processes_bounded_messages_and_drains():
    events: list[str] = []
    messages = [
        FakeMessage("shopdb.public.customers", create_payload(key_value=1)),
        FakeMessage("shopdb.public.orders", create_payload("orders", key_value=10)),
        FakeMessage("shopdb.public.order_items", create_payload("order_items", key_value=90)),
    ]
    sink = FakeSink(events)
    kafka_consumer = FakeConsumer(messages, events)
    consumer = CDCConsumer(
        make_settings(batch_size=1), sink, consumer=kafka_consumer, logger=silent_logger()
    )

    metrics = consumer.run(max_messages=3)

    assert metrics.processed == 3
    assert metrics.written == 3
    assert metrics.decode_errors == 0
    assert kafka_consumer.closed is True
    assert {target for target, _ in sink.writes} == {
        "cdc.customers",
        "cdc.orders",
        "cdc.order_items",
    }


def test_run_flushes_remaining_buffer_on_exit():
    events: list[str] = []
    messages = [
        FakeMessage("shopdb.public.customers", create_payload(key_value=1)),
        FakeMessage("shopdb.public.customers", create_payload(key_value=2)),
    ]
    sink = FakeSink(events)
    consumer = CDCConsumer(
        make_settings(batch_size=100),
        sink,
        consumer=FakeConsumer(messages, events),
        logger=silent_logger(),
    )

    metrics = consumer.run(max_messages=2)

    assert metrics.processed == 2
    assert metrics.written == 2
    assert consumer.buffered() == 0


class FakeClock:
    """Monotonic clock that advances one second per read."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        self.now += 1.0
        return self.now


def test_bounded_run_stops_after_idle_timeout():
    events: list[str] = []
    consumer = CDCConsumer(
        make_settings(idle_timeout_seconds=5),
        FakeSink(events),
        consumer=FakeConsumer([], events),
        logger=silent_logger(),
        clock=FakeClock(),
    )

    metrics = consumer.run(max_messages=50)

    assert metrics.processed == 0
    assert metrics.written == 0
