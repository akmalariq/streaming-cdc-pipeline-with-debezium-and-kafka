"""Emit Debezium-shaped change events, for demos, load tests, and offline verification.

Real change events come from the Debezium Postgres connector. This simulator produces
the same envelope shape so the pipeline can be exercised without a source database,
and so tests can run without a broker.
"""

from __future__ import annotations

import json
import random
import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from cdc_pipeline.config import Settings

CITIES = ("Jakarta", "Bandung", "Surabaya", "Medan", "Makassar", "Semarang")
ORDER_STATUSES = ("pending", "paid", "shipped", "delivered", "cancelled")
SKUS = ("SKU-ESPRESSO", "SKU-LATTE", "SKU-COLD-BREW", "SKU-MATCHA", "SKU-TEA")


@dataclass
class SimulatedEvent:
    """One generated change, ready to serialise into a Debezium envelope."""

    table: str
    op: str
    key: dict
    before: dict | None
    after: dict | None
    ts_ms: int

    @property
    def topic(self) -> str:
        return f"shopdb.public.{self.table}"

    def envelope(self) -> dict:
        return {
            "before": self.before,
            "after": self.after,
            "source": {
                "db": "shopdb",
                "schema": "public",
                "table": self.table,
                "ts_ms": self.ts_ms,
            },
            "op": self.op,
            "ts_ms": self.ts_ms,
        }


class ChangeEventSimulator:
    """Generate a realistic create/update/delete stream across the three source tables."""

    def __init__(self, seed: int | None = None, start_ts_ms: int | None = None) -> None:
        self._random = random.Random(seed)
        self._ts = start_ts_ms if start_ts_ms is not None else int(time.time() * 1000)
        self.customers: dict[int, dict] = {}
        self.orders: dict[int, dict] = {}
        self.order_items: dict[int, dict] = {}
        self._next_customer = 1
        self._next_order = 1001
        self._next_item = 9001

    def _tick(self) -> int:
        self._ts += self._random.randint(1, 50)
        return self._ts

    def _stamp(self) -> int:
        return self._tick() * 1000

    def create_customer(self) -> SimulatedEvent:
        customer_id = self._next_customer
        self._next_customer += 1
        after = {
            "customer_id": customer_id,
            "name": f"Customer {customer_id}",
            "email": f"customer{customer_id}@example.com",
            "city": self._random.choice(CITIES),
            "updated_at": self._stamp(),
        }
        self.customers[customer_id] = after
        return SimulatedEvent(
            "customers", "c", {"customer_id": customer_id}, None, after, self._tick()
        )

    def create_order(self) -> SimulatedEvent:
        if not self.customers:
            return self.create_customer()
        order_id = self._next_order
        self._next_order += 1
        after = {
            "order_id": order_id,
            "customer_id": self._random.choice(list(self.customers)),
            "status": "pending",
            "amount": round(self._random.uniform(25_000, 750_000), 2),
            "updated_at": self._stamp(),
        }
        self.orders[order_id] = after
        return SimulatedEvent("orders", "c", {"order_id": order_id}, None, after, self._tick())

    def create_order_item(self) -> SimulatedEvent:
        if not self.orders:
            return self.create_order()
        item_id = self._next_item
        self._next_item += 1
        after = {
            "order_item_id": item_id,
            "order_id": self._random.choice(list(self.orders)),
            "sku": self._random.choice(SKUS),
            "quantity": self._random.randint(1, 5),
            "unit_price": round(self._random.uniform(15_000, 120_000), 2),
            "updated_at": self._stamp(),
        }
        self.order_items[item_id] = after
        return SimulatedEvent(
            "order_items", "c", {"order_item_id": item_id}, None, after, self._tick()
        )

    def update_entity(self) -> SimulatedEvent:
        choices: list[tuple[str, dict, str]] = []
        if self.customers:
            choices.append(("customers", self.customers, "city"))
        if self.orders:
            choices.append(("orders", self.orders, "status"))
        if self.order_items:
            choices.append(("order_items", self.order_items, "quantity"))
        if not choices:
            return self.create_customer()

        table, store, field = self._random.choice(choices)
        key_value = self._random.choice(list(store))
        before = dict(store[key_value])
        after = dict(before)
        if field == "city":
            after["city"] = self._random.choice(CITIES)
        elif field == "status":
            after["status"] = self._random.choice(ORDER_STATUSES)
        else:
            after["quantity"] = self._random.randint(1, 9)
        after["updated_at"] = self._stamp()
        store[key_value] = after
        key_name = {
            "customers": "customer_id",
            "orders": "order_id",
            "order_items": "order_item_id",
        }[table]
        return SimulatedEvent(
            table, "u", {key_name: key_value}, before, after, self._tick()
        )

    def delete_entity(self) -> SimulatedEvent:
        choices: list[tuple[str, dict, str]] = []
        if self.order_items:
            choices.append(("order_items", self.order_items, "order_item_id"))
        if self.orders:
            choices.append(("orders", self.orders, "order_id"))
        if self.customers:
            choices.append(("customers", self.customers, "customer_id"))
        if not choices:
            return self.create_customer()

        table, store, key_name = self._random.choice(choices)
        key_value = self._random.choice(list(store))
        before = store.pop(key_value)
        return SimulatedEvent(table, "d", {key_name: key_value}, before, None, self._tick())

    def next_event(self) -> SimulatedEvent:
        """Produce the next event, weighted toward inserts with regular updates and deletes."""

        roll = self._random.random()
        if roll < 0.45:
            return self._random.choice(
                (self.create_customer, self.create_order, self.create_order_item)
            )()
        if roll < 0.8:
            return self.update_entity()
        return self.delete_entity()

    def events(self, count: int) -> Iterator[SimulatedEvent]:
        for _ in range(count):
            yield self.next_event()


def generate_events(
    count: int, seed: int | None = None, start_ts_ms: int | None = None
) -> list[SimulatedEvent]:
    """Convenience wrapper returning a fixed list of events (used by tests)."""

    simulator = ChangeEventSimulator(seed=seed, start_ts_ms=start_ts_ms)
    return list(simulator.events(count))


def simulate(
    settings: Settings,
    count: int = 25,
    seed: int | None = None,
    producer: Any = None,
) -> int:
    """Produce ``count`` simulated change events into the configured Kafka topics."""

    if producer is None:
        from confluent_kafka import Producer

        producer = Producer({"bootstrap.servers": settings.bootstrap_servers})

    simulator = ChangeEventSimulator(seed=seed)
    produced = 0
    for event in simulator.events(count):
        producer.produce(
            f"{settings.topic_prefix}.public.{event.table}",
            key=json.dumps(event.key).encode("utf-8"),
            value=json.dumps(event.envelope()).encode("utf-8"),
        )
        produced += 1

    flush = getattr(producer, "flush", None)
    if callable(flush):
        flush(10)
    return produced
