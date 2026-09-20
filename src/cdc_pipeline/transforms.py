"""Map Debezium change events onto ClickHouse sink rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from cdc_pipeline.events import ChangeEvent


class UnknownEntityError(ValueError):
    """Raised when an event refers to a table with no configured sink."""


@dataclass(frozen=True)
class EntitySpec:
    """Describes how one source table lands in ClickHouse."""

    table: str
    target: str
    primary_key: str
    columns: tuple[str, ...]
    money_columns: tuple[str, ...] = ()
    int_columns: tuple[str, ...] = ()


SPECS: dict[str, EntitySpec] = {
    "customers": EntitySpec(
        table="customers",
        target="cdc.customers",
        primary_key="customer_id",
        columns=("customer_id", "name", "email", "city", "updated_at"),
        int_columns=("customer_id",),
    ),
    "orders": EntitySpec(
        table="orders",
        target="cdc.orders",
        primary_key="order_id",
        columns=("order_id", "customer_id", "status", "amount", "updated_at"),
        money_columns=("amount",),
        int_columns=("order_id", "customer_id"),
    ),
    "order_items": EntitySpec(
        table="order_items",
        target="cdc.order_items",
        primary_key="order_item_id",
        columns=("order_item_id", "order_id", "sku", "quantity", "unit_price", "updated_at"),
        money_columns=("unit_price",),
        int_columns=("order_item_id", "order_id", "quantity"),
    ),
}


def to_iso_timestamp(value: object, fallback: datetime | None = None) -> str:
    """Normalise Debezium timestamps (micros, millis, seconds, or ISO) to ISO-8601."""

    if value is None or value == "":
        moment = fallback or datetime.now(UTC)
        return moment.isoformat()

    if isinstance(value, str):
        return value

    if isinstance(value, (int, float)):
        magnitude = abs(float(value))
        if magnitude > 1e14:
            seconds = float(value) / 1_000_000
        elif magnitude > 1e11:
            seconds = float(value) / 1_000
        else:
            seconds = float(value)
        return datetime.fromtimestamp(seconds, UTC).isoformat()

    return str(value)


def _format_money(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, (int, float, str)):
        return str(value)
    return f"{float(value):.2f}"


def to_row(event: ChangeEvent) -> dict:
    """Convert a change event into a row shaped for the ClickHouse sink."""

    spec = SPECS.get(event.table)
    if spec is None:
        raise UnknownEntityError(f"no sink configured for table {event.table!r}")

    payload = event.before if event.is_delete else event.after
    if payload is None:
        raise UnknownEntityError(f"{event.table!r} event has no row image to write")

    row: dict = {}
    for column in spec.columns:
        value = payload.get(column)
        if column in spec.int_columns and value is not None:
            value = int(value)
        elif column in spec.money_columns:
            value = _format_money(value)
        elif column == "updated_at":
            value = to_iso_timestamp(value)
        row[column] = value

    row["op"] = event.op
    row["source_ts_ms"] = event.source_ts_ms
    row["is_deleted"] = 1 if event.is_delete else 0
    row["_version"] = event.source_ts_ms
    return row
