import pytest

from cdc_pipeline.events import ChangeEvent
from cdc_pipeline.transforms import UnknownEntityError, to_iso_timestamp, to_row

TS_MS = 1_700_000_000_000


def make_event(op: str, table: str, before=None, after=None, ts_ms: int = TS_MS) -> ChangeEvent:
    return ChangeEvent(
        op=op,
        table=table,
        topic=f"shopdb.public.{table}",
        source_ts_ms=ts_ms,
        before=before,
        after=after,
        key=None,
    )


def test_to_row_for_customers():
    event = make_event(
        "c",
        "customers",
        after={
            "customer_id": 1,
            "name": "Ada Lovelace",
            "email": "ada@example.com",
            "city": "Jakarta",
            "updated_at": 1_700_000_000_000_000,
        },
    )

    row = to_row(event)

    assert row["customer_id"] == 1
    assert row["city"] == "Jakarta"
    assert row["op"] == "c"
    assert row["is_deleted"] == 0
    assert row["_version"] == TS_MS
    assert row["source_ts_ms"] == TS_MS
    assert row["updated_at"].startswith("2023-11-14T")


def test_to_row_formats_order_money_and_ints():
    event = make_event(
        "u",
        "orders",
        before={"order_id": 10, "customer_id": 1, "status": "pending", "amount": 250000.5},
        after={"order_id": 10, "customer_id": 1, "status": "paid", "amount": 250000.5},
    )

    row = to_row(event)

    assert row["order_id"] == 10
    assert row["customer_id"] == 1
    assert row["status"] == "paid"
    assert row["amount"] == "250000.50"
    assert row["is_deleted"] == 0


def test_to_row_marks_delete_and_uses_before_image():
    event = make_event(
        "d",
        "order_items",
        before={"order_item_id": 12, "order_id": 10, "sku": "SKU-LATTE", "quantity": 2,
                "unit_price": 87500.0},
    )

    row = to_row(event)

    assert row["order_item_id"] == 12
    assert row["sku"] == "SKU-LATTE"
    assert row["quantity"] == 2
    assert row["is_deleted"] == 1
    assert row["op"] == "d"


def test_to_row_rejects_unconfigured_table():
    event = make_event("c", "unknown_table", after={"id": 1})

    with pytest.raises(UnknownEntityError):
        to_row(event)


def test_to_row_rejects_event_without_row_image():
    event = make_event("d", "customers", before=None, after=None)

    with pytest.raises(UnknownEntityError):
        to_row(event)


def test_timestamp_normalisation_handles_micros_millis_seconds_and_iso():
    micros = to_iso_timestamp(1_700_000_000_000_000)
    millis = to_iso_timestamp(1_700_000_000_000)
    seconds = to_iso_timestamp(1_700_000_000)
    iso = to_iso_timestamp("2026-09-18T00:00:00+00:00")

    assert micros == millis == seconds == "2023-11-14T22:13:20+00:00"
    assert iso == "2026-09-18T00:00:00+00:00"


def test_timestamp_normalisation_falls_back_to_now():
    assert to_iso_timestamp(None)
    assert to_iso_timestamp("")
