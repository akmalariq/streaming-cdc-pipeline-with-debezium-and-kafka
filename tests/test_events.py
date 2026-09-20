import json

import pytest

from cdc_pipeline.events import DecodeError, decode_debezium

TS_MS = 1_700_000_000_000


def envelope(op: str, table: str = "customers", before=None, after=None, ts_ms: int = TS_MS) -> str:
    return json.dumps(
        {
            "before": before,
            "after": after,
            "source": {"db": "shopdb", "schema": "public", "table": table, "ts_ms": ts_ms},
            "op": op,
            "ts_ms": ts_ms,
        }
    )


def test_decode_create_event():
    event = decode_debezium(
        envelope("c", after={"customer_id": 1, "city": "Jakarta"}),
        "shopdb.public.customers",
        b'{"customer_id":1}',
    )

    assert event.op == "c"
    assert event.operation == "create"
    assert event.table == "customers"
    assert event.after == {"customer_id": 1, "city": "Jakarta"}
    assert event.before is None
    assert event.key == {"customer_id": 1}
    assert event.source_ts_ms == TS_MS
    assert event.is_delete is False


def test_decode_update_event_keeps_both_images():
    event = decode_debezium(
        envelope("u", "orders", before={"order_id": 7, "status": "pending"},
                 after={"order_id": 7, "status": "paid"}),
        "shopdb.public.orders",
    )

    assert event.operation == "update"
    assert event.before == {"order_id": 7, "status": "pending"}
    assert event.after == {"order_id": 7, "status": "paid"}


def test_decode_delete_event_uses_before_image():
    event = decode_debezium(
        envelope("d", before={"order_item_id": 12}, after=None),
        "shopdb.public.order_items",
    )

    assert event.is_delete is True
    assert event.operation == "delete"
    assert event.before == {"order_item_id": 12}
    assert event.after is None


def test_decode_snapshot_read_event():
    event = decode_debezium(envelope("r", after={"customer_id": 3}), "t")

    assert event.operation == "read"
    assert event.is_delete is False


def test_decode_accepts_missing_key():
    event = decode_debezium(envelope("c", after={"customer_id": 4}), "t", key=None)

    assert event.key is None


def test_decode_accepts_empty_payload_as_error():
    with pytest.raises(DecodeError):
        decode_debezium("   ", "t")


@pytest.mark.parametrize(
    "payload",
    [
        "not json at all",
        "[]",
        json.dumps({"source": {"table": "customers"}, "op": "c"}),
        json.dumps({"op": "x", "source": {"table": "customers"}}),
        json.dumps({"op": "c", "after": {"customer_id": 1}}),
        json.dumps({"op": "c", "source": {"table": ""}, "after": {}}),
    ],
)
def test_decode_rejects_malformed_payloads(payload):
    with pytest.raises(DecodeError):
        decode_debezium(payload, "t")


def test_decode_rejects_delete_without_before_image():
    with pytest.raises(DecodeError):
        decode_debezium(envelope("d", before=None, after=None), "t")


def test_decode_rejects_non_integer_timestamp():
    payload = json.dumps(
        {
            "before": None,
            "after": {"customer_id": 1},
            "source": {"table": "customers", "ts_ms": "not-a-number"},
            "op": "c",
        }
    )

    with pytest.raises(DecodeError):
        decode_debezium(payload, "t")
