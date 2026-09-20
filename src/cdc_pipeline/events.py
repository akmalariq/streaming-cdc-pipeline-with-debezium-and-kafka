"""Decode Debezium change events from Kafka message payloads."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

OPS = {"c": "create", "u": "update", "d": "delete", "r": "read"}


class DecodeError(ValueError):
    """Raised when a Kafka payload is not a valid Debezium change event."""


@dataclass
class ChangeEvent:
    """A single row-level change extracted from a Debezium envelope."""

    op: str
    table: str
    topic: str
    source_ts_ms: int
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    key: dict[str, Any] | None

    @property
    def operation(self) -> str:
        return OPS.get(self.op, self.op)

    @property
    def is_delete(self) -> bool:
        return self.op == "d"


def _load_json(raw: bytes | str | None) -> Any:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if raw.strip() == "":
        return None
    return json.loads(raw)


def decode_debezium(
    payload: bytes | str | None,
    topic: str,
    key: bytes | str | None = None,
) -> ChangeEvent:
    """Parse a Debezium JSON envelope (schemas disabled) into a ChangeEvent."""

    try:
        envelope = _load_json(payload)
    except json.JSONDecodeError as exc:
        raise DecodeError(f"payload is not valid JSON: {exc}") from exc

    if not isinstance(envelope, dict):
        raise DecodeError("payload is not a JSON object")

    op = envelope.get("op")
    if op not in OPS:
        raise DecodeError(f"unsupported or missing op: {op!r}")

    source = envelope.get("source")
    if not isinstance(source, dict):
        raise DecodeError("missing source block")

    table = source.get("table")
    if not isinstance(table, str) or not table:
        raise DecodeError("missing source.table")

    ts_ms = source.get("ts_ms", envelope.get("ts_ms", 0))
    try:
        source_ts_ms = int(ts_ms)
    except (TypeError, ValueError) as exc:
        raise DecodeError(f"source.ts_ms is not an integer: {ts_ms!r}") from exc

    before = envelope.get("before")
    after = envelope.get("after")
    if op == "d" and before is None:
        raise DecodeError("delete event is missing the before image")
    if op != "d" and after is None:
        raise DecodeError(f"{op!r} event is missing the after image")

    try:
        key_data = _load_json(key)
    except json.JSONDecodeError:
        key_data = None

    return ChangeEvent(
        op=op,
        table=table,
        topic=topic,
        source_ts_ms=source_ts_ms,
        before=before if isinstance(before, dict) else None,
        after=after if isinstance(after, dict) else None,
        key=key_data if isinstance(key_data, dict) else None,
    )
