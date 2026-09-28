"""Parses and validates real Debezium Kafka messages, per the structure
independently inspected against actual messages in S11/S12 (docs/24 §13,
docs/25 §5) — never assumed from Debezium's documentation alone.

Real, verified structure:
    key   = {"schema": {...}, "payload": {<primary key column(s)>}}  (or None for a tombstone)
    value = {"schema": {...}, "payload": {before, after, source, op, ts_ms, transaction}}
            (or None for a tombstone — Kafka's null-value convention, distinct
            from a genuine Debezium "d" delete event, which has a populated
            value with op="d" and before set)

VALID_OPS matches exactly the four real Debezium PostgreSQL connector codes
demonstrated in S11 (r/c/u/d) — nothing invented. Tombstones are handled as a
distinct, explicit third case (see ParsedEvent.is_tombstone), never folded
into "d" and never treated as a normal business event.
"""

import json
from dataclasses import dataclass
from typing import Any

VALID_OPS = {"r", "c", "u", "d"}
TOMBSTONE_OP = "t"  # NOT a real Debezium code — assigned by this consumer only
# to give tombstones a value in the same `operation` column as real events,
# documented explicitly as synthetic (docs/25 §6), never confused with "d".


class EnvelopeValidationError(ValueError):
    pass


@dataclass
class ParsedEvent:
    is_tombstone: bool
    event_key_json: str | None  # the raw key payload, re-serialized verbatim (§8: preserve raw)
    operation: str
    before_json: str | None
    after_json: str | None
    source_connector: str | None
    source_name: str | None
    source_schema: str | None
    source_table: str | None
    source_database: str | None
    source_lsn: int | None
    source_timestamp_ms: int | None
    snapshot_indicator: str | None
    transaction_id: str | None
    transaction_total_order: int | None
    transaction_data_collection_order: int | None


def _key_payload_json(raw_key_bytes: bytes | None) -> str | None:
    if raw_key_bytes is None:
        return None
    key_envelope = json.loads(raw_key_bytes)
    payload = key_envelope.get("payload") if isinstance(key_envelope, dict) else key_envelope
    return json.dumps(payload, sort_keys=True)


def parse_message(raw_key_bytes: bytes | None, raw_value_bytes: bytes | None) -> ParsedEvent:
    """Raises EnvelopeValidationError for anything that isn't a valid
    Debezium envelope or a genuine tombstone — callers (src/cdc/consumer.py)
    route that to quarantine, never silently drop it."""
    event_key_json = _key_payload_json(raw_key_bytes)

    if raw_value_bytes is None:
        # A genuine Kafka tombstone: key present (or None), value entirely
        # absent. Distinct from a Debezium "d" event, which has op="d" and a
        # populated `before` in its (non-null) value.
        return ParsedEvent(
            is_tombstone=True,
            event_key_json=event_key_json,
            operation=TOMBSTONE_OP,
            before_json=None,
            after_json=None,
            source_connector=None,
            source_name=None,
            source_schema=None,
            source_table=None,
            source_database=None,
            source_lsn=None,
            source_timestamp_ms=None,
            snapshot_indicator=None,
            transaction_id=None,
            transaction_total_order=None,
            transaction_data_collection_order=None,
        )

    try:
        value_envelope = json.loads(raw_value_bytes)
    except json.JSONDecodeError as exc:
        raise EnvelopeValidationError(f"value is not valid JSON: {exc}") from exc

    if not isinstance(value_envelope, dict) or "payload" not in value_envelope:
        raise EnvelopeValidationError("value JSON has no top-level 'payload' — not a Debezium envelope")

    payload = value_envelope["payload"]
    if not isinstance(payload, dict):
        raise EnvelopeValidationError("value 'payload' is not an object")

    op = payload.get("op")
    if op is None:
        raise EnvelopeValidationError("payload missing required field 'op'")
    if op not in VALID_OPS:
        raise EnvelopeValidationError(f"unknown Debezium operation code: {op!r} (expected one of {sorted(VALID_OPS)})")

    source = payload.get("source") or {}
    transaction = payload.get("transaction") or {}

    return ParsedEvent(
        is_tombstone=False,
        event_key_json=event_key_json,
        operation=op,
        before_json=json.dumps(payload.get("before"), sort_keys=True) if payload.get("before") is not None else None,
        after_json=json.dumps(payload.get("after"), sort_keys=True) if payload.get("after") is not None else None,
        source_connector=source.get("connector"),
        source_name=source.get("name"),
        source_schema=source.get("schema"),
        source_table=source.get("table"),
        source_database=source.get("db"),
        source_lsn=source.get("lsn"),
        source_timestamp_ms=source.get("ts_ms"),
        snapshot_indicator=source.get("snapshot") if source.get("snapshot") not in (None, "false") else None,
        transaction_id=transaction.get("id"),
        transaction_total_order=transaction.get("total_order"),
        transaction_data_collection_order=transaction.get("data_collection_order"),
    )
