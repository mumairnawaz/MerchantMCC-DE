"""S12 — unit tests for src/cdc/envelope.py. Pure parsing logic, no Kafka
needed. Fixtures mirror the real, independently-inspected Debezium message
shapes (docs/25 §5), not assumptions.
"""

import json

import pytest

from src.cdc.envelope import TOMBSTONE_OP, VALID_OPS, EnvelopeValidationError, parse_message

KEY_ENVELOPE = json.dumps({"schema": {"type": "struct"}, "payload": {"transaction_id": "TXN-0000001"}}).encode()


def _value_envelope(op, before=None, after=None, source_overrides=None, transaction=None):
    source = {
        "version": "3.0.0.Final", "connector": "postgresql", "name": "finpay",
        "db": "merchantmcc", "schema": "finpay", "table": "transactions",
        "txId": 831, "lsn": 43666144, "ts_ms": 1789987830220, "snapshot": "false",
    }
    if source_overrides:
        source.update(source_overrides)
    return json.dumps(
        {
            "schema": {"type": "struct"},
            "payload": {"before": before, "after": after, "source": source, "op": op, "ts_ms": 1789987830723, "transaction": transaction},
        }
    ).encode()


# ---- operation parsing ----


def test_parses_snapshot_read_event():
    parsed = parse_message(KEY_ENVELOPE, _value_envelope("r", after={"transaction_id": "TXN-1"}, source_overrides={"snapshot": "first_in_data_collection"}))
    assert parsed.operation == "r"
    assert parsed.is_tombstone is False
    assert parsed.snapshot_indicator == "first_in_data_collection"


def test_parses_insert_event():
    parsed = parse_message(KEY_ENVELOPE, _value_envelope("c", after={"amount": "42.00"}))
    assert parsed.operation == "c"
    assert parsed.before_json is None
    assert json.loads(parsed.after_json) == {"amount": "42.00"}


def test_parses_update_event_with_before_and_after():
    parsed = parse_message(KEY_ENVELOPE, _value_envelope("u", before={"amount": "42.00"}, after={"amount": "99.00"}))
    assert parsed.operation == "u"
    assert json.loads(parsed.before_json) == {"amount": "42.00"}
    assert json.loads(parsed.after_json) == {"amount": "99.00"}


def test_parses_delete_event_after_is_none():
    parsed = parse_message(KEY_ENVELOPE, _value_envelope("d", before={"amount": "99.00"}, after=None))
    assert parsed.operation == "d"
    assert parsed.after_json is None
    assert json.loads(parsed.before_json) == {"amount": "99.00"}


def test_unknown_operation_code_is_rejected():
    with pytest.raises(EnvelopeValidationError, match="unknown Debezium operation"):
        parse_message(KEY_ENVELOPE, _value_envelope("x"))


def test_valid_ops_matches_the_four_real_debezium_codes():
    assert VALID_OPS == {"r", "c", "u", "d"}


# ---- tombstone handling ----


def test_tombstone_is_value_none_not_an_op_code():
    parsed = parse_message(KEY_ENVELOPE, None)
    assert parsed.is_tombstone is True
    assert parsed.operation == TOMBSTONE_OP
    assert parsed.operation not in VALID_OPS  # never confused with a real Debezium op


def test_tombstone_preserves_the_key():
    parsed = parse_message(KEY_ENVELOPE, None)
    assert json.loads(parsed.event_key_json) == {"transaction_id": "TXN-0000001"}


def test_tombstone_has_no_source_metadata():
    parsed = parse_message(KEY_ENVELOPE, None)
    assert parsed.source_lsn is None
    assert parsed.source_table is None
    assert parsed.before_json is None
    assert parsed.after_json is None


def test_delete_event_is_never_confused_with_a_tombstone():
    delete_event = parse_message(KEY_ENVELOPE, _value_envelope("d", before={"x": 1}))
    assert delete_event.is_tombstone is False
    assert delete_event.operation == "d"


# ---- malformed / missing envelope ----


def test_invalid_json_value_is_rejected():
    with pytest.raises(EnvelopeValidationError, match="not valid JSON"):
        parse_message(KEY_ENVELOPE, b"{not json")


def test_missing_payload_key_is_rejected():
    with pytest.raises(EnvelopeValidationError, match="no top-level 'payload'"):
        parse_message(KEY_ENVELOPE, json.dumps({"schema": {}}).encode())


def test_missing_op_field_is_rejected():
    bad = json.dumps({"schema": {}, "payload": {"before": None, "after": {}, "source": {}}}).encode()
    with pytest.raises(EnvelopeValidationError, match="missing required field 'op'"):
        parse_message(KEY_ENVELOPE, bad)


def test_payload_not_an_object_is_rejected():
    bad = json.dumps({"schema": {}, "payload": "not-an-object"}).encode()
    with pytest.raises(EnvelopeValidationError):
        parse_message(KEY_ENVELOPE, bad)


# ---- metadata extraction ----


def test_source_metadata_extracted_correctly():
    parsed = parse_message(KEY_ENVELOPE, _value_envelope("c", after={}))
    assert parsed.source_connector == "postgresql"
    assert parsed.source_database == "merchantmcc"
    assert parsed.source_schema == "finpay"
    assert parsed.source_table == "transactions"
    assert parsed.source_lsn == 43666144
    assert parsed.source_timestamp_ms == 1789987830220


def test_missing_transaction_metadata_becomes_none_not_invented():
    parsed = parse_message(KEY_ENVELOPE, _value_envelope("c", after={}, transaction=None))
    assert parsed.transaction_id is None
    assert parsed.transaction_total_order is None
    assert parsed.transaction_data_collection_order is None


def test_real_snapshot_false_string_normalizes_to_none():
    # Real Debezium sends the literal string "false" (not JSON false) for a
    # non-snapshot event's source.snapshot — normalized to None here so
    # "no snapshot" is represented consistently as NULL, not the string "false".
    parsed = parse_message(KEY_ENVELOPE, _value_envelope("c", after={}, source_overrides={"snapshot": "false"}))
    assert parsed.snapshot_indicator is None


# ---- raw payload preservation (§8: never flatten into typed columns) ----


def test_raw_payload_preserved_verbatim_not_flattened():
    nested_payload = {"transaction_id": "TXN-1", "amount": "42.00", "nested": {"x": 1}}
    parsed = parse_message(KEY_ENVELOPE, _value_envelope("c", after=nested_payload))
    assert json.loads(parsed.after_json) == nested_payload  # exact round-trip, no column extraction
