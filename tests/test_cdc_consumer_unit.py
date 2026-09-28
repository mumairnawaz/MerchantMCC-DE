"""S12 — unit tests for the consumer's core per-message logic
(src/cdc/consumer.py::process_message), using fake RawMessage objects. No
live Kafka broker needed — this exercises dedup/quarantine/tombstone routing
in isolation, exactly the design reason RawMessage exists as a separate type
from confluent_kafka.Message.
"""

import json

from src.cdc.consumer import RawMessage, TopicRunResult, _table_name_from_topic, build_bronze_record, process_message
from src.cdc.envelope import parse_message

KEY = json.dumps({"schema": {}, "payload": {"transaction_id": "TXN-1"}}).encode()


def _insert_value(amount="42.00"):
    return json.dumps(
        {
            "schema": {},
            "payload": {
                "before": None,
                "after": {"transaction_id": "TXN-1", "amount": amount},
                "source": {"connector": "postgresql", "db": "merchantmcc", "schema": "finpay", "table": "transactions", "lsn": 100, "ts_ms": 1000, "snapshot": "false"},
                "op": "c",
                "ts_ms": 1001,
                "transaction": None,
            },
        }
    ).encode()


def _new_env():
    return {"checkpoints": {}, "buffers": {}, "rejections": {}, "results": {}}


def test_table_name_extracted_from_topic():
    assert _table_name_from_topic("finpay.finpay.transactions") == "transactions"
    assert _table_name_from_topic("finpay.finpay.transaction_events") == "transaction_events"


def test_valid_event_is_buffered_and_counted_as_persisted():
    env = _new_env()
    msg = RawMessage("finpay.finpay.transactions", 0, 0, KEY, _insert_value())
    process_message(msg, **env)

    result = env["results"]["transactions"]
    assert result.events_read == 1
    assert result.events_persisted == 1
    assert result.tombstones_handled == 0
    assert len(env["buffers"]["transactions"]) == 1
    assert env["buffers"]["transactions"][0]["operation"] == "c"


def test_tombstone_is_buffered_but_counted_separately_from_persisted():
    env = _new_env()
    msg = RawMessage("finpay.finpay.transactions", 0, 3, KEY, None)
    process_message(msg, **env)

    result = env["results"]["transactions"]
    assert result.events_read == 1
    assert result.events_persisted == 0
    assert result.tombstones_handled == 1
    assert env["buffers"]["transactions"][0]["operation"] == "t"


def test_malformed_envelope_is_quarantined_not_dropped():
    env = _new_env()
    msg = RawMessage("finpay.finpay.transactions", 0, 0, KEY, b"{not valid json")
    process_message(msg, **env)

    result = env["results"]["transactions"]
    assert result.quarantined == 1
    assert result.events_persisted == 0
    assert "transactions" not in env["buffers"]
    assert len(env["rejections"]["transactions"]) == 1
    rejection = env["rejections"]["transactions"][0]
    assert rejection["failing_check_name"] == "envelope_validation"
    assert rejection["source_record_identifier"] == "finpay.finpay.transactions:0:0"


def test_unknown_operation_is_quarantined():
    env = _new_env()
    bad_value = json.dumps({"schema": {}, "payload": {"before": None, "after": {}, "source": {}, "op": "x"}}).encode()
    msg = RawMessage("finpay.finpay.transactions", 0, 0, KEY, bad_value)
    process_message(msg, **env)
    assert env["results"]["transactions"].quarantined == 1


def test_duplicate_offset_below_checkpoint_is_skipped_not_reprocessed():
    env = _new_env()
    env["checkpoints"][("finpay.finpay.transactions", 0)] = 5  # already persisted up to offset 5
    msg = RawMessage("finpay.finpay.transactions", 0, 3, KEY, _insert_value())  # offset 3 <= 5
    process_message(msg, **env)

    result = env["results"]["transactions"]
    assert result.duplicates_skipped == 1
    assert result.events_persisted == 0
    assert "transactions" not in env["buffers"]


def test_offset_equal_to_checkpoint_is_also_a_duplicate():
    env = _new_env()
    env["checkpoints"][("finpay.finpay.transactions", 0)] = 5
    msg = RawMessage("finpay.finpay.transactions", 0, 5, KEY, _insert_value())
    process_message(msg, **env)
    assert env["results"]["transactions"].duplicates_skipped == 1


def test_offset_above_checkpoint_is_processed_normally():
    env = _new_env()
    env["checkpoints"][("finpay.finpay.transactions", 0)] = 5
    msg = RawMessage("finpay.finpay.transactions", 0, 6, KEY, _insert_value())
    process_message(msg, **env)
    assert env["results"]["transactions"].events_persisted == 1
    assert env["results"]["transactions"].duplicates_skipped == 0


def test_idempotency_key_is_topic_partition_offset_not_transaction_id():
    """Two different messages for the SAME business transaction_id (an insert
    then an update) must both be processed — the dedup key is Kafka identity,
    never the business key, per §11's explicit instruction."""
    env = _new_env()
    insert_msg = RawMessage("finpay.finpay.transactions", 0, 10, KEY, _insert_value("42.00"))
    update_value = json.dumps(
        {"schema": {}, "payload": {"before": {"amount": "42.00"}, "after": {"transaction_id": "TXN-1", "amount": "99.00"}, "source": {}, "op": "u", "ts_ms": 2000, "transaction": None}}
    ).encode()
    update_msg = RawMessage("finpay.finpay.transactions", 0, 11, KEY, update_value)

    process_message(insert_msg, **env)
    process_message(update_msg, **env)

    result = env["results"]["transactions"]
    assert result.events_persisted == 2  # both kept — same transaction_id, different offsets
    assert [r["operation"] for r in env["buffers"]["transactions"]] == ["c", "u"]


def test_max_offset_seen_tracks_the_highest_offset_per_partition():
    env = _new_env()
    for offset in [10, 12, 11]:
        msg = RawMessage("finpay.finpay.transactions", 0, offset, KEY, _insert_value())
        process_message(msg, **env)
    assert env["results"]["transactions"].max_offset_seen[0] == 12  # highest, not last-processed


def test_events_read_equals_the_sum_of_all_four_dispositions():
    env = _new_env()
    env["checkpoints"][("finpay.finpay.transactions", 0)] = 100
    process_message(RawMessage("finpay.finpay.transactions", 0, 50, KEY, _insert_value()), **env)  # duplicate
    process_message(RawMessage("finpay.finpay.transactions", 0, 101, KEY, _insert_value()), **env)  # persisted
    process_message(RawMessage("finpay.finpay.transactions", 0, 102, KEY, None), **env)  # tombstone
    process_message(RawMessage("finpay.finpay.transactions", 0, 103, KEY, b"bad json"), **env)  # quarantined

    result = env["results"]["transactions"]
    assert result.events_read == 4
    total = result.events_persisted + result.duplicates_skipped + result.quarantined + result.tombstones_handled
    assert total == result.events_read == 4


# ---- Bronze record construction ----


def test_build_bronze_record_contains_all_documented_fields():
    msg = RawMessage("finpay.finpay.transactions", 0, 42, KEY, _insert_value())
    parsed = parse_message(msg.key, msg.value)
    record = build_bronze_record(msg, parsed)

    expected_fields = {
        "source_topic", "kafka_partition", "kafka_offset", "event_key", "operation", "before", "after",
        "source_connector", "source_name", "source_schema", "source_table", "source_database",
        "source_lsn", "source_timestamp_ms", "snapshot_indicator",
        "transaction_id", "transaction_total_order", "transaction_data_collection_order", "cdc_received_at_utc",
    }
    assert set(record.keys()) == expected_fields
    assert record["source_topic"] == "finpay.finpay.transactions"
    assert record["kafka_partition"] == 0
    assert record["kafka_offset"] == 42
    assert record["cdc_received_at_utc"]  # non-empty, generated


def test_build_bronze_record_preserves_raw_after_payload_unflattened():
    msg = RawMessage("finpay.finpay.transactions", 0, 0, KEY, _insert_value("42.00"))
    parsed = parse_message(msg.key, msg.value)
    record = build_bronze_record(msg, parsed)
    assert json.loads(record["after"]) == {"transaction_id": "TXN-1", "amount": "42.00"}
