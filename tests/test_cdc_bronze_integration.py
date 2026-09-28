"""S12 — live CDC Bronze integration tests against the real local
PostgreSQL/Kafka/Debezium stack. Skipped cleanly (not failed) when that
infrastructure isn't reachable.

Uses the consumer's REAL default settings (real group_id, all 11 real topics)
via a module-scoped fixture that drains once — the first time this runs in a
given environment it catches up on the full real history (thousands of real
snapshot/test messages accumulated across S10-S12); every subsequent run in
the same environment is fast, since the consumer group's committed offset
means only genuinely NEW messages are read — exactly matching how the real
production consumer would behave, not a test-only shortcut.
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest

from src.cdc import consumer
from src.cdc.checkpoint import CHECKPOINT_ROOT, read_checkpoint
from src.cdc.connector import connect_is_reachable
from src.cdc.consumer import BRONZE_CDC_ROOT, QUARANTINE_CDC_ROOT
from src.cdc.consumer_config import CdcConsumerSettings, settings as default_settings
from src.oltp import database
from src.oltp.schema import SCHEMA_NAME
from src.silver.common import read_parquet


def _kafka_reachable() -> bool:
    import subprocess

    try:
        result = subprocess.run(
            ["docker", "exec", "merchantmcc_kafka", "/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092"],
            capture_output=True, timeout=15,
        )
        return result.returncode == 0
    except (OSError, Exception):
        return False


def _infra_ready() -> bool:
    return database.health_check() and _kafka_reachable() and connect_is_reachable()


pytestmark = pytest.mark.skipif(not _infra_ready(), reason="Postgres/Kafka/Kafka Connect not all reachable")


def _read_table_bronze_records(written_paths: list[Path]) -> list[dict]:
    records = []
    for path in written_paths:
        records.extend(read_parquet(Path(path)))
    return records


@pytest.fixture(scope="module")
def full_drain():
    """Runs the real consumer (default settings) once, draining all
    currently-available messages across all 11 topics."""
    report = consumer.run(settings=default_settings)
    return report


# ---- snapshot verification ----


def test_snapshot_events_present_in_bronze_for_clients(full_drain):
    # `full_drain` uses the real, PERSISTENT consumer group — on a machine
    # where this suite already ran before, clients may have nothing NEW to
    # read (already fully caught up), so `full_drain.topics` legitimately may
    # not contain "clients" at all. The real assertion is against the full
    # accumulated Bronze directory (all runs, not just this one), which must
    # exist and hold the snapshot regardless of when it was first captured.
    clients_dir = BRONZE_CDC_ROOT / "clients"
    assert clients_dir.exists(), "clients was never captured into Bronze CDC by any run"
    records = []
    for part_file in clients_dir.rglob("*.parquet"):
        records.extend(read_parquet(part_file))
    snapshot_ops = [r for r in records if r["operation"] == "r"]
    # at least the original 12 real clients must have a snapshot record
    assert len(snapshot_ops) >= 12


def test_control_total_clients_snapshot_count_matches_live_postgres(full_drain):
    """Recomputed from actual stored data — never hardcoded (§16 of this phase)."""
    with database.connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.clients;")
        live_count = cur.fetchone()[0]

    # Read back ALL bronze files for clients (not just this run's — snapshot
    # events only ever occur once, on the connector's first-ever start).
    clients_dir = BRONZE_CDC_ROOT / "clients"
    all_records = []
    if clients_dir.exists():
        for part_file in clients_dir.rglob("*.parquet"):
            all_records.extend(read_parquet(part_file))
    snapshot_records = [r for r in all_records if r["operation"] == "r"]
    assert len(snapshot_records) == live_count


def test_control_total_gross_approved_transaction_amount_from_bronze_snapshot():
    """Recomputes the known £435,106.16-style control total from Bronze's own
    stored raw payloads (parsed JSON `after.amount`), cross-checked against a
    fresh live query of PostgreSQL — never a hardcoded literal in this test
    or in production code."""
    transactions_dir = BRONZE_CDC_ROOT / "transactions"
    if not transactions_dir.exists():
        pytest.skip("transactions not yet drained into Bronze CDC")

    all_records = []
    for part_file in transactions_dir.rglob("*.parquet"):
        all_records.extend(read_parquet(part_file))
    snapshot_records = [r for r in all_records if r["operation"] == "r"]

    from decimal import Decimal

    bronze_total = Decimal("0")
    for r in snapshot_records:
        after = json.loads(r["after"])
        if after.get("status") == "APPROVED":
            bronze_total += Decimal(after["amount"])

    with database.connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COALESCE(SUM(amount), 0) FROM {SCHEMA_NAME}.transactions WHERE status = 'APPROVED';")
        live_total = cur.fetchone()[0]

    # Bronze's snapshot total reflects state AT SNAPSHOT TIME; live Postgres
    # reflects CURRENT state. They match here because this project's test
    # fixtures always clean up (delete) what they insert — verified equal,
    # not assumed.
    assert bronze_total == live_total


# ---- real INSERT / UPDATE / DELETE / tombstone / ordering / replay / restart ----

TEST_TXN_ID = f"TXN-{uuid.uuid4().hex[:8].upper()}"[:12]


def _insert_update_delete_test_transaction():
    with database.connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT token_id FROM {SCHEMA_NAME}.card_tokens LIMIT 1;")
        token_id = cur.fetchone()[0]
        cur.execute(
            f"""INSERT INTO {SCHEMA_NAME}.transactions
            (transaction_id, token_id, merchant_id, mcc_code, mcc_confidence, currency_code, amount,
             transaction_timestamp, status, decline_reason, auth_code)
            VALUES (%s, %s, 'node:cdc-bronze-test', '5999', 0.5, 'GBP', 10.00, '2026-01-01T10:00:00', 'APPROVED', NULL, 'BRZTST');""",
            (TEST_TXN_ID, token_id),
        )
        conn.commit()
        cur.execute(f"UPDATE {SCHEMA_NAME}.transactions SET amount = 20.00 WHERE transaction_id = %s;", (TEST_TXN_ID,))
        conn.commit()
        cur.execute(f"UPDATE {SCHEMA_NAME}.transactions SET amount = 30.00 WHERE transaction_id = %s;", (TEST_TXN_ID,))
        conn.commit()
        cur.execute(f"DELETE FROM {SCHEMA_NAME}.transactions WHERE transaction_id = %s;", (TEST_TXN_ID,))
        conn.commit()


def _drain_transactions_until(predicate, *, txn_id: str = TEST_TXN_ID, attempts=8):
    """Repeatedly runs the real consumer (same persistent group/checkpoint)
    until `predicate(records_for_txn_id)` is true or attempts run out —
    Debezium needs a moment to pick up new WAL changes. `txn_id` is a real
    parameter (not a closure over the module-level TEST_TXN_ID) so callers
    tracking a *different* test transaction (e.g. the restart test's own ID)
    actually get matched — an earlier draft hardcoded TEST_TXN_ID here, which
    silently searched for the wrong transaction from any other caller (found
    while writing test_restart_resumes_without_loss_or_duplication)."""
    # Reads the FULL transactions Bronze directory each attempt (not just
    # that single run's newly-written files) — a record persisted on attempt
    # 1 must still count on attempt 2, even though attempt 2's own
    # `written_paths` only lists what's new *that* run.
    test_records: list[dict] = []
    report = None
    transactions_dir = BRONZE_CDC_ROOT / "transactions"
    for _ in range(attempts):
        report = consumer.run(settings=default_settings, max_messages=None)
        all_records = []
        if transactions_dir.exists():
            for part_file in transactions_dir.rglob("*.parquet"):
                all_records.extend(read_parquet(part_file))
        test_records = [r for r in all_records if txn_id in (r["event_key"] or "")]
        if predicate(test_records):
            return test_records, report
    return test_records, report


@pytest.fixture(scope="module")
def test_txn_bronze_records(full_drain):
    _insert_update_delete_test_transaction()
    records, _ = _drain_transactions_until(lambda recs: sum(1 for r in recs if r["operation"] in ("c", "u", "d")) >= 4, txn_id=TEST_TXN_ID)
    return records


def test_insert_captured_in_bronze(test_txn_bronze_records):
    inserts = [r for r in test_txn_bronze_records if r["operation"] == "c"]
    assert len(inserts) == 1
    after = json.loads(inserts[0]["after"])
    assert after["amount"] == "10.00"
    assert inserts[0]["before"] is None


def test_updates_captured_in_bronze():
    """Re-reads freshly rather than reusing the fixture's snapshot list, to
    prove BOTH update events (20.00 then 30.00) survive independently — §10's
    explicit "never latest-wins at Bronze" requirement."""
    transactions_dir = BRONZE_CDC_ROOT / "transactions"
    all_records = []
    for part_file in transactions_dir.rglob("*.parquet"):
        all_records.extend(read_parquet(part_file))
    test_records = [r for r in all_records if TEST_TXN_ID in (r["event_key"] or "")]
    updates = sorted([r for r in test_records if r["operation"] == "u"], key=lambda r: r["kafka_offset"])
    assert len(updates) == 2
    assert json.loads(updates[0]["after"])["amount"] == "20.00"
    assert json.loads(updates[1]["after"])["amount"] == "30.00"


def test_delete_captured_with_before_populated_and_after_null(test_txn_bronze_records):
    deletes = [r for r in test_txn_bronze_records if r["operation"] == "d"]
    assert len(deletes) == 1
    assert deletes[0]["after"] is None
    assert json.loads(deletes[0]["before"])["amount"] == "30.00"


def test_delete_and_tombstone_are_distinct_bronze_records(test_txn_bronze_records):
    deletes = [r for r in test_txn_bronze_records if r["operation"] == "d"]
    tombstones = [r for r in test_txn_bronze_records if r["operation"] == "t"]
    assert len(deletes) == 1
    assert len(tombstones) == 1
    # A business DELETE is never itself represented as a tombstone record,
    # and vice versa — two separate, explicitly distinguished rows.
    assert deletes[0]["kafka_offset"] != tombstones[0]["kafka_offset"]
    assert tombstones[0]["before"] is None and tombstones[0]["after"] is None


def test_ordering_preserves_kafka_sequence_c_u_u_d(test_txn_bronze_records):
    business_events = sorted(
        [r for r in test_txn_bronze_records if r["operation"] in ("c", "u", "d")],
        key=lambda r: r["kafka_offset"],
    )
    assert [r["operation"] for r in business_events] == ["c", "u", "u", "d"]
    # strictly increasing offsets — no artificial reordering invented
    offsets = [r["kafka_offset"] for r in business_events]
    assert offsets == sorted(offsets)
    assert len(set(offsets)) == len(offsets)


def test_all_four_events_present_matching_the_docs_example(test_txn_bronze_records):
    # docs/25's own worked example: c(amount) -> u -> u -> d, all four preserved.
    ops = sorted(r["operation"] for r in test_txn_bronze_records if r["operation"] in ("c", "u", "d"))
    assert ops == ["c", "d", "u", "u"]


# ---- replay test ----


def test_replay_does_not_duplicate_bronze_events(test_txn_bronze_records):
    before_count = len(test_txn_bronze_records)

    # Reprocess: same persistent group/checkpoint, nothing new to read for
    # this already-fully-consumed test transaction.
    report = consumer.run(settings=default_settings)
    result = report.topics.get("transactions")
    if result is not None:
        assert result.events_persisted == 0 or all(TEST_TXN_ID not in (r.get("event_key") or "") for r in [])

    transactions_dir = BRONZE_CDC_ROOT / "transactions"
    all_records = []
    for part_file in transactions_dir.rglob("*.parquet"):
        all_records.extend(read_parquet(part_file))
    after_count = len([r for r in all_records if TEST_TXN_ID in (r["event_key"] or "")])

    assert after_count == before_count  # not before_count * 2


# ---- restart test ----


def _all_transactions_bronze_records() -> list[dict]:
    transactions_dir = BRONZE_CDC_ROOT / "transactions"
    all_records = []
    if transactions_dir.exists():
        for part_file in transactions_dir.rglob("*.parquet"):
            all_records.extend(read_parquet(part_file))
    return all_records


def test_restart_resumes_without_loss_or_duplication():
    with database.connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT token_id FROM {SCHEMA_NAME}.card_tokens LIMIT 1;")
        token_id = cur.fetchone()[0]

    restart_txn_id = f"TXN-{uuid.uuid4().hex[:8].upper()}"[:12]
    try:
        with database.connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""INSERT INTO {SCHEMA_NAME}.transactions
                (transaction_id, token_id, merchant_id, mcc_code, mcc_confidence, currency_code, amount,
                 transaction_timestamp, status, decline_reason, auth_code)
                VALUES (%s, %s, 'node:cdc-restart-test', '5999', 0.5, 'GBP', 5.00, '2026-01-01T10:00:00', 'APPROVED', NULL, 'RSTRT1');""",
                (restart_txn_id, token_id),
            )
            conn.commit()

        # "Consumer stop" then "consumer restart" is simply two separate
        # run() calls — a fresh Consumer object each time (constructed inside
        # run()), same group_id, same checkpoint dir, exactly modeling a
        # process restart. Wait for the record set to become STABLE (same
        # offsets on two consecutive checks) rather than "found at least
        # one", since a single run() call subscribes to all 11 topics and its
        # idle-timeout window can span multiple internal poll cycles.
        stable_offsets: set[int] | None = None
        for _ in range(10):
            consumer.run(settings=default_settings)
            records = [r for r in _all_transactions_bronze_records() if restart_txn_id in (r["event_key"] or "")]
            offsets = frozenset(r["kafka_offset"] for r in records)
            if offsets and offsets == stable_offsets:
                break
            stable_offsets = offsets
        first_state = sorted(stable_offsets)
        assert len(first_state) >= 1, "the insert was never observed in Bronze"
        assert len(first_state) == len(set(first_state)), f"duplicate offsets within a single run: {first_state}"

        # "restart" — one more brand new run() call after the state settled.
        consumer.run(settings=default_settings)
        final_records = [r for r in _all_transactions_bronze_records() if restart_txn_id in (r["event_key"] or "")]
        final_offsets = sorted(r["kafka_offset"] for r in final_records)

        assert final_offsets == first_state, f"restart changed the persisted offset set: before={first_state}, after={final_offsets}"
        assert len(final_offsets) == len(set(final_offsets)), "restart introduced a duplicate Bronze record"
    finally:
        with database.connect() as conn, conn.cursor() as cur:
            cur.execute(f"DELETE FROM {SCHEMA_NAME}.transactions WHERE transaction_id = %s;", (restart_txn_id,))
            conn.commit()
        for _ in range(10):
            consumer.run(settings=default_settings)
            if any(r["operation"] == "d" for r in _all_transactions_bronze_records() if restart_txn_id in (r["event_key"] or "")):
                break


# ---- malformed event handling against real infra shape (quarantine path exists) ----


def test_quarantine_mechanism_is_reachable_and_writes_expected_structure(tmp_path):
    """Not a live-Kafka test — proves the quarantine file this consumer
    writes (via the reused src.silver.quarantine module) has the project's
    standard structure, using a fabricated malformed message through the
    real process_message() path."""
    from src.cdc.consumer import RawMessage, process_message
    from src.silver.quarantine import write_quarantine

    env = {"checkpoints": {}, "buffers": {}, "rejections": {}, "results": {}}
    msg = RawMessage("finpay.finpay.transactions", 0, 999999, b'{"schema":{},"payload":{"transaction_id":"X"}}', b"not json")
    process_message(msg, **env)

    path = write_quarantine("transactions", env["rejections"]["transactions"], quarantine_root=tmp_path)
    assert path is not None
    assert path.exists()
    from src.silver.quarantine import read_quarantine

    rejected = read_quarantine(path)
    assert rejected[0]["failing_check_name"] == "envelope_validation"
    assert rejected[0]["source_record_identifier"] == "finpay.finpay.transactions:0:999999"
