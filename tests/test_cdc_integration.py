"""S11 — live CDC integration tests against the real local
PostgreSQL/Kafka/Debezium stack (docker/docker-compose.yml). Skipped cleanly
(not failed) when that infrastructure isn't reachable, so the rest of the
suite never depends on it.

No Kafka client library is used — Kafka broker/topic verification shells out
to `docker exec merchantmcc_kafka ...` using Kafka's own bundled CLI tools,
per this phase's explicit "do not introduce Python Kafka clients" instruction.
Kafka Connect's REST API is plain HTTP (via `requests`, already an approved
dependency).
"""

import json
import subprocess
import time

import psycopg
import pytest

from src.cdc import connector as connector_mod
from src.cdc.publication import PUBLICATION_NAME, PUBLISHED_TABLES, ensure_publication, replication_slot_info
from src.oltp import database
from src.oltp.schema import SCHEMA_NAME


def _kafka_reachable() -> bool:
    try:
        result = subprocess.run(
            ["docker", "exec", "merchantmcc_kafka", "/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092"],
            capture_output=True, timeout=15,
        )
        return result.returncode == 0
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        return False


def _infra_ready() -> bool:
    return database.health_check() and _kafka_reachable() and connector_mod.connect_is_reachable()


pytestmark = pytest.mark.skipif(
    not _infra_ready(),
    reason="Postgres/Kafka/Kafka Connect not all reachable — run `docker compose up -d` in docker/ and wait for health checks",
)


def _kafka_exec(*args: str, timeout: int = 20) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", "exec", "merchantmcc_kafka", *args], capture_output=True, text=True, timeout=timeout)


def _list_topics() -> list[str]:
    result = _kafka_exec("/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list")
    return [line for line in result.stdout.splitlines() if line.strip()]


def _consume_messages(topic: str, max_messages: int, timeout_ms: int = 15000) -> list[dict]:
    """Reads from the beginning of the topic — fine for these small,
    dedicated test topics (thousands of messages at most, not a production
    consumer pattern). Debezium emits a null-value "tombstone" record
    immediately after each delete event (for Kafka log-compaction purposes,
    per docs/24 §13) — it parses as JSON `null`, not a dict, so it's skipped
    here rather than treated as a malformed message."""
    result = _kafka_exec(
        "/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server", "localhost:9092",
        "--topic", topic,
        "--from-beginning",
        "--max-messages", str(max_messages),
        "--timeout-ms", str(timeout_ms),
        timeout=(timeout_ms // 1000) + 15,
    )
    messages = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if line:
            try:
                parsed = json.loads(line)
                if parsed is None:
                    continue  # tombstone record
                messages.append(parsed)
            except json.JSONDecodeError:
                continue
    return messages


@pytest.fixture(scope="module", autouse=True)
def _ensure_cdc_stack_configured():
    ensure_publication()
    connector_mod.ensure_connector_registered()
    deadline = time.time() + 120
    while time.time() < deadline:
        status = connector_mod.connector_status()
        connector_state = status.get("connector", {}).get("state")
        task_states = [t.get("state") for t in status.get("tasks", [])]
        if connector_state == "RUNNING" and task_states and all(s == "RUNNING" for s in task_states):
            break
        time.sleep(3)
    yield


# ---- publication / replication slot (Postgres side) ----


def test_publication_exists_with_exactly_the_eleven_tables():
    with database.connect() as conn:
        from src.cdc.publication import published_tables

        assert published_tables(conn) == PUBLISHED_TABLES


def test_replication_slot_exists_after_connector_start():
    slots = replication_slot_info(slot_name="finpay_slot")
    assert len(slots) == 1
    assert slots[0]["plugin"] == "pgoutput"
    assert slots[0]["slot_type"] == "logical"


# ---- connector state ----


def test_connector_reaches_running_state():
    status = connector_mod.connector_status()
    assert status["connector"]["state"] == "RUNNING"
    assert len(status["tasks"]) >= 1
    assert all(t["state"] == "RUNNING" for t in status["tasks"])


# ---- Kafka broker / topics ----


def test_kafka_broker_is_healthy():
    assert _kafka_reachable() is True


def test_expected_topics_exist_for_all_eleven_tables():
    topics = _list_topics()
    for table in PUBLISHED_TABLES:
        assert f"finpay.finpay.{table}" in topics, f"missing topic for {table}"


# ---- snapshot events ----


def test_snapshot_events_observed_on_clients_topic():
    messages = _consume_messages("finpay.finpay.clients", max_messages=12)
    assert len(messages) >= 1
    ops = {m["payload"]["op"] for m in messages}
    assert "r" in ops, "expected at least one snapshot (op=r) event for the already-populated clients table"


def test_snapshot_event_structure_has_expected_debezium_fields():
    messages = _consume_messages("finpay.finpay.clients", max_messages=1)
    assert len(messages) == 1
    payload = messages[0]["payload"]
    for field in ("before", "after", "source", "op", "ts_ms"):
        assert field in payload
    for field in ("connector", "db", "schema", "table", "ts_ms"):
        assert field in payload["source"]
    assert payload["source"]["db"] == "merchantmcc"
    assert payload["source"]["schema"] == "finpay"


# ---- real INSERT / UPDATE / DELETE on an isolated test transaction ----

TEST_TXN_ID = "TXN-CDCTST01"


def _cleanup_test_transaction():
    with database.connect() as conn, conn.cursor() as cur:
        cur.execute(f"DELETE FROM {SCHEMA_NAME}.transactions WHERE transaction_id = %s;", (TEST_TXN_ID,))
        conn.commit()


def test_insert_update_delete_are_captured_as_real_cdc_events():
    _cleanup_test_transaction()  # in case a prior failed run left it behind
    try:
        with database.connect() as conn, conn.cursor() as cur:
            # a real, valid token must already exist to satisfy the FK
            cur.execute(f"SELECT token_id FROM {SCHEMA_NAME}.card_tokens LIMIT 1;")
            token_id = cur.fetchone()[0]

            cur.execute(
                f"""INSERT INTO {SCHEMA_NAME}.transactions
                (transaction_id, token_id, merchant_id, mcc_code, mcc_confidence, currency_code, amount,
                 transaction_timestamp, status, decline_reason, auth_code)
                VALUES (%s, %s, 'node:cdc-test', '5999', 0.5, 'GBP', 42.00, '2026-01-01T10:00:00', 'APPROVED', NULL, 'CDCTST');""",
                (TEST_TXN_ID, token_id),
            )
            conn.commit()

            cur.execute(f"UPDATE {SCHEMA_NAME}.transactions SET amount = 99.00 WHERE transaction_id = %s;", (TEST_TXN_ID,))
            conn.commit()

            cur.execute(f"DELETE FROM {SCHEMA_NAME}.transactions WHERE transaction_id = %s;", (TEST_TXN_ID,))
            conn.commit()

        # Give Debezium a moment to pick up the WAL changes and produce them.
        # The transactions topic already holds ~4000 snapshot messages (S10's
        # real data) ahead of our 3 new ones, so max_messages must comfortably
        # exceed that to ever reach the tail — an earlier, smaller cap missed
        # them entirely (a real bug found and fixed while writing this test).
        deadline = time.time() + 90
        messages: list[dict] = []
        while time.time() < deadline:
            messages = _consume_messages("finpay.finpay.transactions", max_messages=4100, timeout_ms=20000)
            test_messages = [m for m in messages if (m["payload"]["after"] or {}).get("transaction_id") == TEST_TXN_ID or (m["payload"]["before"] or {}).get("transaction_id") == TEST_TXN_ID]
            ops = [m["payload"]["op"] for m in test_messages]
            if "c" in ops and "u" in ops and "d" in ops:
                break

        test_messages = [m for m in messages if (m["payload"]["after"] or {}).get("transaction_id") == TEST_TXN_ID or (m["payload"]["before"] or {}).get("transaction_id") == TEST_TXN_ID]
        ops_seen = [m["payload"]["op"] for m in test_messages]

        assert "c" in ops_seen, f"INSERT (op=c) not observed for {TEST_TXN_ID}; ops seen: {ops_seen}"
        assert "u" in ops_seen, f"UPDATE (op=u) not observed for {TEST_TXN_ID}; ops seen: {ops_seen}"
        assert "d" in ops_seen, f"DELETE (op=d) not observed for {TEST_TXN_ID}; ops seen: {ops_seen}"

        insert_event = next(m for m in test_messages if m["payload"]["op"] == "c")
        assert insert_event["payload"]["before"] is None
        assert insert_event["payload"]["after"]["amount"] == "42.00"

        update_event = next(m for m in test_messages if m["payload"]["op"] == "u")
        assert update_event["payload"]["after"]["amount"] == "99.00"

        delete_event = next(m for m in test_messages if m["payload"]["op"] == "d")
        assert delete_event["payload"]["after"] is None
        assert delete_event["payload"]["before"]["transaction_id"] == TEST_TXN_ID
    finally:
        _cleanup_test_transaction()


# ---- no existing business data touched ----


def test_real_business_transactions_untouched_by_cdc_test(TXN_SAMPLE="TXN-0000001"):
    with database.connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.transactions WHERE transaction_id = %s;", (TXN_SAMPLE,))
        assert cur.fetchone()[0] == 1
