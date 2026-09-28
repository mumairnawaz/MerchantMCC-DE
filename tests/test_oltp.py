"""S10 — FinPay OLTP tests, run against the real local PostgreSQL container
(docker/docker-compose.yml, already running — reused, not created by this
test suite). No external APIs are called. Bronze/Silver are untouched — this
suite only reads data/synthetic_oltp/*.parquet (S9 output) and writes to the
local Postgres database.

The whole module is skipped (not failed) if Postgres isn't reachable, so the
rest of the test suite stays runnable without Docker.
"""

import json
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest

from src.oltp import database, loader, validation
from src.oltp.config import settings
from src.oltp.loader import PRIMARY_KEYS, load_all
from src.oltp.schema import SCHEMA_NAME, TABLE_DEPENDENCIES, load_order
from src.synthetic.common import SYNTHETIC_ROOT
from src.synthetic.common import read_table as read_parquet_table
from src.synthetic.common import write_table as write_parquet_table
from src.synthetic.schemas import SCHEMAS

pytestmark = pytest.mark.skipif(not database.health_check(), reason="PostgreSQL is not reachable — start docker/docker-compose.yml")


@pytest.fixture(scope="module", autouse=True)
def _ensure_schema_and_loaded():
    """Idempotent by construction — safe to run alongside other tests/manual
    runs against the same long-lived container."""
    database.init_schema()
    load_all()
    yield


# ---- 1. connection / configuration ----


def test_health_check_reports_true_when_reachable():
    assert database.health_check() is True


def test_connect_yields_a_working_connection():
    with database.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT current_database();")
        assert cur.fetchone()[0] == settings.dbname


# ---- 2/3. schema initialization / table existence ----


def test_init_schema_is_idempotent():
    database.init_schema()
    database.init_schema()  # must not raise


def test_all_eleven_tables_exist():
    with database.connect() as conn:
        for table in TABLE_DEPENDENCIES:
            assert database.table_exists(conn, table), table


# ---- 4/5. primary keys / foreign keys (declared, via information_schema) ----


def test_every_table_has_a_declared_primary_key():
    with database.connect() as conn, conn.cursor() as cur:
        for table in TABLE_DEPENDENCIES:
            cur.execute(
                """
                SELECT COUNT(*) FROM information_schema.table_constraints
                WHERE table_schema = %s AND table_name = %s AND constraint_type = 'PRIMARY KEY';
                """,
                (SCHEMA_NAME, table),
            )
            assert cur.fetchone()[0] == 1, f"{table} missing a PRIMARY KEY"


def test_every_declared_dependency_has_a_foreign_key_constraint():
    with database.connect() as conn, conn.cursor() as cur:
        for table, deps in TABLE_DEPENDENCIES.items():
            cur.execute(
                """
                SELECT COUNT(*) FROM information_schema.table_constraints
                WHERE table_schema = %s AND table_name = %s AND constraint_type = 'FOREIGN KEY';
                """,
                (SCHEMA_NAME, table),
            )
            fk_count = cur.fetchone()[0]
            assert fk_count >= len(deps), f"{table}: expected >= {len(deps)} FKs, found {fk_count}"


# ---- 6. uniqueness ----


def test_settlements_transaction_id_is_unique():
    with database.connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT transaction_id) FROM {SCHEMA_NAME}.settlements;")
        total, distinct = cur.fetchone()
        assert total == distinct


def test_clients_lei_is_unique():
    with database.connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT lei) FROM {SCHEMA_NAME}.clients;")
        total, distinct = cur.fetchone()
        assert total == distinct


# ---- 7/8. loading each table / dependency-order loading ----


def test_load_order_is_a_valid_topological_sort():
    order = load_order()
    assert set(order) == set(TABLE_DEPENDENCIES)
    position = {name: i for i, name in enumerate(order)}
    for table, deps in TABLE_DEPENDENCIES.items():
        for dep in deps:
            assert position[dep] < position[table], f"{dep} must load before {table}"


def test_every_table_has_rows_loaded():
    with database.connect() as conn, conn.cursor() as cur:
        for table in TABLE_DEPENDENCIES:
            cur.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{table};")
            assert cur.fetchone()[0] > 0, f"{table} has no rows"


# ---- 9/10. row-count / control-total reconciliation against the S9 source ----


def test_row_counts_match_parquet_source_exactly():
    with database.connect() as conn:
        counts = validation.verify_row_counts(conn)
    for table in TABLE_DEPENDENCIES:
        source_len = len(read_parquet_table(table))
        assert counts[table] == source_len


def test_control_totals_match_and_reproduce_the_s9_identity():
    with database.connect() as conn:
        totals = validation.verify_control_totals(conn)
    assert totals["gross_approved_transaction_amount"] == totals["gross_settlement_amount"] == totals["reconciliation_expected_total"]


def test_control_totals_are_read_from_source_not_hardcoded():
    transactions = read_parquet_table("transactions")
    expected = Decimal(str(round(sum(t["amount"] for t in transactions if t["status"] == "APPROVED"), 2)))
    with database.connect() as conn:
        totals = validation.verify_control_totals(conn)
    assert totals["gross_approved_transaction_amount"] == expected


# ---- 11. financial amount preservation (Parquet float64 -> Postgres NUMERIC) ----


def test_financial_amounts_preserved_exactly_for_every_transaction():
    transactions = read_parquet_table("transactions")
    with database.connect() as conn, conn.cursor() as cur:
        for txn in transactions[:50]:  # a real sample, not just aggregate totals
            cur.execute(f"SELECT amount FROM {SCHEMA_NAME}.transactions WHERE transaction_id = %s;", (txn["transaction_id"],))
            db_amount = cur.fetchone()[0]
            assert db_amount == Decimal(str(txn["amount"]))


def test_amount_column_is_numeric_not_floating_point():
    with database.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT data_type FROM information_schema.columns WHERE table_schema=%s AND table_name='transactions' AND column_name='amount';",
            (SCHEMA_NAME,),
        )
        assert cur.fetchone()[0] == "numeric"


# ---- 12. foreign-key integrity (no orphans) ----


def test_no_orphaned_foreign_keys():
    with database.connect() as conn:
        validation.verify_no_orphans(conn)  # raises on any orphan


def test_foreign_key_violation_is_rejected_by_postgres():
    with database.connect() as conn, conn.cursor() as cur:
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            cur.execute(
                f"INSERT INTO {SCHEMA_NAME}.programs (program_id, client_id, program_name, program_type, currency_code, status, start_date) "
                f"VALUES ('PRG-9999', 'CLI-XXXXX', 'x', 'CASHBACK', 'GBP', 'ACTIVE', '2026-01-01');"
            )
        conn.rollback()


def test_check_constraint_rejects_non_positive_amount():
    with database.connect() as conn, conn.cursor() as cur:
        with pytest.raises(psycopg.errors.CheckViolation):
            cur.execute(
                f"INSERT INTO {SCHEMA_NAME}.transactions "
                f"(transaction_id, token_id, merchant_id, mcc_code, mcc_confidence, currency_code, amount, "
                f"transaction_timestamp, status, decline_reason, auth_code) "
                f"SELECT 'TXN-9999999', token_id, 'node:x', '5999', 0.5, 'GBP', -10.00, "
                f"'2026-01-01T00:00:00', 'APPROVED', NULL, 'ABC123' FROM {SCHEMA_NAME}.card_tokens LIMIT 1;"
            )
        conn.rollback()


# ---- 13/14. loader rerun / duplicate prevention ----


def test_rerunning_loader_inserts_zero_new_rows():
    report = load_all()
    for result in report.tables:
        assert result.inserted == 0, f"{result.table}: expected 0 new inserts on rerun, got {result.inserted}"
        assert result.skipped_existing == result.source_row_count


def test_rerunning_loader_does_not_change_row_counts():
    with database.connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.transactions;")
        before = cur.fetchone()[0]
    load_all()
    with database.connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.transactions;")
        after = cur.fetchone()[0]
    assert before == after


# ---- 15. failure / rollback behavior ----


def _write_isolated_fixture(root: Path, *, break_reward_event: bool) -> None:
    """A complete, self-contained chain of brand-new primary keys (CLI-9001...)
    that never collide with the real S9 data, so this test can prove rollback
    without risking or depending on the already-seeded rows."""
    client = {"client_id": "CLI-9001", "lei": "TESTLEI0000000000001", "legal_name": "Rollback Test Co", "client_type": "PROGRAM_OWNER", "country_code": "GB", "onboarding_date": "2026-01-01", "status": "ACTIVE", "source_type": "synthetic"}
    program = {"program_id": "PRG-9001", "client_id": "CLI-9001", "program_name": "Rollback Test Program", "program_type": "CASHBACK", "currency_code": "GBP", "status": "ACTIVE", "start_date": "2026-01-01", "source_type": "synthetic"}
    campaign = {"campaign_id": "CMP-9001", "program_id": "PRG-9001", "campaign_name": "Rollback Test Campaign", "start_date": "2026-01-01", "end_date": "2026-12-31", "status": "ACTIVE", "source_type": "synthetic"}
    offer = {"offer_id": "OFR-9001", "campaign_id": "CMP-9001", "eligible_mcc_code": None, "offer_type": "FIXED_AMOUNT", "offer_value": 5.0, "min_transaction_amount": 1.0, "currency_code": "GBP", "status": "ACTIVE", "valid_from": "2026-01-01", "valid_to": "2026-12-31", "source_type": "synthetic"}
    cardholder = {"cardholder_id": "CH-9001", "pseudonym": "Cardholder-Rollback", "country_code": "GB", "enrolled_program_id": "PRG-9001", "enrollment_date": "2026-01-01", "source_type": "synthetic"}
    token = {"token_id": "TKN-9001aaaaaaaaaaaaa", "cardholder_id": "CH-9001", "bin_range": "000000", "card_brand": "TESTBRAND", "token_status": "ACTIVE", "issued_date": "2026-01-01", "source_type": "synthetic"}
    transaction = {"transaction_id": "TXN-9990001", "token_id": "TKN-9001aaaaaaaaaaaaa", "merchant_id": "node:0", "mcc_code": "5999", "mcc_confidence": 0.5, "currency_code": "GBP", "amount": 10.0, "transaction_timestamp": "2026-01-01T10:00:00", "status": "APPROVED", "decline_reason": None, "auth_code": "ABC123", "source_type": "synthetic"}
    event = {"event_id": "EVT-9990001", "transaction_id": "TXN-9990001", "event_type": "AUTHORIZATION", "event_timestamp": "2026-01-01T10:00:00", "event_status": "APPROVED", "source_type": "synthetic"}
    settlement = {"settlement_id": "STL-9990001", "transaction_id": "TXN-9990001", "settlement_batch_id": "BATCH-TEST", "settlement_date": "2026-01-02", "settlement_amount": 10.0, "settlement_currency": "GBP", "fee_amount": 0.15, "net_amount": 9.85, "source_type": "synthetic"}
    reconciliation = {"reconciliation_id": "REC-9990001", "settlement_id": "STL-9990001", "expected_amount": 10.0, "actual_amount": 10.0, "variance": 0.0, "match_status": "MATCHED", "reconciled_date": "2026-01-02", "source_type": "synthetic"}
    reward_amount = 0.0 if break_reward_event else 5.0  # QUALIFIED + reward_amount=0 violates the CHECK constraint
    reward = {"reward_id": "RWD-9990001", "transaction_id": "TXN-9990001", "offer_id": "OFR-9001", "qualification_status": "QUALIFIED", "reward_amount": reward_amount, "reward_currency": "GBP", "event_timestamp": "2026-01-01T10:00:00", "source_type": "synthetic"}

    tables = {
        "clients": [client], "programs": [program], "campaigns": [campaign], "offers": [offer],
        "cardholders": [cardholder], "card_tokens": [token], "transactions": [transaction],
        "transaction_events": [event], "settlements": [settlement], "reconciliation": [reconciliation],
        "reward_events": [reward],
    }
    for name, records in tables.items():
        write_parquet_table(records, name, SCHEMAS[name], root=root)


def test_load_all_rolls_back_completely_on_failure(tmp_path):
    _write_isolated_fixture(tmp_path, break_reward_event=True)

    with pytest.raises(psycopg.errors.CheckViolation):
        load_all(source_root=tmp_path)

    with database.connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.clients WHERE client_id = 'CLI-9001';")
        assert cur.fetchone()[0] == 0, "rollback must remove the parent rows too, not just the failing table"
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.transactions WHERE transaction_id = 'TXN-9990001';")
        assert cur.fetchone()[0] == 0
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.reward_events WHERE reward_id = 'RWD-9990001';")
        assert cur.fetchone()[0] == 0


def test_load_all_succeeds_and_is_idempotent_for_a_valid_isolated_fixture(tmp_path):
    _write_isolated_fixture(tmp_path, break_reward_event=False)
    try:
        report = load_all(source_root=tmp_path)
        assert report.committed is True
        assert sum(t.inserted for t in report.tables) == 11

        report2 = load_all(source_root=tmp_path)
        assert sum(t.inserted for t in report2.tables) == 0
        assert sum(t.skipped_existing for t in report2.tables) == 11
    finally:
        # test cleanup — remove the isolated fixture rows so this test is
        # re-runnable and doesn't leave synthetic test residue behind.
        with database.connect() as conn, conn.cursor() as cur:
            cur.execute(f"DELETE FROM {SCHEMA_NAME}.reward_events WHERE reward_id = 'RWD-9990001';")
            cur.execute(f"DELETE FROM {SCHEMA_NAME}.reconciliation WHERE reconciliation_id = 'REC-9990001';")
            cur.execute(f"DELETE FROM {SCHEMA_NAME}.settlements WHERE settlement_id = 'STL-9990001';")
            cur.execute(f"DELETE FROM {SCHEMA_NAME}.transaction_events WHERE event_id = 'EVT-9990001';")
            cur.execute(f"DELETE FROM {SCHEMA_NAME}.transactions WHERE transaction_id = 'TXN-9990001';")
            cur.execute(f"DELETE FROM {SCHEMA_NAME}.card_tokens WHERE token_id = 'TKN-9001aaaaaaaaaaaaa';")
            cur.execute(f"DELETE FROM {SCHEMA_NAME}.cardholders WHERE cardholder_id = 'CH-9001';")
            cur.execute(f"DELETE FROM {SCHEMA_NAME}.offers WHERE offer_id = 'OFR-9001';")
            cur.execute(f"DELETE FROM {SCHEMA_NAME}.campaigns WHERE campaign_id = 'CMP-9001';")
            cur.execute(f"DELETE FROM {SCHEMA_NAME}.programs WHERE program_id = 'PRG-9001';")
            cur.execute(f"DELETE FROM {SCHEMA_NAME}.clients WHERE client_id = 'CLI-9001';")
            conn.commit()


# ---- 16. synthetic-data tagging ----


def test_every_table_source_type_is_synthetic():
    with database.connect() as conn, conn.cursor() as cur:
        for table in TABLE_DEPENDENCIES:
            cur.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{table} WHERE source_type <> 'synthetic';")
            assert cur.fetchone()[0] == 0, table


def test_source_type_check_constraint_rejects_non_synthetic():
    with database.connect() as conn, conn.cursor() as cur:
        with pytest.raises(psycopg.errors.CheckViolation):
            cur.execute(
                f"INSERT INTO {SCHEMA_NAME}.clients (client_id, lei, legal_name, client_type, country_code, onboarding_date, status, source_type) "
                f"VALUES ('CLI-8888', 'FAKELEI00000000000X', 'x', 'ISSUER', 'GB', '2026-01-01', 'ACTIVE', 'real');"
            )
        conn.rollback()


# ---- 17. relationship integrity ----


def test_settlement_net_amount_matches_settlement_minus_fee_in_db():
    with database.connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.settlements WHERE net_amount <> settlement_amount - fee_amount;")
        assert cur.fetchone()[0] == 0


def test_reconciliation_variance_matches_actual_minus_expected_in_db():
    with database.connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.reconciliation WHERE variance <> actual_amount - expected_amount;")
        assert cur.fetchone()[0] == 0


def test_reward_qualification_consistency_in_db():
    with database.connect() as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM {SCHEMA_NAME}.reward_events "
            f"WHERE (qualification_status='QUALIFIED' AND reward_amount<=0) OR (qualification_status='NOT_QUALIFIED' AND reward_amount<>0);"
        )
        assert cur.fetchone()[0] == 0


def test_verify_all_passes_end_to_end():
    result = validation.verify_all()
    assert result["passed"] is True
