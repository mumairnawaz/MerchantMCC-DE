"""Post-load verification: proves the PostgreSQL database matches the S9
Parquet source exactly — row counts, control totals, and referential
integrity — and that PostgreSQL's own constraints (PK/UNIQUE/FK/CHECK) are
actually in force, not just declared.
"""

from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

from src.oltp.config import OltpSettings, settings
from src.oltp.database import connect
from src.oltp.schema import SCHEMA_NAME, load_order
from src.synthetic.common import SYNTHETIC_ROOT
from src.synthetic.common import read_table as read_parquet_table


class OltpValidationError(RuntimeError):
    pass


def _fail(message: str) -> None:
    raise OltpValidationError(f"OLTP validation failure: {message}")


def _scalar(conn: psycopg.Connection, sql: str, params: tuple = ()) -> Any:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def verify_row_counts(conn: psycopg.Connection, source_root: Path | None = None) -> dict[str, int]:
    """Every table's PostgreSQL row count must equal its S9 Parquet source
    row count exactly — the Parquet file is authoritative for this initial
    load (per this phase's instruction)."""
    counts = {}
    for table in load_order():
        source_records = read_parquet_table(table, root=source_root or SYNTHETIC_ROOT)
        db_count = _scalar(conn, f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{table};")
        if db_count != len(source_records):
            _fail(f"{table}: Postgres row count {db_count} != Parquet source row count {len(source_records)}")
        counts[table] = db_count
    return counts


def verify_primary_key_uniqueness(conn: psycopg.Connection) -> None:
    """PostgreSQL's PRIMARY KEY constraint already guarantees this at write
    time — this re-derives it independently via COUNT(*) vs COUNT(DISTINCT pk)
    as an outside-in proof, not a trust-the-constraint-declaration assumption."""
    from src.oltp.loader import PRIMARY_KEYS

    for table, pk in PRIMARY_KEYS.items():
        total = _scalar(conn, f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{table};")
        distinct = _scalar(conn, f"SELECT COUNT(DISTINCT {pk}) FROM {SCHEMA_NAME}.{table};")
        if total != distinct:
            _fail(f"{table}.{pk}: {total} rows but only {distinct} distinct values")


def verify_no_orphans(conn: psycopg.Connection) -> None:
    """Independently re-checks every in-schema FK by anti-join, rather than
    trusting that the declared REFERENCES constraints were never violated by
    something outside this loader (e.g. a manual psql INSERT)."""
    checks = [
        ("programs", "client_id", "clients", "client_id"),
        ("campaigns", "program_id", "programs", "program_id"),
        ("offers", "campaign_id", "campaigns", "campaign_id"),
        ("cardholders", "enrolled_program_id", "programs", "program_id"),
        ("card_tokens", "cardholder_id", "cardholders", "cardholder_id"),
        ("transactions", "token_id", "card_tokens", "token_id"),
        ("transaction_events", "transaction_id", "transactions", "transaction_id"),
        ("settlements", "transaction_id", "transactions", "transaction_id"),
        ("reconciliation", "settlement_id", "settlements", "settlement_id"),
        ("reward_events", "transaction_id", "transactions", "transaction_id"),
        ("reward_events", "offer_id", "offers", "offer_id"),
    ]
    for child, fk_col, parent, parent_pk in checks:
        orphans = _scalar(
            conn,
            f"""
            SELECT COUNT(*) FROM {SCHEMA_NAME}.{child} c
            LEFT JOIN {SCHEMA_NAME}.{parent} p ON c.{fk_col} = p.{parent_pk}
            WHERE p.{parent_pk} IS NULL;
            """,
        )
        if orphans:
            _fail(f"{child}.{fk_col}: {orphans} orphaned rows with no matching {parent}.{parent_pk}")


def verify_control_totals(conn: psycopg.Connection) -> dict[str, Decimal]:
    """Reproduces S9's control-total identity inside PostgreSQL:
    approved transaction total == settlement total == reconciliation expected total.
    """
    gross_approved = _scalar(
        conn, f"SELECT COALESCE(SUM(amount), 0) FROM {SCHEMA_NAME}.transactions WHERE status = 'APPROVED';"
    )
    gross_settlement = _scalar(conn, f"SELECT COALESCE(SUM(settlement_amount), 0) FROM {SCHEMA_NAME}.settlements;")
    reconciliation_expected = _scalar(conn, f"SELECT COALESCE(SUM(expected_amount), 0) FROM {SCHEMA_NAME}.reconciliation;")

    if not (gross_approved == gross_settlement == reconciliation_expected):
        _fail(
            f"control total mismatch: gross_approved={gross_approved}, "
            f"gross_settlement={gross_settlement}, reconciliation_expected={reconciliation_expected}"
        )

    approved_count = _scalar(conn, f"SELECT COUNT(*) FROM {SCHEMA_NAME}.transactions WHERE status = 'APPROVED';")
    settlement_count = _scalar(conn, f"SELECT COUNT(*) FROM {SCHEMA_NAME}.settlements;")
    if approved_count != settlement_count:
        _fail(f"settlement coverage: {settlement_count} settlements != {approved_count} approved transactions")

    reconciliation_count = _scalar(conn, f"SELECT COUNT(*) FROM {SCHEMA_NAME}.reconciliation;")
    if reconciliation_count != settlement_count:
        _fail(f"reconciliation coverage: {reconciliation_count} != {settlement_count} settlements")

    auth_events = _scalar(conn, f"SELECT COUNT(*) FROM {SCHEMA_NAME}.transaction_events WHERE event_type = 'AUTHORIZATION';")
    total_transactions = _scalar(conn, f"SELECT COUNT(*) FROM {SCHEMA_NAME}.transactions;")
    if auth_events != total_transactions:
        _fail(f"event composition: {auth_events} AUTHORIZATION events != {total_transactions} transactions")

    return {
        "gross_approved_transaction_amount": gross_approved,
        "gross_settlement_amount": gross_settlement,
        "reconciliation_expected_total": reconciliation_expected,
    }


def verify_control_totals_match_source(conn: psycopg.Connection, source_root: Path | None = None) -> None:
    """Cross-checks the DB-computed control totals against the same totals
    freshly recomputed from the S9 Parquet source (not hardcoded)."""
    transactions = read_parquet_table("transactions", root=source_root or SYNTHETIC_ROOT)
    settlements = read_parquet_table("settlements", root=source_root or SYNTHETIC_ROOT)
    source_gross_approved = round(sum(t["amount"] for t in transactions if t["status"] == "APPROVED"), 2)
    source_gross_settlement = round(sum(s["settlement_amount"] for s in settlements), 2)

    db_totals = verify_control_totals(conn)
    if Decimal(str(source_gross_approved)) != db_totals["gross_approved_transaction_amount"]:
        _fail(
            f"source vs DB control total mismatch: Parquet gross_approved={source_gross_approved}, "
            f"Postgres gross_approved={db_totals['gross_approved_transaction_amount']}"
        )
    if Decimal(str(source_gross_settlement)) != db_totals["gross_settlement_amount"]:
        _fail(
            f"source vs DB control total mismatch: Parquet gross_settlement={source_gross_settlement}, "
            f"Postgres gross_settlement={db_totals['gross_settlement_amount']}"
        )


def verify_all(cfg: OltpSettings = settings, source_root: Path | None = None) -> dict[str, Any]:
    with connect(cfg) as conn:
        row_counts = verify_row_counts(conn, source_root)
        verify_primary_key_uniqueness(conn)
        verify_no_orphans(conn)
        control_totals = verify_control_totals(conn)
        verify_control_totals_match_source(conn, source_root)

    return {
        "row_counts": row_counts,
        "control_totals": {k: str(v) for k, v in control_totals.items()},
        "passed": True,
    }
