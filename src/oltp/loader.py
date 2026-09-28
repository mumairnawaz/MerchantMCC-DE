"""Loads S9's data/synthetic_oltp/*.parquet into the FinPay OLTP database.

STRATEGY (documented per this phase's explicit instruction not to silently
choose one): COPY each table's records into a per-transaction TEMP staging
table (fast bulk load — psycopg's binary COPY protocol, not row-by-row
INSERT), then `INSERT ... SELECT FROM staging ON CONFLICT (<primary key>) DO
NOTHING` into the real target table. This gives COPY's bulk-load speed for the
actual data transfer while still being idempotent: rerunning against an
unchanged Parquet snapshot finds every primary key already present and inserts
nothing new. Truncate-and-reload was considered and rejected — it doesn't fit
an OLTP system's semantics (a live operational database wouldn't truncate its
transaction ledger on every load) even though this particular run is a one-
time initial seed.

TRANSACTION SAFETY: the entire load — every table, in dependency order — runs
inside ONE database transaction. If any table fails partway through (e.g. a
constraint violation), the whole transaction rolls back and the database is
left exactly as it was before the run — never partially loaded. See
tests/test_oltp_loader.py::test_load_all_rolls_back_completely_on_failure.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

import psycopg

from src.oltp.config import OltpSettings, settings
from src.oltp.database import connect
from src.oltp.schema import SCHEMA_NAME, load_order
from src.synthetic.common import SYNTHETIC_ROOT
from src.synthetic.common import read_table as read_parquet_table

PRIMARY_KEYS: dict[str, str] = {
    "clients": "client_id",
    "programs": "program_id",
    "campaigns": "campaign_id",
    "offers": "offer_id",
    "cardholders": "cardholder_id",
    "card_tokens": "token_id",
    "transactions": "transaction_id",
    "transaction_events": "event_id",
    "settlements": "settlement_id",
    "reconciliation": "reconciliation_id",
    "reward_events": "reward_id",
}


def _to_date(value: Any) -> date | None:
    return date.fromisoformat(value) if value else None


def _to_timestamp(value: Any) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _to_decimal(value: Any) -> Decimal | None:
    # str(float) first, never Decimal(float) directly — avoids surfacing IEEE754
    # binary-float noise (e.g. Decimal(12.1) != Decimal("12.1")) for values that
    # were always meant to be clean 2-4 decimal-place amounts.
    return Decimal(str(value)) if value is not None else None


# Only columns that need conversion away from "pass the string through
# unchanged" are listed — every column not named here is TEXT/VARCHAR/CHAR and
# goes to Postgres as-is.
COLUMN_CONVERTERS: dict[str, dict[str, Callable[[Any], Any]]] = {
    "clients": {"onboarding_date": _to_date},
    "programs": {"start_date": _to_date},
    "campaigns": {"start_date": _to_date, "end_date": _to_date},
    "offers": {
        "offer_value": _to_decimal,
        "min_transaction_amount": _to_decimal,
        "valid_from": _to_date,
        "valid_to": _to_date,
    },
    "cardholders": {"enrollment_date": _to_date},
    "card_tokens": {"issued_date": _to_date},
    "transactions": {
        "mcc_confidence": _to_decimal,
        "amount": _to_decimal,
        "transaction_timestamp": _to_timestamp,
    },
    "transaction_events": {"event_timestamp": _to_timestamp},
    "settlements": {
        "settlement_date": _to_date,
        "settlement_amount": _to_decimal,
        "fee_amount": _to_decimal,
        "net_amount": _to_decimal,
    },
    "reconciliation": {
        "expected_amount": _to_decimal,
        "actual_amount": _to_decimal,
        "variance": _to_decimal,
        "reconciled_date": _to_date,
    },
    "reward_events": {"reward_amount": _to_decimal, "event_timestamp": _to_timestamp},
}


@dataclass
class TableLoadResult:
    table: str
    source_row_count: int
    inserted: int
    skipped_existing: int


@dataclass
class LoadReport:
    tables: list[TableLoadResult] = field(default_factory=list)
    committed: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "committed": self.committed,
            "tables": {t.table: {"source_row_count": t.source_row_count, "inserted": t.inserted, "skipped_existing": t.skipped_existing} for t in self.tables},
        }


def _coerce_records(table: str, records: list[dict[str, Any]], columns: list[str]) -> list[tuple[Any, ...]]:
    converters = COLUMN_CONVERTERS.get(table, {})
    rows = []
    for record in records:
        rows.append(tuple(converters[col](record[col]) if col in converters else record[col] for col in columns))
    return rows


def _load_one_table(conn: psycopg.Connection, table: str, records: list[dict[str, Any]]) -> TableLoadResult:
    pk = PRIMARY_KEYS[table]
    columns = list(records[0].keys()) if records else []
    qualified = f"{SCHEMA_NAME}.{table}"
    staging = f"staging_{table}"

    with conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE {staging} (LIKE {qualified} INCLUDING DEFAULTS) ON COMMIT DROP;")

        if records:
            column_list = ", ".join(columns)
            rows = _coerce_records(table, records, columns)
            with cur.copy(f"COPY {staging} ({column_list}) FROM STDIN") as copy:
                for row in rows:
                    copy.write_row(row)

        cur.execute(f"INSERT INTO {qualified} SELECT * FROM {staging} ON CONFLICT ({pk}) DO NOTHING;")
        inserted = cur.rowcount

    return TableLoadResult(table=table, source_row_count=len(records), inserted=inserted, skipped_existing=len(records) - inserted)


def load_all(*, cfg: OltpSettings = settings, source_root: Path = SYNTHETIC_ROOT) -> LoadReport:
    """Loads every table in the real FK dependency order (src.oltp.schema.load_order()),
    inside one all-or-nothing transaction. Returns a LoadReport; the caller can
    inspect it even if an exception propagates (the DB itself is rolled back,
    but the report object still reflects what was attempted before the
    failure — useful for the failure log, not a claim about DB state)."""
    report = LoadReport()
    order = load_order()

    with connect(cfg) as conn:
        try:
            for table in order:
                records = read_parquet_table(table, root=source_root)
                result = _load_one_table(conn, table, records)
                report.tables.append(result)
            conn.commit()
            report.committed = True
        except Exception:
            conn.rollback()
            report.committed = False
            raise

    return report
