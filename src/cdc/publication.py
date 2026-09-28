"""PostgreSQL-side CDC configuration: verifies wal_level/replication settings
and manages the dedicated `finpay_publication` publication.

Explicit table selection only (docs §5) — never `FOR ALL TABLES`, which would
publish every table in the database, including any future non-FinPay ones.
"""

from typing import Any

import psycopg

from src.oltp.config import OltpSettings, settings
from src.oltp.database import connect
from src.oltp.schema import SCHEMA_NAME, TABLE_DEPENDENCIES

PUBLICATION_NAME = "finpay_publication"

PUBLISHED_TABLES = sorted(TABLE_DEPENDENCIES)  # all 11 finpay OLTP tables, nothing else


def _scalar(conn: psycopg.Connection, sql: str, params: tuple = ()) -> Any:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


def check_replication_settings(cfg: OltpSettings = settings) -> dict[str, Any]:
    """Read-only — reports current wal_level/max_replication_slots/max_wal_senders
    without changing anything. wal_level is NOT settable at runtime (requires a
    server restart), so this only reports state; the actual value is set via
    docker/docker-compose.yml's postgres `command:` override (docs/24 §5)."""
    with connect(cfg) as conn:
        return {
            "wal_level": _scalar(conn, "SHOW wal_level;"),
            "max_replication_slots": int(_scalar(conn, "SHOW max_replication_slots;")),
            "max_wal_senders": int(_scalar(conn, "SHOW max_wal_senders;")),
        }


def publication_exists(conn: psycopg.Connection, name: str = PUBLICATION_NAME) -> bool:
    return bool(_scalar(conn, "SELECT COUNT(*) FROM pg_publication WHERE pubname = %s;", (name,)))


def published_tables(conn: psycopg.Connection, name: str = PUBLICATION_NAME) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT tablename FROM pg_publication_tables WHERE pubname = %s ORDER BY tablename;",
            (name,),
        )
        return [row[0] for row in cur.fetchall()]


def ensure_publication(cfg: OltpSettings = settings) -> dict[str, Any]:
    """Idempotent: creates finpay_publication over exactly the 11 finpay OLTP
    tables if it doesn't already exist; otherwise verifies its table set is
    still exactly right (raises if something else altered it) and leaves it
    untouched. Never drops/recreates a publication that already matches."""
    qualified_tables = ", ".join(f"{SCHEMA_NAME}.{t}" for t in PUBLISHED_TABLES)

    with connect(cfg) as conn:
        if not publication_exists(conn):
            with conn.cursor() as cur:
                cur.execute(f"CREATE PUBLICATION {PUBLICATION_NAME} FOR TABLE {qualified_tables};")
            conn.commit()
            created = True
        else:
            created = False

        current_tables = published_tables(conn)

    if current_tables != PUBLISHED_TABLES:
        raise RuntimeError(
            f"{PUBLICATION_NAME} exists but publishes {current_tables}, expected exactly {PUBLISHED_TABLES} — "
            f"not touching it automatically; investigate before proceeding."
        )

    return {"created": created, "publication": PUBLICATION_NAME, "tables": current_tables}


def replication_slot_info(cfg: OltpSettings = settings, slot_name: str | None = None) -> list[dict[str, Any]]:
    """Read-only. Debezium itself creates the replication slot on first
    connector start (docs/24 §8) — this module never creates one, only
    reports what exists, so a slot's presence/absence is real evidence of
    whether the connector has actually started streaming."""
    with connect(cfg) as conn, conn.cursor() as cur:
        if slot_name:
            cur.execute(
                "SELECT slot_name, plugin, slot_type, active, restart_lsn::text FROM pg_replication_slots WHERE slot_name = %s;",
                (slot_name,),
            )
        else:
            cur.execute("SELECT slot_name, plugin, slot_type, active, restart_lsn::text FROM pg_replication_slots;")
        columns = [d.name for d in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
