"""Connection management and schema initialization for the FinPay OLTP database."""

from contextlib import contextmanager
from typing import Iterator

import psycopg

from src.oltp.config import OltpSettings, settings
from src.oltp.schema import DDL_STATEMENTS, INDEX_STATEMENTS


@contextmanager
def connect(cfg: OltpSettings = settings) -> Iterator[psycopg.Connection]:
    """Autocommit is OFF (psycopg's default) — callers control transaction
    boundaries explicitly, which src/oltp/loader.py relies on for its
    all-or-nothing load transaction (see docs/23 §10)."""
    conn = psycopg.connect(cfg.conninfo())
    try:
        yield conn
    finally:
        conn.close()


def health_check(cfg: OltpSettings = settings) -> bool:
    """True if the database is reachable and accepting queries. Never raises —
    callers that need the failure reason should call connect() directly."""
    try:
        with connect(cfg) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1;")
            return cur.fetchone() == (1,)
    except psycopg.OperationalError:
        return False


def init_schema(cfg: OltpSettings = settings) -> None:
    """Creates the finpay schema, all 11 tables, and all indexes. Every
    statement is CREATE ... IF NOT EXISTS — safe to rerun against an already-
    initialized database (idempotent DDL), never destructive (no DROP)."""
    with connect(cfg) as conn:
        with conn.cursor() as cur:
            for statement in DDL_STATEMENTS:
                cur.execute(statement)
            for statement in INDEX_STATEMENTS:
                cur.execute(statement)
        conn.commit()


def table_exists(conn: psycopg.Connection, table_name: str, cfg: OltpSettings = settings) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s);",
            (cfg.schema, table_name),
        )
        return cur.fetchone()[0]
