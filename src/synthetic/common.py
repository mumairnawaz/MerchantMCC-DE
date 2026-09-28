"""Shared helpers for synthetic OLTP data generation: seeding, IDs, IO.

Determinism is the whole point of this package: the same seed against the same
Silver snapshot must always produce byte-identical output. No use of
datetime.now()/date.today() or unseeded randomness anywhere in src/synthetic/ —
every date is computed relative to REFERENCE_DATE below, and every random draw
goes through a `random.Random(SEED)` instance threaded explicitly through the
call chain, never the global `random` module.
"""

from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

# The fixed anchor for every relative date in the generated dataset. Using
# date.today() here would make output non-reproducible across days — this
# project's whole idempotency discipline (S7/S8) depends on determinism, and
# S9 explicitly requires it too.
REFERENCE_DATE = date(2026, 9, 21)

SEED = 20260921  # documented, fixed — see docs/22 §10

SYNTHETIC_ROOT = Path("data/synthetic_oltp")

SOURCE_TYPE = "synthetic"  # per docs/08's existing source_type convention (live_api/reference/synthetic)


def write_table(records: list[dict[str, Any]], name: str, schema: pa.Schema, root: Path = SYNTHETIC_ROOT) -> Path:
    """Every synthetic table gets an explicit schema (same discipline as
    src/silver/fx_rate.py, merchant.py, legal_entity.py) — never bare type
    inference for financial/identifier fields."""
    path = root / f"{name}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(table, path)
    return path


def read_table(name: str, root: Path = SYNTHETIC_ROOT) -> list[dict[str, Any]]:
    return pq.read_table(root / f"{name}.parquet").to_pylist()


def days_before_reference(days: int) -> date:
    return REFERENCE_DATE - timedelta(days=days)


def iso_date(d: date) -> str:
    return d.isoformat()
