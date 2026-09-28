"""Reusable Silver-layer framework: Bronze discovery/reading, Parquet output, lineage.

Per docs/21-silver-data-contracts.md. This module provides only generic, dataset-
agnostic utilities — no business logic for any of the seven Silver datasets lives
here (that belongs to src/silver/<dataset>.py in later phases).

Reuses src.ingestion.common.utc_now_iso() rather than duplicating it — the same
convention already used by src/ingestion/watermark.py.
"""

import csv
import hashlib
import io
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from src.ingestion.common import utc_now_iso

BRONZE_ROOT = Path("data/bronze")
SILVER_ROOT = Path("data/silver")

# The 5 standard lineage fields frozen in docs/21-silver-data-contracts.md §11.
# Do not add fields here beyond what S1 approved.
STANDARD_LINEAGE_FIELDS = [
    "source_name",
    "bronze_run_id",
    "ingestion_timestamp_utc",
    "silver_processed_at_utc",
    "silver_transform_version",
]


# ---- Bronze discovery/reading ----


def list_bronze_runs(source_name: str, bronze_root: Path = BRONZE_ROOT) -> list[Path]:
    """All run directories for a source, oldest first. Bronze run directory names are
    UTC timestamps (run_YYYYMMDDTHHMMSSZ), so lexicographic sort is chronological.
    Returns an empty list if the source has never been ingested — not an error.
    """
    source_dir = bronze_root / source_name
    if not source_dir.exists():
        return []
    return sorted(p for p in source_dir.iterdir() if p.is_dir() and p.name.startswith("run_"))


def latest_bronze_run(source_name: str, bronze_root: Path = BRONZE_ROOT) -> Path | None:
    """The most recent Bronze run directory for a source, or None if it has never run."""
    runs = list_bronze_runs(source_name, bronze_root)
    return runs[-1] if runs else None


def bronze_run_id(run_dir: Path) -> str:
    """The bronze_run_id lineage value — literally the run directory's own name."""
    return run_dir.name


def read_bronze_metadata(run_dir: Path) -> dict[str, Any]:
    """Raises FileNotFoundError if metadata.json is missing — a Bronze run without
    metadata is a real problem, never silently treated as empty."""
    return json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))


def read_bronze_json(path: Path) -> Any:
    """Raises FileNotFoundError / json.JSONDecodeError on missing/malformed input —
    matching the existing Bronze ingestion modules' own fail-loudly convention."""
    return json.loads(path.read_text(encoding="utf-8"))


def read_bronze_csv(path: Path) -> list[dict[str, str]]:
    """Raises FileNotFoundError on missing input. Malformed CSV does not raise (csv.DictReader
    tolerates ragged rows) — mirrors src/ingestion/mcc.py's own parse_for_metadata() behavior."""
    reader = csv.DictReader(io.StringIO(path.read_text(encoding="utf-8")))
    return list(reader)


# ---- Lineage ----


def build_lineage(*, source_name: str, run_dir: Path, ingestion_timestamp_utc: str, transform_version: str) -> dict[str, str]:
    """The 5 standard lineage fields for one Silver row. Pure given its inputs except
    for the wall-clock silver_processed_at_utc stamp — everything else is deterministic.
    """
    return {
        "source_name": source_name,
        "bronze_run_id": bronze_run_id(run_dir),
        "ingestion_timestamp_utc": ingestion_timestamp_utc,
        "silver_processed_at_utc": utc_now_iso(),
        "silver_transform_version": transform_version,
    }


def compute_record_hash(record: dict[str, Any], fields: list[str]) -> str:
    """Deterministic hash over a defined field subset — cheap upsert change-detection
    for silver_merchant/silver_legal_entity. Same fields + same values -> same hash,
    regardless of dict key order (sort_keys=True)."""
    payload = json.dumps({f: record.get(f) for f in fields}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ---- Upsert/merge (generic mechanics only — no dataset-specific business rules) ----


def merge_upsert(existing: list[dict[str, Any]], incoming: list[dict[str, Any]], key_field: str, compare_field: str) -> list[dict[str, Any]]:
    """Merge incoming records into existing by key_field, keeping whichever row (per
    key) has the greater compare_field value (e.g. a timestamp). Used by the upsert-
    style datasets (silver_merchant, silver_legal_entity) — generic mechanics only,
    no OSM- or GLEIF-specific logic.
    """
    merged: dict[Any, dict[str, Any]] = {r[key_field]: r for r in existing}
    for r in incoming:
        k = r[key_field]
        if k not in merged or r.get(compare_field, "") >= merged[k].get(compare_field, ""):
            merged[k] = r
    return list(merged.values())


# ---- Silver output (Parquet via PyArrow) ----


def new_silver_output_path(dataset_name: str, filename: str = "data.parquet", silver_root: Path = SILVER_ROOT) -> Path:
    return silver_root / dataset_name / filename


def write_parquet(records: list[dict[str, Any]], path: Path, schema: pa.Schema | None = None) -> Path:
    """Write records (a list of flat dicts, one per row) to a Parquet file, creating
    parent directories as needed. If records is empty, an explicit schema must be
    provided — PyArrow cannot infer types from zero rows, and silently writing an
    untyped empty file would hide a real problem rather than surface it.
    """
    if not records and schema is None:
        raise ValueError("write_parquet: cannot infer a schema from an empty record list — pass schema= explicitly")
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(table, path)
    return path


def read_parquet(path: Path) -> list[dict[str, Any]]:
    return pq.read_table(path).to_pylist()


# ---- Generic value coercion helpers (dataset-agnostic; per docs/21 §4: empty/missing
# always becomes NULL, never an invented default) ----


def trim_or_none(value: Any) -> str | None:
    """"" / whitespace-only / None all collapse to None; anything else is trimmed."""
    if value is None:
        return None
    trimmed = str(value).strip()
    return trimmed if trimmed != "" else None


def yes_no_to_bool(value: Any) -> bool | None:
    """Case-insensitive "Yes"/"No" -> bool. Anything else (blank, other text) -> None,
    never guessed."""
    trimmed = trim_or_none(value)
    if trimmed is None:
        return None
    lowered = trimmed.lower()
    if lowered == "yes":
        return True
    if lowered == "no":
        return False
    return None


def parse_int_or_none(value: Any) -> int | None:
    """Blank/None -> None. A non-blank value that isn't a valid int is also None
    rather than raising — malformed numeric fields are a data-quality concern for the
    caller's own validation checks, not a parsing crash."""
    trimmed = trim_or_none(value)
    if trimmed is None:
        return None
    try:
        return int(trimmed)
    except ValueError:
        return None


# ---- Bronze -> Silver reconciliation (S7) ----


def reconcile_counts(bronze_count: int, dispositions: dict[str, int], *, label: str = "reconciliation") -> dict[str, Any]:
    """Every Bronze record must land in exactly one named disposition bucket — no
    dataset-specific knowledge here, callers supply whatever buckets their own
    semantics need (accepted/quarantined/excluded_x/merged/inserted/updated/...).
    Raises RuntimeError on ANY mismatch: an under-count means a record vanished,
    an over-count means one was double-counted into two buckets — both are bugs,
    never just logged. `dispositions` values must not overlap in what they count
    (a given Bronze record contributes to exactly one bucket); it is the caller's
    responsibility to bucket records this way before calling.
    """
    accounted_for = sum(dispositions.values())
    if accounted_for != bronze_count:
        raise RuntimeError(
            f"{label}: reconciliation mismatch — bronze_count={bronze_count}, "
            f"accounted_for={accounted_for}, dispositions={dispositions!r}"
        )
    return {
        "bronze_count": bronze_count,
        "accounted_for": accounted_for,
        "dispositions": dict(dispositions),
        "passed": True,
    }
