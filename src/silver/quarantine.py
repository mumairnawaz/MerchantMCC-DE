"""Silver-layer quarantine, per docs/21-silver-data-contracts.md §7.

data/quarantine/<dataset>/run_<timestamp>/rejected.jsonl — append-only, one JSONL
file per rejected batch, created only when at least one record is actually
rejected. No silent deletion; every rejection carries enough context to debug
without needing to re-fetch Bronze.

Note: `original_raw_record` preserves whatever was rejected verbatim. None of this
project's seven Bronze sources contain secrets (all public reference/live data), so
no redaction logic exists here — if a future source ever could carry sensitive
values, that would need addressing before reusing this module for it, not assumed
safe by default.
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.ingestion.common import utc_now_iso

QUARANTINE_ROOT = Path("data/quarantine")


def _quarantine_run_id() -> str:
    # Timestamp for human readability/chronological sort, plus a short random suffix
    # for guaranteed uniqueness — verified live (this test suite) that microsecond-
    # formatted timestamps alone can still collide, since the OS clock's actual
    # resolution can be coarser than %f implies. A counter would only guard against
    # collisions within one process; a random suffix guards against it unconditionally.
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"run_{timestamp}_{uuid.uuid4().hex[:8]}"


def build_rejection(
    *,
    original_raw_record: Any,
    error_reason: str,
    failing_check_name: str,
    source_bronze_run_id: str,
    source_record_identifier: Any,
) -> dict[str, Any]:
    """One rejection record, matching the exact fields frozen in docs/21 §7."""
    return {
        "original_raw_record": original_raw_record,
        "error_reason": error_reason,
        "failing_check_name": failing_check_name,
        "source_bronze_run_id": source_bronze_run_id,
        "source_record_identifier": source_record_identifier,
        "rejected_at_utc": utc_now_iso(),
    }


def write_quarantine(dataset_name: str, rejected_records: list[dict[str, Any]], quarantine_root: Path = QUARANTINE_ROOT) -> Path | None:
    """Write rejected records as JSONL, one line per record. Returns the file path,
    or None if there was nothing to reject — no file is created for a clean run.
    """
    if not rejected_records:
        return None
    run_dir = quarantine_root / dataset_name / _quarantine_run_id()
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "rejected.jsonl"
    with path.open("w", encoding="utf-8") as f:
        for rec in rejected_records:
            f.write(json.dumps(rec, default=str) + "\n")
    return path


def read_quarantine(path: Path) -> list[dict[str, Any]]:
    records = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records
