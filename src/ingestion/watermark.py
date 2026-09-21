"""Local, file-based watermark store for incremental ingestion.

This is the pre-Airflow, pre-database watermark mechanism for the current local
project stage. It is deliberately a narrow interface — `read_watermark` /
`write_watermark` — so it can be swapped for Airflow Variables or a PostgreSQL
`pipeline_watermark` table later without changing any ingestion module that calls
it; only this module's internals would need to change.

Watermark files live under data/watermarks/, alongside data/bronze/ as project-
local generated state — gitignored, not source-controlled, same as Bronze data.
"""

import json
from pathlib import Path

from src.ingestion.common import utc_now_iso

WATERMARK_ROOT = Path("data/watermarks")


def _watermark_path(source_name: str, root: Path = WATERMARK_ROOT) -> Path:
    return root / f"{source_name}.json"


def read_watermark(source_name: str, root: Path = WATERMARK_ROOT) -> str | None:
    """Return the persisted watermark for `source_name`, or None if no successful
    run has recorded one yet — callers treat None as "do a full load"."""
    path = _watermark_path(source_name, root)
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("watermark")


def write_watermark(source_name: str, watermark_value: str, *, run_dir: str, root: Path = WATERMARK_ROOT) -> Path:
    """Persist a new watermark. Callers must only invoke this after a successful,
    validated Bronze write — never on a failed request or a failed validation."""
    root.mkdir(parents=True, exist_ok=True)
    path = _watermark_path(source_name, root)
    payload = {
        "source": source_name,
        "watermark": watermark_value,
        "updated_at_utc": utc_now_iso(),
        "last_successful_run_dir": run_dir,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path
