"""Shared helpers for bronze-layer ingestion: run directories and metadata capture."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BRONZE_ROOT = Path("data/bronze")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")


def new_run_dir(source_name: str, bronze_root: Path = BRONZE_ROOT) -> Path:
    """Create a fresh, timestamped run directory for a source. Never reuses/overwrites a prior run."""
    run_dir = bronze_root / source_name / _utc_run_id()
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def write_raw(run_dir: Path, filename: str, content: str) -> Path:
    """Write the source response verbatim, with no business transformation applied."""
    path = run_dir / filename
    path.write_text(content, encoding="utf-8")
    return path


def write_metadata(
    run_dir: Path,
    *,
    source_name: str,
    source_url: str,
    raw_filename: str,
    record_count: int,
    discovered_fields: list[str],
    validation_result: dict[str, Any],
    http_status: int | None = None,
    http_headers: dict[str, str] | None = None,
    extra: dict[str, Any] | None = None,
) -> Path:
    metadata = {
        "source_name": source_name,
        "source_url": source_url,
        "raw_file": raw_filename,
        "ingestion_timestamp_utc": utc_now_iso(),
        "record_count": record_count,
        "discovered_fields": discovered_fields,
        "http_status": http_status,
        "http_headers": http_headers or {},
        "validation": validation_result,
        "ingestion_tool": "merchantmcc-de/0.1 src.ingestion",
    }
    if extra:
        metadata.update(extra)
    path = run_dir / "metadata.json"
    path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return path
