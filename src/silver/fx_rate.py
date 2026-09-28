"""silver_fx_rate — the first append/partitioned/time-series Silver dataset.

Bronze source: `currency` (Frankfurter). Grain: one row per
(rate_date, base_currency, quote_currency).

REAL-DATA FINDINGS (inspected before writing this module; see the S4 completion
report for the full write-up):

1. Only one real Bronze `currency` run exists on disk
   (data/bronze/currency/run_20260920T083149Z), and its raw `rates.json` is the
   FLAT shape: {amount, base, date, rates: {currency: rate}} — a single-date
   response. The NESTED/range shape ({start_date, end_date, rates: {date:
   {currency: rate}}}) is real, documented Frankfurter behavior (see
   src/ingestion/currency.py's `_is_range_response` and its own module docstring,
   and docs/21 §14 item 6) but has not yet been observed in any real Bronze run.
   Both shapes are implemented here (`_is_range_response`/`_flatten`), since both
   are genuinely documented in this project, not invented for this module; only
   the flat shape is exercised against real local data (see report §O).

2. Bronze's own `metadata.json.record_count` for `currency` is NOT "one record per
   API response" — src/ingestion/currency.py's `parse_for_metadata()` already
   flattens to one record per (date, currency) pair before counting (confirmed:
   real run's rates.json has 29 keys under `rates`, and metadata.json declares
   record_count=29). Because this project's Bronze `currency` module only ever
   returns a single `base` value per response, Bronze's declared record_count is
   therefore already at the exact same grain this Silver dataset needs per run
   (one Bronze "record" == one prospective Silver row for that run, before
   validation/quarantine/immutability outcomes are applied). This module does NOT
   trust that number blindly — it independently re-flattens the raw JSON itself
   (mirroring, not importing, that same flattening logic — Silver never imports
   src.ingestion, per docs/21 §1) and reconciles its own count against Bronze's
   declared record_count as a hard-fail check (see run()).

APPEND / IMMUTABILITY DESIGN:

Storage is one Parquet *part file per (partition, Bronze run)* that contributed
rows to that partition — data/silver/fx_rate/year=YYYY/month=MM/part-<bronze_run_id>.parquet.
A partition's full contents are the union of all its part files. This means an
already-published part file is NEVER reopened/rewritten by a later run — true
immutability at the file level, not just "we didn't change the values."

For each valid parsed row this run:
  - composite key (rate_date, base_currency, quote_currency) not yet published
    anywhere in that partition -> appended into this run's new part file.
  - key already published with an IDENTICAL exchange_rate -> counted as
    "unchanged" (the expected outcome of reprocessing the same Bronze run twice —
    a no-op, not an error, not written anywhere again).
  - key already published with a DIFFERENT exchange_rate -> a genuine conflict.
    Per the frozen contract, no silent "latest wins" is invented: the incoming
    row is quarantined (failing_check_name="immutability_conflict") and the
    already-published row is left untouched.

LINEAGE: exactly the 5 standard fields this S4 directive enumerates
(source_name, bronze_run_id, ingestion_timestamp_utc, silver_processed_at_utc,
silver_transform_version). docs/21 §11 additionally lists an optional
`source_updated_timestamp` "(=rate_date)" for fx_rate — omitted here since it
would just duplicate the `rate_date` column already on every row; noted for
review, not silently decided.
"""

from collections import defaultdict
from decimal import Decimal, InvalidOperation
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa

from src.silver import common, quarantine, validation

SOURCE_NAME = "currency"
DATASET_NAME = "fx_rate"
TRANSFORM_VERSION = "v1"

CURRENCY_CODE_PATTERN = r"^[A-Z]{3}$"

FX_RATE_SCHEMA = pa.schema(
    [
        ("rate_date", pa.date32()),
        ("base_currency", pa.string()),
        ("quote_currency", pa.string()),
        ("exchange_rate", pa.decimal128(18, 6)),
        ("source_name", pa.string()),
        ("bronze_run_id", pa.string()),
        ("ingestion_timestamp_utc", pa.string()),
        ("silver_processed_at_utc", pa.string()),
        ("silver_transform_version", pa.string()),
    ]
)


# ---- Bronze raw JSON shape handling (mirrors src/ingestion/currency.py's own
# _is_range_response/parse_for_metadata; not imported, per docs/21 §1) ----


def _is_range_response(raw: dict[str, Any]) -> bool:
    return "start_date" in raw and "end_date" in raw


def _flatten(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """One dict per (date, currency) pair, for both Frankfurter shapes."""
    base = raw.get("base")
    rates = raw.get("rates") or {}
    if _is_range_response(raw):
        return [
            {"date": rate_date, "base": base, "currency": code, "rate": rate}
            for rate_date, currency_map in rates.items()
            for code, rate in (currency_map or {}).items()
        ]
    rate_date = raw.get("date")
    return [{"date": rate_date, "base": base, "currency": code, "rate": rate} for code, rate in rates.items()]


# ---- transform ----


def _currency_or_none(value: Any) -> str | None:
    trimmed = common.trim_or_none(value)
    return trimmed.upper() if trimmed is not None else None


def _parse_date_or_none(value: Any) -> date | None:
    trimmed = common.trim_or_none(value)
    if trimmed is None:
        return None
    try:
        return date.fromisoformat(trimmed)
    except ValueError:
        return None


def _parse_decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _transform_record(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "rate_date": _parse_date_or_none(row.get("date")),
        "base_currency": _currency_or_none(row.get("base")),
        "quote_currency": _currency_or_none(row.get("currency")),
        "exchange_rate": _parse_decimal_or_none(row.get("rate")),
    }


# ---- validation ----


def _check_positive_decimal(record: dict[str, Any], field: str) -> dict[str, Any]:
    """Decimal-aware counterpart of validation.check_positive, which only accepts
    int/float. Mirrors its exact semantics (None fails, not just non-positive)."""
    value = record.get(field)
    passed = isinstance(value, Decimal) and value > 0
    return {
        "name": f"positive_{field}",
        "passed": passed,
        "severity": validation.HARD_FAIL,
        "details": {"field": field, "value": str(value) if value is not None else None},
    }


def _validate_record(record: dict[str, Any]) -> dict[str, Any]:
    checks = [
        validation.check_required_field(record, "rate_date"),
        validation.check_required_field(record, "base_currency"),
        validation.check_pattern(record, "base_currency", CURRENCY_CODE_PATTERN),
        validation.check_required_field(record, "quote_currency"),
        validation.check_pattern(record, "quote_currency", CURRENCY_CODE_PATTERN),
        validation.check_required_field(record, "exchange_rate"),
        _check_positive_decimal(record, "exchange_rate"),
    ]
    return validation.classify(checks)


# ---- partition I/O ----


def _partition_dir(silver_root: Path, rate_date: date) -> Path:
    return silver_root / DATASET_NAME / f"year={rate_date.year:04d}" / f"month={rate_date.month:02d}"


def _read_existing_partition(partition_dir: Path) -> dict[tuple[date, str, str], Decimal]:
    """Union of every already-published part file in this partition. Decimal
    equality is value-based (Decimal('0.9462') == Decimal('0.946200')), so scale
    differences between a freshly parsed rate and a decimal128(18,6) round-trip
    don't produce false conflicts."""
    existing: dict[tuple[date, str, str], Decimal] = {}
    if not partition_dir.exists():
        return existing
    for part_file in sorted(partition_dir.glob("part-*.parquet")):
        for r in common.read_parquet(part_file):
            key = (r["rate_date"], r["base_currency"], r["quote_currency"])
            existing[key] = r["exchange_rate"]
    return existing


def run(
    *,
    bronze_root: Path = common.BRONZE_ROOT,
    silver_root: Path = common.SILVER_ROOT,
    quarantine_root: Path = quarantine.QUARANTINE_ROOT,
) -> dict[str, Any]:
    run_dir = common.latest_bronze_run(SOURCE_NAME, bronze_root=bronze_root)
    if run_dir is None:
        raise FileNotFoundError(f"no Bronze run found for source '{SOURCE_NAME}'")

    metadata = common.read_bronze_metadata(run_dir)
    raw = common.read_bronze_json(run_dir / metadata["raw_file"])
    lineage = common.build_lineage(
        source_name=SOURCE_NAME,
        run_dir=run_dir,
        ingestion_timestamp_utc=metadata["ingestion_timestamp_utc"],
        transform_version=TRANSFORM_VERSION,
    )
    bronze_run_id = common.bronze_run_id(run_dir)

    flat_rows = _flatten(raw)

    accepted: list[tuple[dict[str, Any], dict[str, Any]]] = []
    rejected: list[dict[str, Any]] = []

    for row in flat_rows:
        transformed = _transform_record(row)
        verdict = _validate_record(transformed)
        if verdict["accepted"]:
            accepted.append((row, transformed))
        else:
            rejected.append(
                quarantine.build_rejection(
                    original_raw_record=row,
                    error_reason="; ".join(c["name"] for c in verdict["hard_failures"]),
                    failing_check_name=verdict["hard_failures"][0]["name"],
                    source_bronze_run_id=bronze_run_id,
                    source_record_identifier=f"{row.get('date')}:{row.get('base')}:{row.get('currency')}",
                )
            )

    by_partition: dict[tuple[int, int], list[tuple[dict[str, Any], dict[str, Any]]]] = defaultdict(list)
    for row, t in accepted:
        by_partition[(t["rate_date"].year, t["rate_date"].month)].append((row, t))

    appended_count = 0
    unchanged_count = 0
    conflict_count = 0
    written_paths: list[Path] = []

    for (_year, _month), pairs in by_partition.items():
        partition_dir = _partition_dir(silver_root, pairs[0][1]["rate_date"])
        existing = _read_existing_partition(partition_dir)
        to_write = []
        for row, t in pairs:
            key = (t["rate_date"], t["base_currency"], t["quote_currency"])
            if key not in existing:
                to_write.append({**t, **lineage})
                appended_count += 1
            elif existing[key] == t["exchange_rate"]:
                unchanged_count += 1
            else:
                conflict_count += 1
                rejected.append(
                    quarantine.build_rejection(
                        original_raw_record=row,
                        error_reason=(
                            f"immutability conflict: already-published exchange_rate={existing[key]} "
                            f"!= incoming {t['exchange_rate']} for {key}"
                        ),
                        failing_check_name="immutability_conflict",
                        source_bronze_run_id=bronze_run_id,
                        source_record_identifier=f"{t['rate_date']}:{t['base_currency']}:{t['quote_currency']}",
                    )
                )
        if to_write:
            part_path = partition_dir / f"part-{bronze_run_id}.parquet"
            if part_path.exists():
                # Should be unreachable: if this bronze_run_id already wrote this
                # partition, every one of its keys would already be in `existing`
                # above, leaving nothing in to_write. Kept as a hard safety net —
                # never silently overwrite a part file.
                raise RuntimeError(
                    f"{DATASET_NAME}: part file {part_path} already exists but new rows were "
                    f"computed for the same bronze_run_id — refusing to overwrite"
                )
            common.write_parquet(to_write, part_path, schema=FX_RATE_SCHEMA)
            written_paths.append(part_path)

    quarantine_path = quarantine.write_quarantine(DATASET_NAME, rejected, quarantine_root=quarantine_root)

    bronze_record_count = metadata["record_count"]
    # `rejected` already includes immutability conflicts alongside plain validation
    # failures (conflict_count is a reported subset, not a separate bucket) — an
    # immutability conflict is a quarantine outcome, per docs/21 and S7 §4.
    common.reconcile_counts(
        bronze_record_count,
        {"appended": appended_count, "unchanged": unchanged_count, "quarantined": len(rejected)},
        label=SOURCE_NAME,
    )

    return {
        "bronze_run_id": bronze_run_id,
        "bronze_record_count": bronze_record_count,
        "appended_count": appended_count,
        "unchanged_count": unchanged_count,
        "conflict_count": conflict_count,
        "quarantined_count": len(rejected),
        "quarantine_path": quarantine_path,
        "written_paths": written_paths,
    }
