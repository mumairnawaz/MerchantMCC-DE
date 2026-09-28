"""silver_mcc — per docs/21-silver-data-contracts.md §3.1.

Simple overwrite: read latest Bronze mcc run, transform/validate every row, write a
single Parquet snapshot that fully replaces the previous one. No watermark, no
upsert — Bronze mcc has none, per §9.
"""

from pathlib import Path
from typing import Any

from src.silver import common, quarantine, validation

SOURCE_NAME = "mcc"
DATASET_NAME = "mcc"
TRANSFORM_VERSION = "v1"
PK_FIELD = "mcc_code"

MCC_CODE_PATTERN = r"^\d{4}$"


def _transform_record(raw: dict[str, str]) -> dict[str, Any]:
    return {
        "mcc_code": common.trim_or_none(raw.get("mcc")) or "",
        "description": common.trim_or_none(raw.get("edited_description")) or "",
        "description_combined": common.trim_or_none(raw.get("combined_description")),
        "description_usda": common.trim_or_none(raw.get("usda_description")),
        "description_irs": common.trim_or_none(raw.get("irs_description")),
        "irs_reportable": common.yes_no_to_bool(raw.get("irs_reportable")),
    }


def _validate_record(record: dict[str, Any]) -> dict[str, Any]:
    checks = [
        validation.check_required_field(record, "mcc_code"),
        validation.check_pattern(record, "mcc_code", MCC_CODE_PATTERN),
        validation.check_required_field(record, "description"),
    ]
    return validation.classify(checks)


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
    raw_records = common.read_bronze_csv(run_dir / metadata["raw_file"])
    lineage = common.build_lineage(
        source_name=SOURCE_NAME,
        run_dir=run_dir,
        ingestion_timestamp_utc=metadata["ingestion_timestamp_utc"],
        transform_version=TRANSFORM_VERSION,
    )
    bronze_run_id = common.bronze_run_id(run_dir)

    accepted: list[tuple[dict[str, str], dict[str, Any]]] = []
    rejected: list[dict[str, Any]] = []

    for raw in raw_records:
        transformed = _transform_record(raw)
        verdict = _validate_record(transformed)
        if verdict["accepted"]:
            accepted.append((raw, transformed))
        else:
            rejected.append(
                quarantine.build_rejection(
                    original_raw_record=raw,
                    error_reason="; ".join(c["name"] for c in verdict["hard_failures"]),
                    failing_check_name=verdict["hard_failures"][0]["name"],
                    source_bronze_run_id=bronze_run_id,
                    source_record_identifier=transformed.get(PK_FIELD) or raw.get("mcc"),
                )
            )

    # Defensive uniqueness enforcement (docs/21 §6): real Bronze mcc data has zero
    # duplicate mcc_code values, but the contract still requires this handled, not
    # assumed. A duplicate is quarantined rather than silently dropped.
    wrapped = [{"key": t[PK_FIELD], "raw": raw, "transformed": t} for raw, t in accepted]
    kept, duplicates = validation.dedupe_keep_first(wrapped, "key")
    for dup in duplicates:
        rejected.append(
            quarantine.build_rejection(
                original_raw_record=dup["raw"],
                error_reason=f"duplicate {PK_FIELD} '{dup['key']}' — defensive dedupe kept the first occurrence",
                failing_check_name=f"uniqueness_{PK_FIELD}",
                source_bronze_run_id=bronze_run_id,
                source_record_identifier=dup["key"],
            )
        )

    valid_records = [{**w["transformed"], **lineage} for w in kept]

    bronze_record_count = metadata["record_count"]
    # Duplicate mcc_code rows are quarantined above (uniqueness_mcc_code), so they're
    # already inside `rejected` — no separate "duplicates" bucket needed here.
    common.reconcile_counts(
        bronze_record_count,
        {"accepted": len(valid_records), "quarantined": len(rejected)},
        label=SOURCE_NAME,
    )

    silver_path = common.new_silver_output_path(DATASET_NAME, silver_root=silver_root)
    common.write_parquet(valid_records, silver_path)
    quarantine_path = quarantine.write_quarantine(DATASET_NAME, rejected, quarantine_root=quarantine_root)

    return {
        "silver_path": silver_path,
        "quarantine_path": quarantine_path,
        "bronze_run_id": bronze_run_id,
        "bronze_record_count": bronze_record_count,
        "valid_count": len(valid_records),
        "quarantined_count": len(rejected),
    }
