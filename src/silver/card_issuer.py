"""silver_card_issuer — per docs/21-silver-data-contracts.md §3.4.

Simple overwrite: read latest Bronze card_issuer run, transform/validate every row,
write a single Parquet snapshot that fully replaces the previous one. No watermark,
no upsert — Bronze card_issuer has none, per §9.

issuer_name blank-handling (docs/21 §14 item 4) is explicitly OPEN — no 'Unknown'
sentinel is invented here. A blank Issuer becomes NULL, per the project's general
"never invent a default" rule (§4); has_known_issuer is derived so downstream
consumers can distinguish "genuinely unknown" without relying on a NULL check alone.
"""

from pathlib import Path
from typing import Any

from src.silver import common, quarantine, validation

SOURCE_NAME = "card_issuer"
DATASET_NAME = "card_issuer"
TRANSFORM_VERSION = "v1"
PK_FIELD = "bin_range"

BIN_PATTERN = r"^\d{6}$"


def _upper_or_none(value: Any) -> str | None:
    trimmed = common.trim_or_none(value)
    return trimmed.upper() if trimmed is not None else None


def _transform_record(raw: dict[str, str]) -> dict[str, Any]:
    issuer_name = common.trim_or_none(raw.get("Issuer"))
    return {
        "bin_range": common.trim_or_none(raw.get("BIN")) or "",
        "card_brand": common.trim_or_none(raw.get("Brand")) or "",
        "card_type": common.trim_or_none(raw.get("Type")),
        "card_category": common.trim_or_none(raw.get("Category")),
        "issuer_name": issuer_name,
        "has_known_issuer": issuer_name is not None,
        "issuer_phone": common.trim_or_none(raw.get("IssuerPhone")),
        "issuer_website": common.trim_or_none(raw.get("IssuerUrl")),
        "issuer_country_alpha2": _upper_or_none(raw.get("isoCode2")),
        "issuer_country_alpha3": _upper_or_none(raw.get("isoCode3")),
        "issuer_country_name": common.trim_or_none(raw.get("CountryName")),
    }


def _validate_record(record: dict[str, Any]) -> dict[str, Any]:
    checks = [
        validation.check_required_field(record, "bin_range"),
        validation.check_pattern(record, "bin_range", BIN_PATTERN),
        validation.check_required_field(record, "card_brand"),
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
                    source_record_identifier=transformed.get(PK_FIELD) or raw.get("BIN"),
                )
            )

    # Defensive uniqueness enforcement (docs/21 §6): real Bronze card_issuer data has
    # zero duplicate BIN values, but the contract still requires this handled, not
    # assumed.
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
    # Duplicate bin_range rows are quarantined above (uniqueness_bin_range), so
    # they're already inside `rejected` — no separate "duplicates" bucket needed here.
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
