"""silver_country — per docs/21-silver-data-contracts.md §3.2.

Simple overwrite: read latest Bronze country run (a single JSON array), transform/
validate every element, write a single Parquet snapshot that fully replaces the
previous one. No watermark, no upsert — Bronze country has none, per §9.
"""

from pathlib import Path
from typing import Any

from src.silver import common, quarantine, validation

SOURCE_NAME = "country"
DATASET_NAME = "country"
TRANSFORM_VERSION = "v1"
PK_FIELD = "country_code_alpha3"

ALPHA3_PATTERN = r"^[A-Z]{3}$"
ALPHA2_PATTERN = r"^[A-Z]{2}$"


def _upper_or_none(value: Any) -> str | None:
    trimmed = common.trim_or_none(value)
    return trimmed.upper() if trimmed is not None else None


def _transform_record(raw: dict[str, Any]) -> dict[str, Any]:
    name = raw.get("name") or {}
    capital_list = raw.get("capital")
    capital = capital_list[0] if isinstance(capital_list, list) and capital_list else None

    currencies = raw.get("currencies")
    default_currency_code = next(iter(currencies), None) if isinstance(currencies, dict) and currencies else None

    independent = raw.get("independent")
    is_independent = independent if isinstance(independent, bool) else None
    un_member = raw.get("unMember")
    is_un_member = un_member if isinstance(un_member, bool) else None

    return {
        "country_code_alpha3": _upper_or_none(raw.get("cca3")) or "",
        "country_code_alpha2": _upper_or_none(raw.get("cca2")) or "",
        "country_code_numeric": common.trim_or_none(raw.get("ccn3")),
        "country_name": common.trim_or_none(name.get("common")) or "",
        "country_official_name": common.trim_or_none(name.get("official")),
        "region": common.trim_or_none(raw.get("region")),
        "subregion": common.trim_or_none(raw.get("subregion")),
        "capital": common.trim_or_none(capital),
        "default_currency_code": _upper_or_none(default_currency_code),
        "is_independent": is_independent,
        "is_un_member": is_un_member,
    }


def _validate_record(record: dict[str, Any]) -> dict[str, Any]:
    checks = [
        validation.check_required_field(record, "country_code_alpha3"),
        validation.check_pattern(record, "country_code_alpha3", ALPHA3_PATTERN),
        validation.check_required_field(record, "country_code_alpha2"),
        validation.check_pattern(record, "country_code_alpha2", ALPHA2_PATTERN),
        validation.check_required_field(record, "country_name"),
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
    raw_records = common.read_bronze_json(run_dir / metadata["raw_file"])
    lineage = common.build_lineage(
        source_name=SOURCE_NAME,
        run_dir=run_dir,
        ingestion_timestamp_utc=metadata["ingestion_timestamp_utc"],
        transform_version=TRANSFORM_VERSION,
    )
    bronze_run_id = common.bronze_run_id(run_dir)

    accepted: list[tuple[dict[str, Any], dict[str, Any]]] = []
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
                    source_record_identifier=transformed.get(PK_FIELD) or raw.get("cca3"),
                )
            )

    # Defensive uniqueness enforcement (docs/21 §6): real Bronze country data has zero
    # duplicate cca3 values, but the contract still requires this handled, not assumed.
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
    # Duplicate cca3 rows are quarantined above (uniqueness_country_code_alpha3), so
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
