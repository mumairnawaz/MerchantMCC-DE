"""silver_iso_currency — per docs/21-silver-data-contracts.md §3.3.

Unlike mcc/country/card_issuer, iso_currency's Bronze source genuinely has multiple
rows per currency code by design (one row per Entity that uses that currency), so
deduplication here is *expected*, not defensive. The frozen contract rule:

    "Group by AlphabeticCode; representative row = first non-null Currency/MinorUnit."

This is implemented as field-independent first-non-null-wins across the group: for
each of currency_name, currency_numeric_code, minor_unit and withdrawal_date, the
first row in the group with a non-blank value for that field wins. Only
currency_name and minor_unit are explicitly named in the frozen rule; numeric_code
and withdrawal_date have no separately documented merge rule, so the same
first-non-null policy is applied to them for consistency — this is a stated
implementation decision, not a silent one (see the S3 completion report).

minor_unit blank-handling (docs/21 §14 item 5) is explicitly OPEN — no default of 2
is invented here. A blank MinorUnit (after merge) becomes NULL, per the project's
general "never invent a default" rule (§4).

Row-level validation only enforces the group key (AlphabeticCode) being present and
well-formed — the contract's merge rule presupposes individual rows may legitimately
have a blank Currency/MinorUnit, so `currency_name NOT NULL` is enforced only after
merging, against the group's representative record.
"""

from collections import defaultdict
from pathlib import Path
from typing import Any

from src.silver import common, quarantine, validation

SOURCE_NAME = "iso_currency"
DATASET_NAME = "iso_currency"
TRANSFORM_VERSION = "v1"
PK_FIELD = "currency_code"

CURRENCY_CODE_PATTERN = r"^[A-Z]{3}$"

_MERGED_FIELDS = ("currency_name", "currency_numeric_code", "minor_unit", "withdrawal_date")


def _transform_row(raw: dict[str, str]) -> dict[str, Any]:
    code = common.trim_or_none(raw.get("AlphabeticCode"))
    return {
        "currency_code": code.upper() if code is not None else "",
        "currency_name": common.trim_or_none(raw.get("Currency")),
        "currency_numeric_code": common.trim_or_none(raw.get("NumericCode")),
        "minor_unit": common.parse_int_or_none(raw.get("MinorUnit")),
        "withdrawal_date": common.trim_or_none(raw.get("WithdrawalDate")),
    }


def _validate_code(record: dict[str, Any]) -> dict[str, Any]:
    checks = [
        validation.check_required_field(record, "currency_code"),
        validation.check_pattern(record, "currency_code", CURRENCY_CODE_PATTERN),
    ]
    return validation.classify(checks)


def _merge_group(code: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """First-non-null-wins per field, independently, across all rows sharing `code`."""
    merged: dict[str, Any] = {"currency_code": code}
    for field in _MERGED_FIELDS:
        value = None
        for row in rows:
            if row.get(field) is not None:
                value = row[field]
                break
        merged[field] = value
    merged["is_active"] = merged["withdrawal_date"] is None
    return merged


def _validate_merged(record: dict[str, Any]) -> dict[str, Any]:
    checks = [validation.check_required_field(record, "currency_name")]
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

    rejected: list[dict[str, Any]] = []
    groups: dict[str, list[tuple[dict[str, str], dict[str, Any]]]] = defaultdict(list)

    for raw in raw_records:
        transformed = _transform_row(raw)
        verdict = _validate_code(transformed)
        if verdict["accepted"]:
            groups[transformed["currency_code"]].append((raw, transformed))
        else:
            rejected.append(
                quarantine.build_rejection(
                    original_raw_record=raw,
                    error_reason="; ".join(c["name"] for c in verdict["hard_failures"]),
                    failing_check_name=verdict["hard_failures"][0]["name"],
                    source_bronze_run_id=bronze_run_id,
                    source_record_identifier=transformed.get("currency_code") or raw.get("AlphabeticCode"),
                )
            )

    valid_records = []
    merged_duplicate_count = 0

    for code, pairs in groups.items():
        raws = [p[0] for p in pairs]
        transformed_rows = [p[1] for p in pairs]
        merged = _merge_group(code, transformed_rows)
        verdict = _validate_merged(merged)
        if verdict["accepted"]:
            valid_records.append({**merged, **lineage})
            merged_duplicate_count += len(pairs) - 1
        else:
            rejected.append(
                quarantine.build_rejection(
                    original_raw_record=raws,
                    error_reason="; ".join(c["name"] for c in verdict["hard_failures"]),
                    failing_check_name=verdict["hard_failures"][0]["name"],
                    source_bronze_run_id=bronze_run_id,
                    source_record_identifier=code,
                )
            )

    # rejected entries are either single raw rows (bad currency_code) or a list of
    # raw rows (a whole group rejected after merge) — count raw rows, not entries,
    # so this matches Bronze's own row-level record_count for reconciliation.
    quarantined_row_count = sum(len(r["original_raw_record"]) if isinstance(r["original_raw_record"], list) else 1 for r in rejected)

    bronze_record_count = metadata["record_count"]
    common.reconcile_counts(
        bronze_record_count,
        {
            "accepted": len(valid_records),
            "merged_duplicates": merged_duplicate_count,
            "quarantined": quarantined_row_count,
        },
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
        "merged_duplicate_count": merged_duplicate_count,
        "quarantined_count": quarantined_row_count,
    }
