"""silver_legal_entity — the second upserted Silver entity dataset (after
silver_merchant), sourced from the real GLEIF Bronze data.

Bronze source: `gleif`. Grain: one row per LEI (Legal Entity Identifier).
Primary/business key: `lei`.

REAL-DATA FINDINGS AND CONTRACT DISCREPANCIES (inspected before writing this
module; see the S6 completion report for the full write-up). This S6 prompt says
to implement "the schema from docs/21-silver-data-contracts.md" and then restates
a column list — but that restated list diverges from docs/21 §3.7 itself in two
places. Per the prompt's own instruction to verify real paths rather than assume
field names from the prompt text, and per the project's standing rule to treat
docs/21 as the frozen contract, this module follows docs/21 §3.7 (verified against
real data below) and reports the divergence rather than silently picking one:

1. ADDRESS SHAPE. This S6 prompt's Step 5 lists `legal_address_type`,
   `legal_address_first_address_line`, `legal_address_additional_address_line`
   (and the hq_ equivalents). Neither the real Bronze JSON nor docs/21 §3.7 has
   any of these. Every one of the 10,000 real records' `legalAddress`/
   `headquartersAddress` objects has no `type` field at all, and address lines
   are a single JSON array (`addressLines`, observed real lengths 1-4 elements),
   not split into "first"/"additional". docs/21 §3.7 already reflects this
   exactly: `legal_address_lines`/`hq_address_lines` (VARCHAR/ARRAY). Implemented
   as `pa.list_(pa.string())` columns, matching docs/21 and the real shape.

2. entity_creation_date. Present in docs/21 §3.7, absent from this prompt's Step
   5 list. Kept (docs/21 is authoritative and this is not an invented column).

3. entity_creation_date's real values carry a full time-of-day (e.g.
   "2012-09-24T05:00:00Z", not midnight) despite docs/21 typing the column DATE.
   Truncating to a bare date would silently discard real precision, so this
   module stores it as a full UTC timestamp instead of date32 — a stated choice,
   not a silent truncation.

4. source_updated_timestamp: not in docs/21 §3.7's own per-column table, but IS
   in docs/21 §11's lineage table, mapped `silver_legal_entity (=registration_
   last_update_date)` — and this S6 prompt explicitly asks for it. Implemented as
   a direct copy of the parsed `registration_last_update_date` value.

JURISDICTION (non-negotiable, per this S6 prompt and docs/21 §8): `entity_
jurisdiction` is trimmed only — never uppercased, never pattern-checked, never
split, never joined to silver_country. Verified against the real Bronze run: GB
(9,949), GB-SCT (34), GB-NIR (17) — all three preserved exactly by this module
(see the S6 completion report for the proof).

LEI VALIDATION: no LEI format check exists anywhere in src/ingestion/gleif.py or
its tests (Bronze only checks required-field presence and cross-page duplicates).
The pattern used here, `^[A-Z0-9]{20}$`, is independently verified against all
10,000 real LEIs in the Bronze run (0 exceptions) and matches docs/21's own
"real ISO 17442 shape confirmed against all 10,000 real records" note.

BRONZE IS MULTI-FILE for this source only (unlike every other Silver module so
far): `metadata.json["raw_file"]` is only `page_0001.json`; the full record set
spans all of `metadata.json["page_files"]` (50 files for the real run). This
module reads every page, not just `raw_file`.

UPSERT DESIGN: mirrors src/silver/merchant.py's model exactly (insert / unchanged
by record_hash / update on strictly-newer source timestamp / stale_skipped on
strictly-older / quarantined as `upsert_conflict` when timestamps are equal-but-
content-differs or when timestamp ordering can't be established on either side —
never an invented "latest wins"). Because `registration_last_update_date` is a
HARD_FAIL-required field here (unlike merchant's optional timestamp), the
missing-timestamp conflict branch is unreachable for any record that ever
actually reaches Silver — kept anyway as a defensive backstop, and exercised in
tests by directly constructing that state.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa

from src.silver import common, quarantine, validation

SOURCE_NAME = "gleif"
DATASET_NAME = "legal_entity"
TRANSFORM_VERSION = "v1"

LEI_PATTERN = r"^[A-Z0-9]{20}$"

BUSINESS_HASH_FIELDS = [
    "legal_name",
    "legal_address_country",
    "legal_address_city",
    "legal_address_region",
    "legal_address_postal_code",
    "legal_address_lines",
    "hq_address_country",
    "hq_address_city",
    "hq_address_region",
    "hq_address_postal_code",
    "hq_address_lines",
    "legal_form_id",
    "entity_category",
    "entity_status",
    "entity_jurisdiction",
    "entity_creation_date",
    "registration_status",
    "registration_next_renewal_date",
    "bic",
]

_TS = pa.timestamp("s")  # naive, implicitly UTC — see _parse_timestamp_or_none

LEGAL_ENTITY_SCHEMA = pa.schema(
    [
        ("lei", pa.string()),
        ("legal_name", pa.string()),
        ("legal_address_country", pa.string()),
        ("legal_address_city", pa.string()),
        ("legal_address_region", pa.string()),
        ("legal_address_postal_code", pa.string()),
        ("legal_address_lines", pa.list_(pa.string())),
        ("hq_address_country", pa.string()),
        ("hq_address_city", pa.string()),
        ("hq_address_region", pa.string()),
        ("hq_address_postal_code", pa.string()),
        ("hq_address_lines", pa.list_(pa.string())),
        ("legal_form_id", pa.string()),
        ("entity_category", pa.string()),
        ("entity_status", pa.string()),
        ("entity_jurisdiction", pa.string()),
        ("entity_creation_date", _TS),
        ("registration_initial_registration_date", _TS),
        ("registration_last_update_date", _TS),
        ("registration_status", pa.string()),
        ("registration_next_renewal_date", _TS),
        ("bic", pa.string()),
        ("source_updated_timestamp", _TS),
        ("record_hash", pa.string()),
        ("source_name", pa.string()),
        ("bronze_run_id", pa.string()),
        ("ingestion_timestamp_utc", pa.string()),
        ("silver_processed_at_utc", pa.string()),
        ("silver_transform_version", pa.string()),
    ]
)


# ---- Bronze multi-page reading ----


def _read_all_pages(run_dir: Path, metadata: dict[str, Any]) -> list[dict[str, Any]]:
    page_files = metadata.get("page_files") or [metadata["raw_file"]]
    records: list[dict[str, Any]] = []
    for filename in page_files:
        page = common.read_bronze_json(run_dir / filename)
        records.extend(page.get("data", []))
    return records


# ---- transform ----


def _upper_or_none(value: Any) -> str | None:
    trimmed = common.trim_or_none(value)
    return trimmed.upper() if trimmed is not None else None


def _parse_timestamp_or_none(value: Any) -> datetime | None:
    """Returns a naive datetime, implicitly UTC — matching this project's existing
    convention (ingestion_timestamp_utc/silver_processed_at_utc are naive too).
    Deliberately NOT tz-aware: pyarrow's tz-aware timestamp round-trip needs the
    zoneinfo/tzdata package, which isn't installed and isn't an approved new
    dependency for this phase — a real environment constraint discovered while
    testing, not a design preference."""
    trimmed = common.trim_or_none(value)
    if trimmed is None:
        return None
    try:
        dt = datetime.fromisoformat(trimmed)
    except ValueError:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _address_lines_or_none(address: dict[str, Any]) -> list[str] | None:
    raw_lines = address.get("addressLines") or []
    trimmed = [common.trim_or_none(line) for line in raw_lines]
    trimmed = [line for line in trimmed if line is not None]
    return trimmed if trimmed else None


def _transform_record(raw_record: dict[str, Any]) -> dict[str, Any]:
    attrs = raw_record.get("attributes") or {}
    entity = attrs.get("entity") or {}
    legal_addr = entity.get("legalAddress") or {}
    hq_addr = entity.get("headquartersAddress") or {}
    legal_name_obj = entity.get("legalName") or {}
    legal_form = entity.get("legalForm") or {}
    registration = attrs.get("registration") or {}

    last_update = _parse_timestamp_or_none(registration.get("lastUpdateDate"))

    return {
        "lei": common.trim_or_none(attrs.get("lei")),
        "legal_name": common.trim_or_none(legal_name_obj.get("name")),
        "legal_address_country": _upper_or_none(legal_addr.get("country")),
        "legal_address_city": common.trim_or_none(legal_addr.get("city")),
        "legal_address_region": common.trim_or_none(legal_addr.get("region")),
        "legal_address_postal_code": common.trim_or_none(legal_addr.get("postalCode")),
        "legal_address_lines": _address_lines_or_none(legal_addr),
        "hq_address_country": _upper_or_none(hq_addr.get("country")),
        "hq_address_city": common.trim_or_none(hq_addr.get("city")),
        "hq_address_region": common.trim_or_none(hq_addr.get("region")),
        "hq_address_postal_code": common.trim_or_none(hq_addr.get("postalCode")),
        "hq_address_lines": _address_lines_or_none(hq_addr),
        "legal_form_id": common.trim_or_none(legal_form.get("id")),
        "entity_category": common.trim_or_none(entity.get("category")),
        "entity_status": common.trim_or_none(entity.get("status")),
        # non-negotiable: trim only, never uppercased/split/pattern-checked (see module docstring)
        "entity_jurisdiction": common.trim_or_none(entity.get("jurisdiction")),
        "entity_creation_date": _parse_timestamp_or_none(entity.get("creationDate")),
        "registration_initial_registration_date": _parse_timestamp_or_none(registration.get("initialRegistrationDate")),
        "registration_last_update_date": last_update,
        "registration_status": common.trim_or_none(registration.get("status")),
        "registration_next_renewal_date": _parse_timestamp_or_none(registration.get("nextRenewalDate")),
        "bic": common.trim_or_none(attrs.get("bic")),
        "source_updated_timestamp": last_update,  # docs/21 §11: (=registration_last_update_date)
    }


# ---- validation ----


def _validate_record(record: dict[str, Any]) -> dict[str, Any]:
    checks = [
        validation.check_required_field(record, "lei"),
        validation.check_pattern(record, "lei", LEI_PATTERN),
        validation.check_required_field(record, "legal_name"),
        validation.check_required_field(record, "entity_status"),
        validation.check_required_field(record, "registration_status"),
        validation.check_required_field(record, "entity_jurisdiction"),
        validation.check_required_field(record, "registration_last_update_date"),
        validation.check_optional_field_present(record, "legal_address_country"),
        validation.check_optional_field_present(record, "legal_address_city"),
        validation.check_optional_field_present(record, "legal_address_region"),
        validation.check_optional_field_present(record, "legal_address_postal_code"),
        validation.check_optional_field_present(record, "legal_address_lines"),
        validation.check_optional_field_present(record, "hq_address_country"),
        validation.check_optional_field_present(record, "hq_address_city"),
        validation.check_optional_field_present(record, "hq_address_region"),
        validation.check_optional_field_present(record, "hq_address_postal_code"),
        validation.check_optional_field_present(record, "hq_address_lines"),
        validation.check_optional_field_present(record, "legal_form_id"),
        validation.check_optional_field_present(record, "entity_category"),
        validation.check_optional_field_present(record, "entity_creation_date"),
        validation.check_optional_field_present(record, "registration_initial_registration_date"),
        validation.check_optional_field_present(record, "registration_next_renewal_date"),
        validation.check_optional_field_present(record, "bic"),
    ]
    return validation.classify(checks)


def _lei_identifier(raw_record: dict[str, Any], transformed: dict[str, Any]) -> Any:
    return transformed.get("lei") or (raw_record.get("attributes") or {}).get("lei") or raw_record.get("id")


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
    raw_records = _read_all_pages(run_dir, metadata)
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
                    source_record_identifier=_lei_identifier(raw, transformed),
                )
            )

    silver_path = common.new_silver_output_path(DATASET_NAME, silver_root=silver_root)
    existing_rows = common.read_parquet(silver_path) if silver_path.exists() else []
    existing_by_lei = {r["lei"]: r for r in existing_rows}
    final_by_lei = dict(existing_by_lei)

    inserted_count = 0
    updated_count = 0
    unchanged_count = 0
    stale_skipped_count = 0

    for raw, t in accepted:
        record_hash = common.compute_record_hash(t, BUSINESS_HASH_FIELDS)
        full = {**t, "record_hash": record_hash, **lineage}
        key = t["lei"]
        # S7 hardening fix: look up against `final_by_lei` (kept in sync below), not
        # the frozen `existing_by_lei` snapshot — see the identical fix and full
        # rationale in src/silver/merchant.py. Two records sharing an LEI within the
        # same Bronze run were previously invisible to each other here, silently
        # last-write-wins overwriting instead of going through proper upsert
        # decisioning. Real Bronze GLEIF data has never had a duplicate LEI within
        # one run (0 duplicates confirmed across all 10,000 real records, S6
        # report), so this was latent, not observed — closed defensively here.
        existing = final_by_lei.get(key)

        if existing is None:
            final_by_lei[key] = full
            inserted_count += 1
            continue

        if existing["record_hash"] == record_hash:
            unchanged_count += 1
            continue

        e_ts, i_ts = existing.get("source_updated_timestamp"), t.get("source_updated_timestamp")
        if e_ts is not None and i_ts is not None:
            if i_ts > e_ts:
                final_by_lei[key] = full
                updated_count += 1
            elif i_ts < e_ts:
                stale_skipped_count += 1
            else:
                rejected.append(
                    quarantine.build_rejection(
                        original_raw_record=raw,
                        error_reason=(
                            f"same source_updated_timestamp ({i_ts}) as the already-published row "
                            f"for {key}, but content differs — no resolution rule defined"
                        ),
                        failing_check_name="upsert_conflict",
                        source_bronze_run_id=bronze_run_id,
                        source_record_identifier=key,
                    )
                )
        else:
            rejected.append(
                quarantine.build_rejection(
                    original_raw_record=raw,
                    error_reason=(
                        f"cannot determine freshness ordering for {key} — source_updated_timestamp "
                        f"missing on at least one side (existing={e_ts!r}, incoming={i_ts!r}) and content differs"
                    ),
                    failing_check_name="upsert_conflict",
                    source_bronze_run_id=bronze_run_id,
                    source_record_identifier=key,
                )
            )

    common.write_parquet(list(final_by_lei.values()), silver_path, schema=LEGAL_ENTITY_SCHEMA)
    quarantine_path = quarantine.write_quarantine(DATASET_NAME, rejected, quarantine_root=quarantine_root)

    bronze_record_count = metadata["record_count"]
    # No generic business-exclusion category for GLEIF — every Bronze record must
    # land in exactly one of these buckets.
    common.reconcile_counts(
        bronze_record_count,
        {
            "quarantined": len(rejected),
            "inserted": inserted_count,
            "updated": updated_count,
            "unchanged": unchanged_count,
            "stale_skipped": stale_skipped_count,
        },
        label=SOURCE_NAME,
    )

    return {
        "silver_path": silver_path,
        "quarantine_path": quarantine_path,
        "bronze_run_id": bronze_run_id,
        "bronze_record_count": bronze_record_count,
        "quarantined_count": len(rejected),
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "unchanged_count": unchanged_count,
        "stale_skipped_count": stale_skipped_count,
        "total_silver_rows": len(final_by_lei),
    }
