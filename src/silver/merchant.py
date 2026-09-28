"""silver_merchant — the first genuinely upserted Silver entity dataset.

Bronze source: `merchant_osm` (OpenStreetMap via Overpass). Grain: one row per real
OSM node, identified by merchant_id = f"{osm_type}:{osm_id}" (never the bare numeric
id, which is only unique within one OSM object type).

REAL-DATA FINDINGS (inspected before writing this module; see the S5 completion
report for the full write-up):

1. The only real Bronze `merchant_osm` run (data/bronze/merchant_osm/run_20260920T083150Z,
   566 elements) was a FULL load using the `out body;` query in configs/sources.json.
   Every element is `{type, id, lat, lon, tags}` — confirmed NO top-level `timestamp`/
   `version`/`changeset` field on any of the 566 real elements. No watermark file
   exists yet for merchant_osm either, confirming no incremental (`out meta;`) run
   has ever happened. `out meta;` (configs/sources.json's `incremental_query_template`)
   is real, documented behavior of this project's own ingestion module, so this
   module still parses a top-level `timestamp` field when present — just not
   exercised against real local data (see report §T).

2. Two business-rule exclusions apply BEFORE validation, matching docs/21
   §3.5/§7 exactly: `tags.shop == "vacant"` (40 of 566 real elements) and missing
   `tags.name` (45 of 566 real elements). This S5 prompt's own Step 4 only
   describes the unnamed-record exclusion; `shop=vacant` is not mentioned there,
   but it IS already frozen (not an open item) in docs/21 §3.5/§7 — implementing
   it here follows the frozen contract rather than silently deviating from it.
   Flagged explicitly in the completion report since the S5 prompt's silence on
   it could otherwise look like an omission.

3. country_code: docs/21 §3.5 permits deriving it from "extract scope" but pairs
   that with a companion `country_code_source='assumed_from_extract_scope'`
   column that this S5 prompt's Step 2 schema does not list. Separately,
   configs/sources.json's merchant_osm entry has no structured ISO country-code
   field at all — only a free-text `area_name` ("Leeds city centre, UK (small
   extract)") and a bbox. Hardcoding "GB" from that free text, or from this
   assistant's own knowledge that the bbox is in the UK, would be exactly the
   kind of invented/assumed value the frozen contract prohibits. Per Step 8's own
   instruction ("if the contract does not [cleanly] permit this, leave NULL and
   report the conflict"), country_code is left NULL here — not decided silently.
   See the completion report.

4. latitude/longitude: docs/21 §3.5 declares type DECIMAL but — unlike
   silver_fx_rate's explicit DECIMAL(18,6) — gives no precision/scale for
   coordinates. Real values have up to 7 decimal digits (e.g. 53.8000274). Stored
   here as float64 (not a scaled decimal128) to avoid inventing an unspecified
   precision that could silently truncate real OSM precision — stated choice, not
   a silent one.

UPSERT DESIGN: a single Parquet snapshot (data/silver/merchant/data.parquet),
merged in place each run per docs/21 §12. Per incoming valid row, keyed by
merchant_id:
  - new key                                  -> insert
  - existing key, identical record_hash      -> unchanged (existing row untouched,
                                                 not even lineage-refreshed)
  - existing key, different content:
      - both sides have source_updated_timestamp, incoming strictly newer -> update
      - both sides have source_updated_timestamp, incoming strictly older -> skip
        (stale_skipped; not an error — just means Silver already has fresher data)
      - both sides have source_updated_timestamp, EQUAL, but content differs -> an
        unresolved same-timestamp conflict
      - either side is missing source_updated_timestamp and content differs -> the
        frozen contract defines no resolution rule for this case (real Bronze data
        never populates it today). Per this S5 prompt's own instruction, this is
        NOT resolved with an invented "latest wins" or bronze_run_id ordering — it
        is quarantined as failing_check_name="upsert_conflict" so nothing is
        silently lost or silently overwritten, and reported as an open decision.

record_hash covers only the mutable business/location/contact fields — never
merchant_id/osm_id/osm_type (that's identity, not content), never
source_updated_timestamp (that's freshness metadata, used separately for
ordering), and never silver_processed_at_utc, per Step 13.
"""

from pathlib import Path
from typing import Any

import pyarrow as pa

from src.silver import common, quarantine, validation

SOURCE_NAME = "merchant_osm"
DATASET_NAME = "merchant"
TRANSFORM_VERSION = "v1"

OSM_TYPE_PATTERN = r"^(node|way|relation)$"

BUSINESS_HASH_FIELDS = [
    "merchant_name",
    "raw_shop_tag",
    "raw_amenity_tag",
    "mcc_code",
    "latitude",
    "longitude",
    "address_city",
    "address_postcode",
    "address_street",
    "phone",
    "website",
    "opening_hours",
    "country_code",
]

MERCHANT_SCHEMA = pa.schema(
    [
        ("merchant_id", pa.string()),
        ("osm_id", pa.int64()),
        ("osm_type", pa.string()),
        ("merchant_name", pa.string()),
        ("raw_shop_tag", pa.string()),
        ("raw_amenity_tag", pa.string()),
        ("mcc_code", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("address_city", pa.string()),
        ("address_postcode", pa.string()),
        ("address_street", pa.string()),
        ("phone", pa.string()),
        ("website", pa.string()),
        ("opening_hours", pa.string()),
        ("country_code", pa.string()),
        ("source_updated_timestamp", pa.string()),
        ("record_hash", pa.string()),
        ("source_name", pa.string()),
        ("bronze_run_id", pa.string()),
        ("ingestion_timestamp_utc", pa.string()),
        ("silver_processed_at_utc", pa.string()),
        ("silver_transform_version", pa.string()),
    ]
)


# ---- transform ----


def _parse_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _transform_record(el: dict[str, Any]) -> dict[str, Any]:
    tags = el.get("tags") or {}
    osm_type = common.trim_or_none(el.get("type"))
    osm_id = el.get("id")
    osm_id = osm_id if isinstance(osm_id, int) and not isinstance(osm_id, bool) else None
    merchant_id = f"{osm_type}:{osm_id}" if osm_type is not None and osm_id is not None else None

    return {
        "merchant_id": merchant_id,
        "osm_id": osm_id,
        "osm_type": osm_type,
        "merchant_name": common.trim_or_none(tags.get("name")),
        "raw_shop_tag": common.trim_or_none(tags.get("shop")),
        "raw_amenity_tag": common.trim_or_none(tags.get("amenity")),
        "mcc_code": None,  # reserved — no approved OSM->MCC crosswalk exists (docs/21 §3.5)
        "latitude": _parse_float_or_none(el.get("lat")),
        "longitude": _parse_float_or_none(el.get("lon")),
        "address_city": common.trim_or_none(tags.get("addr:city")),
        "address_postcode": common.trim_or_none(tags.get("addr:postcode")),
        "address_street": common.trim_or_none(tags.get("addr:street")),
        "phone": common.trim_or_none(tags.get("phone")),
        "website": common.trim_or_none(tags.get("website")),
        "opening_hours": common.trim_or_none(tags.get("opening_hours")),
        "country_code": None,  # OPEN — see module docstring, finding 3
        "source_updated_timestamp": common.trim_or_none(el.get("timestamp")),  # top-level `out meta;` field
    }


# ---- validation ----


def _validate_record(record: dict[str, Any]) -> dict[str, Any]:
    checks = [
        validation.check_required_field(record, "merchant_id"),
        validation.check_required_field(record, "osm_id"),
        validation.check_required_field(record, "osm_type"),
        validation.check_pattern(record, "osm_type", OSM_TYPE_PATTERN),
        validation.check_required_field(record, "merchant_name"),
        validation.check_required_field(record, "latitude"),
        validation.check_numeric_range(record, "latitude", minimum=-90, maximum=90),
        validation.check_required_field(record, "longitude"),
        validation.check_numeric_range(record, "longitude", minimum=-180, maximum=180),
        validation.check_optional_field_present(record, "raw_shop_tag"),
        validation.check_optional_field_present(record, "raw_amenity_tag"),
        validation.check_optional_field_present(record, "address_city"),
        validation.check_optional_field_present(record, "address_postcode"),
        validation.check_optional_field_present(record, "address_street"),
        validation.check_optional_field_present(record, "phone"),
        validation.check_optional_field_present(record, "website"),
        validation.check_optional_field_present(record, "opening_hours"),
        validation.check_optional_field_present(record, "source_updated_timestamp"),
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
    raw = common.read_bronze_json(run_dir / metadata["raw_file"])
    lineage = common.build_lineage(
        source_name=SOURCE_NAME,
        run_dir=run_dir,
        ingestion_timestamp_utc=metadata["ingestion_timestamp_utc"],
        transform_version=TRANSFORM_VERSION,
    )
    bronze_run_id = common.bronze_run_id(run_dir)
    elements = raw.get("elements", [])

    excluded_vacant_shop = 0
    excluded_unnamed = 0
    accepted: list[tuple[dict[str, Any], dict[str, Any]]] = []
    rejected: list[dict[str, Any]] = []

    for el in elements:
        tags = el.get("tags") or {}
        shop = common.trim_or_none(tags.get("shop"))
        name = common.trim_or_none(tags.get("name"))

        if shop == "vacant":
            excluded_vacant_shop += 1
            continue
        if name is None:
            excluded_unnamed += 1
            continue

        transformed = _transform_record(el)
        verdict = _validate_record(transformed)
        if verdict["accepted"]:
            accepted.append((el, transformed))
        else:
            rejected.append(
                quarantine.build_rejection(
                    original_raw_record=el,
                    error_reason="; ".join(c["name"] for c in verdict["hard_failures"]),
                    failing_check_name=verdict["hard_failures"][0]["name"],
                    source_bronze_run_id=bronze_run_id,
                    source_record_identifier=f"{el.get('type')}:{el.get('id')}",
                )
            )

    silver_path = common.new_silver_output_path(DATASET_NAME, silver_root=silver_root)
    existing_rows = common.read_parquet(silver_path) if silver_path.exists() else []
    existing_by_id = {r["merchant_id"]: r for r in existing_rows}
    final_by_id = dict(existing_by_id)

    inserted_count = 0
    updated_count = 0
    unchanged_count = 0
    stale_skipped_count = 0

    for el, t in accepted:
        record_hash = common.compute_record_hash(t, BUSINESS_HASH_FIELDS)
        full = {**t, "record_hash": record_hash, **lineage}
        key = t["merchant_id"]
        # S7 hardening fix: look up against `final_by_id` (kept in sync below), not
        # the frozen `existing_by_id` snapshot. Two elements sharing a merchant_id
        # within the SAME Bronze run were previously invisible to each other here,
        # so the second silently last-write-wins overwrote the first via plain dict
        # assignment — bypassing insert/update/unchanged/stale/conflict decisioning
        # entirely (an invented latest-wins the frozen contract explicitly forbids)
        # and double-counting inserted_count without reconciliation catching it,
        # since each element still landed in exactly one bucket. Real Bronze data
        # has never had a duplicate merchant_id within one run (see the S5 report),
        # so this was latent, not observed — closed defensively here.
        existing = final_by_id.get(key)

        if existing is None:
            final_by_id[key] = full
            inserted_count += 1
            continue

        if existing["record_hash"] == record_hash:
            unchanged_count += 1
            continue

        e_ts, i_ts = existing.get("source_updated_timestamp"), t.get("source_updated_timestamp")
        if e_ts is not None and i_ts is not None:
            if i_ts > e_ts:
                final_by_id[key] = full
                updated_count += 1
            elif i_ts < e_ts:
                stale_skipped_count += 1
            else:
                rejected.append(
                    quarantine.build_rejection(
                        original_raw_record=el,
                        error_reason=(
                            f"same source_updated_timestamp ({i_ts}) as the already-published "
                            f"row for {key}, but content differs — no resolution rule defined"
                        ),
                        failing_check_name="upsert_conflict",
                        source_bronze_run_id=bronze_run_id,
                        source_record_identifier=key,
                    )
                )
        else:
            rejected.append(
                quarantine.build_rejection(
                    original_raw_record=el,
                    error_reason=(
                        f"cannot determine freshness ordering for {key} — source_updated_timestamp "
                        f"missing on at least one side (existing={e_ts!r}, incoming={i_ts!r}) and content differs"
                    ),
                    failing_check_name="upsert_conflict",
                    source_bronze_run_id=bronze_run_id,
                    source_record_identifier=key,
                )
            )

    common.write_parquet(list(final_by_id.values()), silver_path, schema=MERCHANT_SCHEMA)
    quarantine_path = quarantine.write_quarantine(DATASET_NAME, rejected, quarantine_root=quarantine_root)

    bronze_record_count = metadata["record_count"]
    # Each element is assigned to exactly one bucket: the vacant-shop check runs
    # (and `continue`s) before the unnamed check, so an element that is both
    # shop=vacant AND unnamed is only ever counted once, under excluded_vacant_shop
    # — never double-counted into both buckets. See test_run_vacant_and_unnamed_
    # overlap_is_not_double_counted for the regression proof.
    common.reconcile_counts(
        bronze_record_count,
        {
            "excluded_vacant_shop": excluded_vacant_shop,
            "excluded_unnamed": excluded_unnamed,
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
        "excluded_vacant_shop": excluded_vacant_shop,
        "excluded_unnamed": excluded_unnamed,
        "quarantined_count": len(rejected),
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "unchanged_count": unchanged_count,
        "stale_skipped_count": stale_skipped_count,
        "total_silver_rows": len(final_by_id),
    }
