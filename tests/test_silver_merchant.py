import json
from pathlib import Path

from src.silver import common, merchant, quarantine

NAMED_NODE = {
    "type": "node",
    "id": 1,
    "lat": 53.8000274,
    "lon": -1.5481087,
    "tags": {"name": "Test Bakery", "shop": "bakery", "phone": "+44 113 000 0000"},
}
UNNAMED = {"type": "node", "id": 2, "lat": 53.8, "lon": -1.54, "tags": {"shop": "newsagent"}}
VACANT = {"type": "node", "id": 3, "lat": 53.8, "lon": -1.54, "tags": {"name": "Old Shop", "shop": "vacant"}}
ADDRESS_FULL = {
    "type": "node",
    "id": 4,
    "lat": 53.8,
    "lon": -1.54,
    "tags": {
        "name": "Full Addr Cafe",
        "amenity": "cafe",
        "addr:city": "Leeds",
        "addr:postcode": "LS1 1AA",
        "addr:street": "High St",
        "website": "https://example.com",
        "opening_hours": "Mo-Fr 09:00-17:00",
    },
}
BAD_LAT = {"type": "node", "id": 5, "lat": 200, "lon": -1.54, "tags": {"name": "Bad Coord Shop"}}
MISSING_COORD = {"type": "node", "id": 6, "tags": {"name": "No Coord Shop"}}
META_NODE = {
    "type": "node",
    "id": 7,
    "lat": 53.8,
    "lon": -1.54,
    "timestamp": "2026-09-20T10:00:00Z",
    "tags": {"name": "Meta Shop"},
}


def _make_bronze_run(bronze_root: Path, run_id: str, elements: list, record_count: int | None = None) -> Path:
    run_dir = bronze_root / "merchant_osm" / run_id
    run_dir.mkdir(parents=True)
    metadata = {
        "raw_file": "osm_places.json",
        "record_count": len(elements) if record_count is None else record_count,
        "ingestion_timestamp_utc": "2026-09-20T08:31:50Z",
    }
    raw = {
        "version": 0.6,
        "generator": "test",
        "osm3s": {"timestamp_osm_base": "2026-09-20T08:30:16Z"},
        "elements": elements,
    }
    (run_dir / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    (run_dir / "osm_places.json").write_text(json.dumps(raw), encoding="utf-8")
    return run_dir


def _run(bronze_root, silver_root, quarantine_root):
    return merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)


# ---- transform: identity, name, tags, coords (2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 15, 16, 17, 18) ----


def test_merchant_id_construction():
    record = merchant._transform_record(NAMED_NODE)
    assert record["merchant_id"] == "node:1"


def test_node_and_way_with_same_numeric_id_are_different_merchant_ids():
    node_record = merchant._transform_record({"type": "node", "id": 123, "lat": 1, "lon": 1, "tags": {"name": "X"}})
    way_record = merchant._transform_record({"type": "way", "id": 123, "lat": 1, "lon": 1, "tags": {"name": "X"}})
    assert node_record["merchant_id"] == "node:123"
    assert way_record["merchant_id"] == "way:123"
    assert node_record["merchant_id"] != way_record["merchant_id"]


def test_transform_extracts_name_shop_amenity():
    record = merchant._transform_record(NAMED_NODE)
    assert record["merchant_name"] == "Test Bakery"
    assert record["raw_shop_tag"] == "bakery"

    amenity_record = merchant._transform_record(ADDRESS_FULL)
    assert amenity_record["raw_amenity_tag"] == "cafe"


def test_transform_preserves_multivalued_shop_tag_unsplit():
    el = {"type": "node", "id": 99, "lat": 1, "lon": 1, "tags": {"name": "Gallery", "shop": "art;gift"}}
    assert merchant._transform_record(el)["raw_shop_tag"] == "art;gift"


def test_transform_extracts_address_fields():
    record = merchant._transform_record(ADDRESS_FULL)
    assert record["address_city"] == "Leeds"
    assert record["address_postcode"] == "LS1 1AA"
    assert record["address_street"] == "High St"


def test_transform_extracts_phone():
    assert merchant._transform_record(NAMED_NODE)["phone"] == "+44 113 000 0000"


def test_transform_extracts_website():
    assert merchant._transform_record(ADDRESS_FULL)["website"] == "https://example.com"


def test_transform_extracts_opening_hours():
    assert merchant._transform_record(ADDRESS_FULL)["opening_hours"] == "Mo-Fr 09:00-17:00"


def test_transform_extracts_coordinates_as_float():
    record = merchant._transform_record(NAMED_NODE)
    assert record["latitude"] == 53.8000274
    assert record["longitude"] == -1.5481087
    assert isinstance(record["latitude"], float)


def test_transform_missing_coordinates_become_none():
    record = merchant._transform_record(MISSING_COORD)
    assert record["latitude"] is None
    assert record["longitude"] is None


def test_transform_mcc_code_always_none():
    assert merchant._transform_record(NAMED_NODE)["mcc_code"] is None
    assert merchant._transform_record(ADDRESS_FULL)["mcc_code"] is None


def test_transform_country_code_always_none():
    # OPEN per module docstring finding 3 — no invented "GB" from scope.
    assert merchant._transform_record(NAMED_NODE)["country_code"] is None


def test_transform_source_updated_timestamp_extracted_when_present():
    assert merchant._transform_record(META_NODE)["source_updated_timestamp"] == "2026-09-20T10:00:00Z"


def test_transform_source_updated_timestamp_none_when_absent():
    assert merchant._transform_record(NAMED_NODE)["source_updated_timestamp"] is None


def test_transform_missing_optional_tags_become_none_not_empty_string():
    record = merchant._transform_record(NAMED_NODE)
    assert record["address_city"] is None
    assert record["opening_hours"] is None
    assert record["raw_amenity_tag"] is None


# ---- validation: coordinates (12) ----


def test_validate_rejects_out_of_range_latitude():
    record = merchant._transform_record(BAD_LAT)
    assert merchant._validate_record(record)["accepted"] is False


def test_validate_rejects_missing_coordinates():
    record = merchant._transform_record(MISSING_COORD)
    assert merchant._validate_record(record)["accepted"] is False


def test_validate_accepts_well_formed_record():
    record = merchant._transform_record(NAMED_NODE)
    assert merchant._validate_record(record)["accepted"] is True


def test_validate_rejects_malformed_osm_type():
    el = dict(NAMED_NODE, type="relationship")  # not node/way/relation
    record = merchant._transform_record(el)
    assert merchant._validate_record(record)["accepted"] is False


# ---- end-to-end run(): exclusions, quarantine, reconciliation, lineage, first upsert (1, 13, 19, 20, 21, 28) ----


def test_run_first_upsert_creates_initial_snapshot(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [NAMED_NODE, ADDRESS_FULL])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["inserted_count"] == 2
    assert result["total_silver_rows"] == 2
    rows = common.read_parquet(result["silver_path"])
    assert {r["merchant_id"] for r in rows} == {"node:1", "node:4"}


def test_run_excludes_unnamed_records(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [NAMED_NODE, UNNAMED])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["excluded_unnamed"] == 1
    assert result["inserted_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert len(rows) == 1


def test_run_excludes_vacant_shops(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [NAMED_NODE, VACANT])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["excluded_vacant_shop"] == 1
    assert result["inserted_count"] == 1


def test_run_quarantines_bad_coordinates(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [NAMED_NODE, BAD_LAT, MISSING_COORD])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["quarantined_count"] == 2
    rejected = quarantine.read_quarantine(result["quarantine_path"])
    identifiers = {r["source_record_identifier"] for r in rejected}
    assert identifiers == {"node:5", "node:6"}


def test_run_reconciliation_matches_bronze_record_count(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [NAMED_NODE, UNNAMED, VACANT, BAD_LAT])
    result = _run(bronze_root, silver_root, quarantine_root)

    total = (
        result["excluded_vacant_shop"]
        + result["excluded_unnamed"]
        + result["quarantined_count"]
        + result["inserted_count"]
        + result["updated_count"]
        + result["unchanged_count"]
        + result["stale_skipped_count"]
    )
    assert total == result["bronze_record_count"]


def test_run_lineage_fields_present(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [NAMED_NODE])
    result = _run(bronze_root, silver_root, quarantine_root)
    rows = common.read_parquet(result["silver_path"])
    for field in common.STANDARD_LINEAGE_FIELDS:
        assert field in rows[0]
    assert rows[0]["source_name"] == "merchant_osm"
    assert rows[0]["bronze_run_id"] == "run_1"
    assert "record_hash" in rows[0]


# ---- upsert semantics (22, 23, 24, 25, 26, 27) ----


def test_run_new_merchant_in_second_run_is_inserted(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [NAMED_NODE])
    _run(bronze_root, silver_root, quarantine_root)

    _make_bronze_run(bronze_root, "run_2", [ADDRESS_FULL])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["inserted_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert {r["merchant_id"] for r in rows} == {"node:1", "node:4"}


def test_run_newer_source_update_overwrites_existing(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    v1 = dict(META_NODE, id=10, tags={"name": "Shop V1", "phone": "111"})
    v2_newer = dict(META_NODE, id=10, timestamp="2026-09-21T10:00:00Z", tags={"name": "Shop V2", "phone": "222"})

    _make_bronze_run(bronze_root, "run_1", [v1])
    _run(bronze_root, silver_root, quarantine_root)

    _make_bronze_run(bronze_root, "run_2", [v2_newer])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["updated_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert rows[0]["phone"] == "222"
    assert rows[0]["merchant_name"] == "Shop V2"


def test_run_older_source_update_does_not_overwrite(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    v1 = dict(META_NODE, id=10, tags={"name": "Shop V1", "phone": "111"})
    v2_older = dict(META_NODE, id=10, timestamp="2026-09-19T10:00:00Z", tags={"name": "Shop V2 Older", "phone": "333"})

    _make_bronze_run(bronze_root, "run_1", [v1])
    _run(bronze_root, silver_root, quarantine_root)

    _make_bronze_run(bronze_root, "run_2", [v2_older])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["stale_skipped_count"] == 1
    assert result["updated_count"] == 0
    rows = common.read_parquet(result["silver_path"])
    assert rows[0]["phone"] == "111"  # original preserved, not overwritten


def test_run_unchanged_content_is_a_true_no_op(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [NAMED_NODE])
    first = _run(bronze_root, silver_root, quarantine_root)
    before_rows = common.read_parquet(first["silver_path"])

    _make_bronze_run(bronze_root, "run_2", [NAMED_NODE])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["unchanged_count"] == 1
    assert result["inserted_count"] == 0
    assert result["updated_count"] == 0
    after_rows = common.read_parquet(result["silver_path"])
    assert after_rows == before_rows  # byte-for-byte untouched, including lineage


def test_run_same_bronze_run_reprocessed_twice_is_idempotent(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [NAMED_NODE, ADDRESS_FULL])
    _run(bronze_root, silver_root, quarantine_root)
    result2 = _run(bronze_root, silver_root, quarantine_root)

    assert result2["inserted_count"] == 0
    assert result2["unchanged_count"] == 2
    rows = common.read_parquet(result2["silver_path"])
    assert len(rows) == 2  # not duplicated


def test_run_duplicate_merchant_id_cannot_exist_across_runs(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    v1 = dict(META_NODE, id=10, tags={"name": "Shop V1"})
    v2_newer = dict(META_NODE, id=10, timestamp="2026-09-21T10:00:00Z", tags={"name": "Shop V2"})

    _make_bronze_run(bronze_root, "run_1", [v1])
    _run(bronze_root, silver_root, quarantine_root)
    _make_bronze_run(bronze_root, "run_2", [v2_newer])
    result = _run(bronze_root, silver_root, quarantine_root)

    rows = common.read_parquet(result["silver_path"])
    ids = [r["merchant_id"] for r in rows]
    assert len(ids) == len(set(ids))


# ---- unresolved-conflict paths (explicit STOP-and-report behavior, not invented latest-wins) ----


def test_run_same_timestamp_different_content_is_quarantined_as_conflict(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    v1 = dict(META_NODE, id=10, tags={"name": "Shop V1"})
    v_conflict = dict(META_NODE, id=10, tags={"name": "Shop Conflicting"})  # same timestamp, different name

    _make_bronze_run(bronze_root, "run_1", [v1])
    _run(bronze_root, silver_root, quarantine_root)
    _make_bronze_run(bronze_root, "run_2", [v_conflict])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["updated_count"] == 0
    assert result["stale_skipped_count"] == 0
    rejected = quarantine.read_quarantine(result["quarantine_path"])
    assert any(r["failing_check_name"] == "upsert_conflict" for r in rejected)
    rows = common.read_parquet(result["silver_path"])
    assert rows[0]["merchant_name"] == "Shop V1"  # untouched


def test_run_missing_timestamp_ordering_is_quarantined_not_latest_wins(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    v1 = {"type": "node", "id": 20, "lat": 1, "lon": 1, "tags": {"name": "No Timestamp V1"}}
    v2 = {"type": "node", "id": 20, "lat": 1, "lon": 1, "tags": {"name": "No Timestamp V2"}}

    _make_bronze_run(bronze_root, "run_1", [v1])
    _run(bronze_root, silver_root, quarantine_root)
    _make_bronze_run(bronze_root, "run_2", [v2])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["updated_count"] == 0
    assert result["stale_skipped_count"] == 0
    rejected = quarantine.read_quarantine(result["quarantine_path"])
    assert any(r["failing_check_name"] == "upsert_conflict" for r in rejected)
    rows = common.read_parquet(result["silver_path"])
    assert rows[0]["merchant_name"] == "No Timestamp V1"  # not silently overwritten


# ---- record_hash determinism ----


def test_record_hash_is_deterministic_for_identical_content():
    record = merchant._transform_record(NAMED_NODE)
    h1 = common.compute_record_hash(record, merchant.BUSINESS_HASH_FIELDS)
    h2 = common.compute_record_hash(record, merchant.BUSINESS_HASH_FIELDS)
    assert h1 == h2


def test_record_hash_differs_for_different_content():
    r1 = merchant._transform_record(NAMED_NODE)
    r2 = merchant._transform_record(dict(NAMED_NODE, tags={"name": "Different Name", "shop": "bakery"}))
    h1 = common.compute_record_hash(r1, merchant.BUSINESS_HASH_FIELDS)
    h2 = common.compute_record_hash(r2, merchant.BUSINESS_HASH_FIELDS)
    assert h1 != h2


# ---- end-to-end smoke test (30) ----


def test_run_against_real_local_bronze_data_reconciles(tmp_path):
    """Controlled fixture data, not real data/bronze/ — deterministic regardless
    of the current state of real ingestion (see the API Automation Test Failures
    audit: this test previously read data/bronze/merchant_osm/'s real latest run,
    which is no longer guaranteed to be non-empty now that real incremental
    Airflow runs exist for this source)."""
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [NAMED_NODE, ADDRESS_FULL])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["total_silver_rows"] > 0
    assert result["bronze_record_count"] > 0


def test_run_raises_file_not_found_when_no_bronze_run_exists(tmp_path):
    import pytest

    bronze_root = tmp_path / "bronze"
    bronze_root.mkdir()
    with pytest.raises(FileNotFoundError):
        merchant.run(bronze_root=bronze_root, silver_root=tmp_path / "silver", quarantine_root=tmp_path / "quarantine")


# ---- S7 regression: within-batch duplicate merchant_id must not silently
# last-write-wins overwrite (a real defect found and fixed in S7 — see
# src/silver/merchant.py's comment at the `existing = final_by_id.get(key)` line) ----


def test_run_within_batch_duplicate_merchant_id_without_timestamps_is_quarantined_not_overwritten(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    version_a = {"type": "node", "id": 50, "lat": 1, "lon": 1, "tags": {"name": "Version A"}}
    version_b = {"type": "node", "id": 50, "lat": 1, "lon": 1, "tags": {"name": "Version B"}}
    _make_bronze_run(bronze_root, "run_1", [version_a, version_b])
    result = _run(bronze_root, silver_root, quarantine_root)

    # Before the fix, this silently produced inserted_count=2 and overwrote to
    # "Version B" via plain dict assignment, with reconciliation still passing
    # (2 elements = 2 "inserted") — the bug was invisible to the count check.
    assert result["inserted_count"] == 1
    assert result["quarantined_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert len(rows) == 1
    assert rows[0]["merchant_name"] == "Version A"  # first occurrence preserved, not silently overwritten
    rejected = quarantine.read_quarantine(result["quarantine_path"])
    assert any(r["failing_check_name"] == "upsert_conflict" for r in rejected)


def test_run_within_batch_duplicate_merchant_id_with_timestamps_prefers_newer_not_array_order(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    older_first = {
        "type": "node",
        "id": 51,
        "lat": 1,
        "lon": 1,
        "timestamp": "2026-01-01T00:00:00Z",
        "tags": {"name": "Older, First In Array"},
    }
    newer_second = {
        "type": "node",
        "id": 51,
        "lat": 1,
        "lon": 1,
        "timestamp": "2026-06-01T00:00:00Z",
        "tags": {"name": "Newer, Second In Array"},
    }
    _make_bronze_run(bronze_root, "run_1", [older_first, newer_second])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["inserted_count"] == 1
    assert result["updated_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert len(rows) == 1
    assert rows[0]["merchant_name"] == "Newer, Second In Array"
