"""S8 — dedicated idempotency / incremental / upsert verification.

A consolidated proof, in one file, that: reprocessing the same Bronze run never
duplicates or mutates published data; incremental scenarios (insert/update/
unchanged/stale/conflict) resolve correctly regardless of within-batch record
order; FX's append-only immutability holds; and every scenario still reconciles
exactly against the shared src/silver/common.reconcile_counts() formula.

Overlaps deliberately with scenario-level tests already in each dataset's own
tests/test_silver_<dataset>.py and in tests/test_silver_reconciliation.py — S8's
purpose (per the phase brief) is a dedicated, standalone verification pass, not
strictly-new coverage. No Silver transformation logic is changed here except
where a genuine defect was found (see the two "S8 defect" tests below).
"""

import json
from decimal import Decimal
from pathlib import Path

from src.silver import card_issuer, common, country, fx_rate, iso_currency, legal_entity, mcc, merchant, quarantine

# ==================================================================
# Fixture helpers (local per dataset, mirroring each dataset's own test file)
# ==================================================================

MCC_HEADER = "mcc,edited_description,combined_description,usda_description,irs_description,irs_reportable\n"
ISO_HEADER = "Entity,Currency,AlphabeticCode,NumericCode,MinorUnit,WithdrawalDate\n"
BIN_HEADER = "BIN,Brand,Type,Category,Issuer,IssuerPhone,IssuerUrl,isoCode2,isoCode3,CountryName\n"


def _mcc_run(bronze_root: Path, run_id: str, csv_body: str) -> Path:
    run_dir = bronze_root / "mcc" / run_id
    run_dir.mkdir(parents=True)
    (run_dir / "mcc_codes.csv").write_text(MCC_HEADER + csv_body, encoding="utf-8")
    (run_dir / "metadata.json").write_text(
        json.dumps({"raw_file": "mcc_codes.csv", "record_count": csv_body.count("\n"), "ingestion_timestamp_utc": "2026-09-21T00:00:00Z"}),
        encoding="utf-8",
    )
    return run_dir


def _country_run(bronze_root: Path, run_id: str, records: list) -> Path:
    run_dir = bronze_root / "country" / run_id
    run_dir.mkdir(parents=True)
    (run_dir / "countries.json").write_text(json.dumps(records), encoding="utf-8")
    (run_dir / "metadata.json").write_text(
        json.dumps({"raw_file": "countries.json", "record_count": len(records), "ingestion_timestamp_utc": "2026-09-20T08:38:48Z"}),
        encoding="utf-8",
    )
    return run_dir


def _iso_run(bronze_root: Path, run_id: str, csv_body: str, record_count: int) -> Path:
    run_dir = bronze_root / "iso_currency" / run_id
    run_dir.mkdir(parents=True)
    (run_dir / "iso_currency_codes.csv").write_text(ISO_HEADER + csv_body, encoding="utf-8")
    (run_dir / "metadata.json").write_text(
        json.dumps({"raw_file": "iso_currency_codes.csv", "record_count": record_count, "ingestion_timestamp_utc": "2026-09-21T00:00:00Z"}),
        encoding="utf-8",
    )
    return run_dir


def _card_issuer_run(bronze_root: Path, run_id: str, csv_body: str, record_count: int) -> Path:
    run_dir = bronze_root / "card_issuer" / run_id
    run_dir.mkdir(parents=True)
    (run_dir / "bin_list_data.csv").write_text(BIN_HEADER + csv_body, encoding="utf-8")
    (run_dir / "metadata.json").write_text(
        json.dumps({"raw_file": "bin_list_data.csv", "record_count": record_count, "ingestion_timestamp_utc": "2026-09-21T00:00:00Z"}),
        encoding="utf-8",
    )
    return run_dir


def _fx_run(bronze_root: Path, run_id: str, raw: dict) -> Path:
    run_dir = bronze_root / "currency" / run_id
    run_dir.mkdir(parents=True)
    (run_dir / "rates.json").write_text(json.dumps(raw), encoding="utf-8")
    (run_dir / "metadata.json").write_text(
        json.dumps({"raw_file": "rates.json", "record_count": len(fx_rate._flatten(raw)), "ingestion_timestamp_utc": "2026-09-18T08:31:49Z"}),
        encoding="utf-8",
    )
    return run_dir


def _merchant_run(bronze_root: Path, run_id: str, elements: list) -> Path:
    run_dir = bronze_root / "merchant_osm" / run_id
    run_dir.mkdir(parents=True)
    raw = {"version": 0.6, "osm3s": {"timestamp_osm_base": "2026-09-20T08:30:16Z"}, "elements": elements}
    (run_dir / "osm_places.json").write_text(json.dumps(raw), encoding="utf-8")
    (run_dir / "metadata.json").write_text(
        json.dumps({"raw_file": "osm_places.json", "record_count": len(elements), "ingestion_timestamp_utc": "2026-09-20T08:31:50Z"}),
        encoding="utf-8",
    )
    return run_dir


def _gleif_record(lei, name, jurisdiction="GB", last_update="2026-06-01T00:00:00Z", entity_status="ACTIVE", registration_status="ISSUED"):
    return {
        "type": "lei-records",
        "id": lei,
        "attributes": {
            "lei": lei,
            "entity": {
                "legalName": {"name": name},
                "legalAddress": {},
                "headquartersAddress": {},
                "jurisdiction": jurisdiction,
                "status": entity_status,
            },
            "registration": {"status": registration_status, "lastUpdateDate": last_update},
        },
    }


def _gleif_run(bronze_root: Path, run_id: str, records: list) -> Path:
    run_dir = bronze_root / "gleif" / run_id
    run_dir.mkdir(parents=True)
    page = {"meta": {"pagination": {"currentPage": 1, "lastPage": 1}}, "data": records}
    (run_dir / "page_0001.json").write_text(json.dumps(page), encoding="utf-8")
    (run_dir / "metadata.json").write_text(
        json.dumps(
            {
                "raw_file": "page_0001.json",
                "record_count": len(records),
                "ingestion_timestamp_utc": "2026-09-21T05:44:17Z",
                "page_files": ["page_0001.json"],
            }
        ),
        encoding="utf-8",
    )
    return run_dir


# ==================================================================
# STEP 2 — idempotency matrix: overwrite datasets (mcc, country, iso_currency, card_issuer)
# ==================================================================


def test_mcc_reprocessing_same_bronze_run_is_idempotent(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _mcc_run(bronze_root, "run_1", "0742,Veterinary Services,,,,Yes\n5812,Eating Places,,,,No\n")

    first = mcc.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    first_rows = common.read_parquet(first["silver_path"])
    second = mcc.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    second_rows = common.read_parquet(second["silver_path"])

    assert len(first_rows) == len(second_rows) == 2
    keys = [r["mcc_code"] for r in second_rows]
    assert len(keys) == len(set(keys))  # no duplicate primary keys
    for a, b in zip(sorted(first_rows, key=lambda r: r["mcc_code"]), sorted(second_rows, key=lambda r: r["mcc_code"])):
        for field in a:
            if field == "silver_processed_at_utc":
                continue  # overwrite datasets legitimately refresh this every run — docs/21 §10
            assert a[field] == b[field]


def test_country_reprocessing_same_bronze_run_is_idempotent(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    record = {"name": {"common": "United Kingdom", "official": "UK"}, "cca2": "GB", "cca3": "GBR", "currencies": {"GBP": {}}, "capital": ["London"]}
    _country_run(bronze_root, "run_1", [record])

    first = country.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    first_rows = common.read_parquet(first["silver_path"])
    second = country.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    second_rows = common.read_parquet(second["silver_path"])

    assert len(first_rows) == len(second_rows) == 1
    keys = [r["country_code_alpha3"] for r in second_rows]
    assert len(keys) == len(set(keys))
    for field in first_rows[0]:
        if field == "silver_processed_at_utc":
            continue
        assert first_rows[0][field] == second_rows[0][field]


def test_card_issuer_reprocessing_same_bronze_run_is_idempotent(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    csv_body = '002102,"PRIVATE LABEL",CREDIT,STANDARD,"CHINA MERCHANTS BANK",95555,https://x,CN,CHN,CHINA\n400000,VISA,DEBIT,CLASSIC,,,,US,USA,UNITED STATES\n'
    _card_issuer_run(bronze_root, "run_1", csv_body, record_count=2)

    first = card_issuer.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    first_rows = common.read_parquet(first["silver_path"])
    second = card_issuer.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    second_rows = common.read_parquet(second["silver_path"])

    assert len(first_rows) == len(second_rows) == 2
    keys = [r["bin_range"] for r in second_rows]
    assert len(keys) == len(set(keys))
    for a, b in zip(sorted(first_rows, key=lambda r: r["bin_range"]), sorted(second_rows, key=lambda r: r["bin_range"])):
        for field in a:
            if field == "silver_processed_at_utc":
                continue
            assert a[field] == b[field]


def test_iso_currency_reprocessing_same_bronze_run_is_idempotent_and_merge_stable(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    csv_body = "UNITED STATES,US Dollar,USD,840,2,\n" "ECUADOR,US Dollar,USD,840,2,\n" "AFGHANISTAN,Afghani,AFN,971,2,\n"
    _iso_run(bronze_root, "run_1", csv_body, record_count=3)

    first = iso_currency.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    first_rows = common.read_parquet(first["silver_path"])
    second = iso_currency.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    second_rows = common.read_parquet(second["silver_path"])

    assert first["merged_duplicate_count"] == second["merged_duplicate_count"] == 1
    assert len(first_rows) == len(second_rows) == 2  # USD (merged from 2) + AFN
    keys = [r["currency_code"] for r in second_rows]
    assert len(keys) == len(set(keys))
    for a, b in zip(sorted(first_rows, key=lambda r: r["currency_code"]), sorted(second_rows, key=lambda r: r["currency_code"])):
        for field in a:
            if field == "silver_processed_at_utc":
                continue
            assert a[field] == b[field]


# ==================================================================
# STEP 2/3/4/8 — FX RATE: idempotency, incremental append, immutability, late arrival
# ==================================================================


def test_fx_second_identical_run_classifies_all_as_unchanged_and_writes_no_new_file(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _fx_run(bronze_root, "run_1", {"base": "EUR", "date": "2026-09-18", "rates": {"USD": 1.1, "GBP": 0.86}})

    first = fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    part_path = first["written_paths"][0]
    before_mtime = part_path.stat().st_mtime_ns
    before_content = common.read_parquet(part_path)

    second = fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert second["appended_count"] == 0
    assert second["unchanged_count"] == 2
    assert second["written_paths"] == []  # no new part file created
    assert part_path.stat().st_mtime_ns == before_mtime  # existing file untouched
    assert common.read_parquet(part_path) == before_content


def test_fx_new_key_appends(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _fx_run(bronze_root, "run_1", {"base": "EUR", "date": "2026-09-18", "rates": {"USD": 1.1}})
    fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    _fx_run(bronze_root, "run_2", {"base": "EUR", "date": "2026-09-18", "rates": {"GBP": 0.86}})
    result = fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["appended_count"] == 1
    assert result["conflict_count"] == 0


def test_fx_same_key_same_rate_is_unchanged(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _fx_run(bronze_root, "run_1", {"base": "EUR", "date": "2026-09-18", "rates": {"USD": 1.1}})
    fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    _fx_run(bronze_root, "run_2", {"base": "EUR", "date": "2026-09-18", "rates": {"USD": 1.1}})
    result = fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["unchanged_count"] == 1
    assert result["appended_count"] == 0


def test_fx_same_key_different_rate_is_quarantined_as_immutability_conflict(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _fx_run(bronze_root, "run_1", {"base": "EUR", "date": "2026-09-18", "rates": {"USD": 1.1}})
    first = fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    published_before = common.read_parquet(first["written_paths"][0])

    _fx_run(bronze_root, "run_2", {"base": "EUR", "date": "2026-09-18", "rates": {"USD": 1.25}})
    result = fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["conflict_count"] == 1
    assert result["appended_count"] == 0
    rejected = quarantine.read_quarantine(result["quarantine_path"])
    assert rejected[0]["failing_check_name"] == "immutability_conflict"
    # the already-published historical value was never overwritten
    assert common.read_parquet(first["written_paths"][0]) == published_before


def test_fx_existing_historical_partition_untouched_when_new_partition_is_created(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _fx_run(bronze_root, "run_1", {"base": "EUR", "date": "2026-08-15", "rates": {"USD": 1.05}})
    first = fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    august_part = first["written_paths"][0]
    before_mtime = august_part.stat().st_mtime_ns

    _fx_run(bronze_root, "run_2", {"base": "EUR", "date": "2026-09-18", "rates": {"USD": 1.1}})
    result = fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["appended_count"] == 1
    new_part = result["written_paths"][0]
    assert new_part != august_part
    assert august_part.stat().st_mtime_ns == before_mtime  # historical partition file never touched


def test_fx_late_arrival_for_new_historical_key_appends_to_its_own_past_partition(tmp_path):
    """A Bronze run processed AFTER a more recent one can still legitimately
    contain an earlier calendar rate_date that simply hadn't been published yet
    (e.g. a backfill) — it must append normally to its own (older) partition."""
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _fx_run(bronze_root, "run_1", {"base": "EUR", "date": "2026-09-18", "rates": {"USD": 1.1}})
    fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    _fx_run(bronze_root, "run_2", {"base": "EUR", "date": "2026-06-01", "rates": {"USD": 1.02}})  # earlier calendar date
    result = fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["appended_count"] == 1
    assert (silver_root / "fx_rate" / "year=2026" / "month=06").is_dir()
    assert (silver_root / "fx_rate" / "year=2026" / "month=09").is_dir()


def test_fx_composite_key_uniqueness_holds_across_multiple_runs(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _fx_run(bronze_root, "run_1", {"base": "EUR", "date": "2026-09-18", "rates": {"USD": 1.1}})
    fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    _fx_run(bronze_root, "run_2", {"base": "EUR", "date": "2026-09-18", "rates": {"USD": 1.1}})  # reprocess-equivalent
    fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    _fx_run(bronze_root, "run_3", {"base": "EUR", "date": "2026-09-19", "rates": {"USD": 1.12}})
    fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    partition_dir = silver_root / "fx_rate" / "year=2026" / "month=09"
    all_keys = []
    for part_file in partition_dir.glob("part-*.parquet"):
        for r in common.read_parquet(part_file):
            all_keys.append((r["rate_date"], r["base_currency"], r["quote_currency"]))
    assert len(all_keys) == len(set(all_keys))


# ==================================================================
# STEP 2/3/4 — MERCHANT: full insert/unchanged/update/stale/conflict matrix,
# same-batch ordering, duplicates, reconciliation
# ==================================================================


def _node(id_, name, ts=None, extra_tags=None):
    tags = {"name": name}
    if extra_tags:
        tags.update(extra_tags)
    el = {"type": "node", "id": id_, "lat": 1.0, "lon": 1.0, "tags": tags}
    if ts is not None:
        el["timestamp"] = ts
    return el


def test_merchant_reprocessing_same_run_is_idempotent_record_hash_stable(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _merchant_run(bronze_root, "run_1", [_node(1, "Shop A")])

    first = merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    first_rows = common.read_parquet(first["silver_path"])
    second = merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    second_rows = common.read_parquet(second["silver_path"])

    assert second["inserted_count"] == 0
    assert second["unchanged_count"] == 1
    assert first_rows == second_rows  # including record_hash and lineage — truly untouched
    assert first_rows[0]["record_hash"] == second_rows[0]["record_hash"]


def test_merchant_new_merchant_inserts(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _merchant_run(bronze_root, "run_1", [_node(1, "Shop A")])
    merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    _merchant_run(bronze_root, "run_2", [_node(2, "Shop B")])
    result = merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    assert result["inserted_count"] == 1


def test_merchant_same_content_is_unchanged(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _merchant_run(bronze_root, "run_1", [_node(1, "Shop A", ts="2026-01-01T00:00:00Z")])
    merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    _merchant_run(bronze_root, "run_2", [_node(1, "Shop A", ts="2026-01-01T00:00:00Z")])
    result = merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    assert result["unchanged_count"] == 1
    assert result["updated_count"] == 0


def test_merchant_newer_timestamp_updates(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _merchant_run(bronze_root, "run_1", [_node(1, "Shop A", ts="2026-01-01T00:00:00Z")])
    merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    _merchant_run(bronze_root, "run_2", [_node(1, "Shop A Renamed", ts="2026-06-01T00:00:00Z")])
    result = merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    assert result["updated_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert rows[0]["merchant_name"] == "Shop A Renamed"


def test_merchant_older_timestamp_is_stale_skipped_late_arrival(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _merchant_run(bronze_root, "run_1", [_node(1, "Current Shop", ts="2026-06-01T00:00:00Z")])
    merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    _merchant_run(bronze_root, "run_2", [_node(1, "Late Arriving Stale Data", ts="2026-01-01T00:00:00Z")])
    result = merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    assert result["stale_skipped_count"] == 1
    assert result["updated_count"] == 0
    rows = common.read_parquet(result["silver_path"])
    assert rows[0]["merchant_name"] == "Current Shop"  # late/older data never overwrites newer state


def test_merchant_equal_timestamp_different_content_is_quarantined(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _merchant_run(bronze_root, "run_1", [_node(1, "Shop A", ts="2026-06-01T00:00:00Z")])
    merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    _merchant_run(bronze_root, "run_2", [_node(1, "Shop A Conflicting", ts="2026-06-01T00:00:00Z")])
    result = merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    assert result["quarantined_count"] == 1
    rejected = quarantine.read_quarantine(result["quarantine_path"])
    assert rejected[0]["failing_check_name"] == "upsert_conflict"


def test_merchant_missing_timestamp_different_content_is_quarantined_not_latest_wins(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _merchant_run(bronze_root, "run_1", [_node(1, "Shop A")])  # no timestamp (out body)
    merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    _merchant_run(bronze_root, "run_2", [_node(1, "Shop A Different")])  # still no timestamp
    result = merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    assert result["quarantined_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert rows[0]["merchant_name"] == "Shop A"  # not silently overwritten


def test_merchant_newer_then_older_in_same_batch_newer_wins(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    newer_first = _node(1, "Newer First", ts="2026-06-01T00:00:00Z")
    older_second = _node(1, "Older Second", ts="2026-01-01T00:00:00Z")
    _merchant_run(bronze_root, "run_1", [newer_first, older_second])
    result = merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["inserted_count"] == 1
    assert result["stale_skipped_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert len(rows) == 1
    assert rows[0]["merchant_name"] == "Newer First"


def test_merchant_older_then_newer_in_same_batch_newer_wins(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    older_first = _node(1, "Older First", ts="2026-01-01T00:00:00Z")
    newer_second = _node(1, "Newer Second", ts="2026-06-01T00:00:00Z")
    _merchant_run(bronze_root, "run_1", [older_first, newer_second])
    result = merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["inserted_count"] == 1
    assert result["updated_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert len(rows) == 1
    assert rows[0]["merchant_name"] == "Newer Second"


def test_merchant_duplicate_key_in_one_batch_final_state_is_exactly_one_row(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    elements = [_node(1, "First"), _node(1, "Second"), _node(2, "Distinct Shop")]
    _merchant_run(bronze_root, "run_1", elements)
    result = merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    rows = common.read_parquet(result["silver_path"])
    merchant_ids = [r["merchant_id"] for r in rows]
    assert len(merchant_ids) == len(set(merchant_ids))
    assert "node:1" in merchant_ids and "node:2" in merchant_ids

    total = (
        result["excluded_vacant_shop"]
        + result["excluded_unnamed"]
        + result["quarantined_count"]
        + result["inserted_count"]
        + result["updated_count"]
        + result["unchanged_count"]
        + result["stale_skipped_count"]
    )
    assert total == result["bronze_record_count"] == 3  # every input record accounted for


# ==================================================================
# STEP 2/3/4 — GLEIF: equivalent full matrix
# ==================================================================

LEI_A = "549300ABCDEFGHIJKL01"
LEI_B = "549300ABCDEFGHIJKL02"


def test_gleif_reprocessing_same_run_is_idempotent_record_hash_stable(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _gleif_run(bronze_root, "run_1", [_gleif_record(LEI_A, "Company A")])

    first = legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    first_rows = common.read_parquet(first["silver_path"])
    second = legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    second_rows = common.read_parquet(second["silver_path"])

    assert second["inserted_count"] == 0
    assert second["unchanged_count"] == 1
    assert first_rows == second_rows
    assert first_rows[0]["record_hash"] == second_rows[0]["record_hash"]


def test_gleif_new_lei_inserts(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _gleif_run(bronze_root, "run_1", [_gleif_record(LEI_A, "Company A")])
    legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    _gleif_run(bronze_root, "run_2", [_gleif_record(LEI_B, "Company B")])
    result = legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    assert result["inserted_count"] == 1


def test_gleif_same_content_is_unchanged(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _gleif_run(bronze_root, "run_1", [_gleif_record(LEI_A, "Company A", last_update="2026-01-01T00:00:00Z")])
    legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    _gleif_run(bronze_root, "run_2", [_gleif_record(LEI_A, "Company A", last_update="2026-01-01T00:00:00Z")])
    result = legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    assert result["unchanged_count"] == 1
    assert result["updated_count"] == 0


def test_gleif_newer_registration_last_update_date_updates(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _gleif_run(bronze_root, "run_1", [_gleif_record(LEI_A, "Old Name", last_update="2026-01-01T00:00:00Z")])
    legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    _gleif_run(bronze_root, "run_2", [_gleif_record(LEI_A, "New Name", last_update="2026-06-01T00:00:00Z")])
    result = legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    assert result["updated_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert rows[0]["legal_name"] == "New Name"


def test_gleif_older_timestamp_is_stale_skipped_late_arrival(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _gleif_run(bronze_root, "run_1", [_gleif_record(LEI_A, "Current Name", last_update="2026-06-01T00:00:00Z")])
    legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    _gleif_run(bronze_root, "run_2", [_gleif_record(LEI_A, "Late Arriving Stale Name", last_update="2026-01-01T00:00:00Z")])
    result = legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    assert result["stale_skipped_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert rows[0]["legal_name"] == "Current Name"


def test_gleif_equal_timestamp_different_content_is_quarantined(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _gleif_run(bronze_root, "run_1", [_gleif_record(LEI_A, "Name A", last_update="2026-06-01T00:00:00Z")])
    legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    _gleif_run(bronze_root, "run_2", [_gleif_record(LEI_A, "Name B Conflicting", last_update="2026-06-01T00:00:00Z")])
    result = legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    assert result["quarantined_count"] == 1
    rejected = quarantine.read_quarantine(result["quarantine_path"])
    assert rejected[0]["failing_check_name"] == "upsert_conflict"


def test_gleif_missing_timestamp_different_content_is_quarantined_not_latest_wins(tmp_path):
    """registration_last_update_date is HARD_FAIL-required, so this state can only
    arise from a pre-existing row lacking it (e.g. a future contract change) —
    exercised by seeding the existing Silver snapshot directly, matching the
    equivalent S6 test."""
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    silver_root.mkdir(parents=True)
    existing_row = {
        "lei": LEI_A,
        "legal_name": "No Timestamp Existing",
        "legal_address_country": None,
        "legal_address_city": None,
        "legal_address_region": None,
        "legal_address_postal_code": None,
        "legal_address_lines": None,
        "hq_address_country": None,
        "hq_address_city": None,
        "hq_address_region": None,
        "hq_address_postal_code": None,
        "hq_address_lines": None,
        "legal_form_id": None,
        "entity_category": None,
        "entity_status": "ACTIVE",
        "entity_jurisdiction": "GB",
        "entity_creation_date": None,
        "registration_initial_registration_date": None,
        "registration_last_update_date": None,
        "registration_status": "ISSUED",
        "registration_next_renewal_date": None,
        "bic": None,
        "source_updated_timestamp": None,
        "record_hash": "deadbeef",
        "source_name": "gleif",
        "bronze_run_id": "run_0",
        "ingestion_timestamp_utc": "2026-01-01T00:00:00Z",
        "silver_processed_at_utc": "2026-01-01T00:00:00Z",
        "silver_transform_version": "v1",
    }
    common.write_parquet([existing_row], silver_root / "legal_entity" / "data.parquet", schema=legal_entity.LEGAL_ENTITY_SCHEMA)

    _gleif_run(bronze_root, "run_1", [_gleif_record(LEI_A, "Incoming Name", last_update="2026-06-01T00:00:00Z")])
    result = legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["quarantined_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert rows[0]["legal_name"] == "No Timestamp Existing"


def test_gleif_newer_then_older_in_same_batch_array_order(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    newer_first = _gleif_record(LEI_A, "Newer First", last_update="2026-06-01T00:00:00Z")
    older_second = _gleif_record(LEI_A, "Older Second", last_update="2026-01-01T00:00:00Z")
    _gleif_run(bronze_root, "run_1", [newer_first, older_second])
    result = legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["inserted_count"] == 1
    assert result["stale_skipped_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert len(rows) == 1
    assert rows[0]["legal_name"] == "Newer First"


def test_gleif_older_then_newer_in_same_batch_array_order(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    older_first = _gleif_record(LEI_A, "Older First", last_update="2026-01-01T00:00:00Z")
    newer_second = _gleif_record(LEI_A, "Newer Second", last_update="2026-06-01T00:00:00Z")
    _gleif_run(bronze_root, "run_1", [older_first, newer_second])
    result = legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["inserted_count"] == 1
    assert result["updated_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert len(rows) == 1
    assert rows[0]["legal_name"] == "Newer Second"


def test_gleif_duplicate_lei_in_one_batch_final_state_is_exactly_one_row_and_reconciles(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    records = [
        _gleif_record(LEI_A, "First"),
        _gleif_record(LEI_A, "Second"),
        _gleif_record(LEI_B, "Distinct Co"),
    ]
    _gleif_run(bronze_root, "run_1", records)
    result = legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    rows = common.read_parquet(result["silver_path"])
    leis = [r["lei"] for r in rows]
    assert len(leis) == len(set(leis))
    assert LEI_A in leis and LEI_B in leis

    total = result["quarantined_count"] + result["inserted_count"] + result["updated_count"] + result["unchanged_count"] + result["stale_skipped_count"]
    assert total == result["bronze_record_count"] == 3


# ==================================================================
# STEP 5 — lineage verification
# ==================================================================


def test_lineage_bronze_run_id_matches_actual_bronze_run_used(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _mcc_run(bronze_root, "run_20260101T000000Z", "0742,Veterinary Services,,,,Yes\n")
    result = mcc.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    rows = common.read_parquet(result["silver_path"])
    assert rows[0]["bronze_run_id"] == "run_20260101T000000Z" == result["bronze_run_id"]
    assert rows[0]["source_name"] == "mcc"
    assert rows[0]["ingestion_timestamp_utc"] == "2026-09-21T00:00:00Z"


def test_lineage_record_hash_deterministic_and_independent_of_processing_timestamp():
    r1 = merchant._transform_record(_node(1, "Shop A"))
    r2 = merchant._transform_record(_node(1, "Shop A"))
    h1 = common.compute_record_hash(r1, merchant.BUSINESS_HASH_FIELDS)
    h2 = common.compute_record_hash(r2, merchant.BUSINESS_HASH_FIELDS)
    assert h1 == h2  # deterministic

    r3 = legal_entity._transform_record(_gleif_record(LEI_A, "Company A", last_update="2026-01-01T00:00:00Z"))
    r4 = legal_entity._transform_record(_gleif_record(LEI_A, "Company A", last_update="2026-06-01T00:00:00Z"))
    h3 = common.compute_record_hash(r3, legal_entity.BUSINESS_HASH_FIELDS)
    h4 = common.compute_record_hash(r4, legal_entity.BUSINESS_HASH_FIELDS)
    assert h3 == h4  # source timestamp change alone does not change the content hash


def test_lineage_unchanged_upsert_rows_are_not_rewritten_merchant_and_gleif(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"

    _merchant_run(bronze_root, "run_1", [_node(1, "Shop A")])
    m1 = merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    m1_processed_at = common.read_parquet(m1["silver_path"])[0]["silver_processed_at_utc"]
    _merchant_run(bronze_root, "run_2", [_node(1, "Shop A")])
    m2 = merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    m2_processed_at = common.read_parquet(m2["silver_path"])[0]["silver_processed_at_utc"]
    assert m1_processed_at == m2_processed_at  # not re-stamped for an unchanged upsert row

    _gleif_run(bronze_root, "run_1", [_gleif_record(LEI_A, "Company A")])
    g1 = legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    g1_processed_at = common.read_parquet(g1["silver_path"])[0]["silver_processed_at_utc"]
    _gleif_run(bronze_root, "run_2", [_gleif_record(LEI_A, "Company A")])
    g2 = legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    g2_processed_at = common.read_parquet(g2["silver_path"])[0]["silver_processed_at_utc"]
    assert g1_processed_at == g2_processed_at


def test_lineage_overwrite_datasets_legitimately_refresh_processed_at_every_run(tmp_path):
    """Contrast case, documented in docs/21 §10: overwrite datasets (no upsert
    state) achieve idempotency "trivially (full replace)" — every run legitimately
    regenerates silver_processed_at_utc even when business content is identical.
    Not a defect; the opposite behavior (freezing it) would be the actual bug for
    these four datasets, since they have no prior-state comparison to freeze against."""
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _mcc_run(bronze_root, "run_1", "0742,Veterinary Services,,,,Yes\n")
    r1 = mcc.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    r2 = mcc.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    rows1 = common.read_parquet(r1["silver_path"])
    rows2 = common.read_parquet(r2["silver_path"])
    # business content identical...
    assert rows1[0]["description"] == rows2[0]["description"]
    # ...silver_processed_at_utc is allowed to differ (both are valid ISO timestamps)
    assert rows1[0]["silver_processed_at_utc"]
    assert rows2[0]["silver_processed_at_utc"]


# ==================================================================
# STEP 6 — reconciliation verification for incremental scenarios (no new formula)
# ==================================================================


def test_reconciliation_fx_formula_holds_across_append_unchanged_conflict(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _fx_run(bronze_root, "run_1", {"base": "EUR", "date": "2026-09-18", "rates": {"USD": 1.1, "GBP": 0.86}})
    fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    _fx_run(bronze_root, "run_2", {"base": "EUR", "date": "2026-09-18", "rates": {"USD": 1.1, "GBP": 0.90, "CHF": 0.95}})
    result = fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    # USD unchanged, GBP conflict (quarantined), CHF appended
    assert result["appended_count"] == 1
    assert result["unchanged_count"] == 1
    assert result["conflict_count"] == 1
    assert result["appended_count"] + result["unchanged_count"] + result["quarantined_count"] == result["bronze_record_count"] == 3


def test_reconciliation_merchant_formula_holds_across_mixed_scenario(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _merchant_run(
        bronze_root,
        "run_1",
        [
            _node(1, "Unchanged Shop", ts="2026-01-01T00:00:00Z"),
            _node(2, "To Be Updated", ts="2026-01-01T00:00:00Z"),
            {"type": "node", "id": 3, "lat": 1, "lon": 1, "tags": {"shop": "vacant"}},
        ],
    )
    merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    _merchant_run(
        bronze_root,
        "run_2",
        [
            _node(1, "Unchanged Shop", ts="2026-01-01T00:00:00Z"),  # unchanged
            _node(2, "Updated Name", ts="2026-06-01T00:00:00Z"),  # updated
            {"type": "node", "id": 4, "lat": 1, "lon": 1, "tags": {"shop": "newsagent"}},  # unnamed -> excluded
            _node(5, "New Merchant"),  # inserted
        ],
    )
    result = merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["unchanged_count"] == 1
    assert result["updated_count"] == 1
    assert result["excluded_unnamed"] == 1
    assert result["inserted_count"] == 1
    total = (
        result["excluded_vacant_shop"]
        + result["excluded_unnamed"]
        + result["quarantined_count"]
        + result["inserted_count"]
        + result["updated_count"]
        + result["unchanged_count"]
        + result["stale_skipped_count"]
    )
    assert total == result["bronze_record_count"] == 4


def test_reconciliation_gleif_formula_holds_across_mixed_scenario(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    lei_stale = "549300ABCDEFGHIJKL03"
    _gleif_run(
        bronze_root,
        "run_1",
        [
            _gleif_record(LEI_A, "Unchanged Co", last_update="2026-01-01T00:00:00Z"),
            _gleif_record(LEI_B, "To Be Updated", last_update="2026-01-01T00:00:00Z"),
            _gleif_record(lei_stale, "Current Name", last_update="2026-06-01T00:00:00Z"),
        ],
    )
    legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    _gleif_run(
        bronze_root,
        "run_2",
        [
            _gleif_record(LEI_A, "Unchanged Co", last_update="2026-01-01T00:00:00Z"),
            _gleif_record(LEI_B, "New Name", last_update="2026-06-01T00:00:00Z"),
            _gleif_record(lei_stale, "Stale Attempt", last_update="2026-01-01T00:00:00Z"),
            _gleif_record("549300ABCDEFGHIJKL04", "Brand New", last_update="2026-06-01T00:00:00Z"),
        ],
    )
    result = legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["unchanged_count"] == 1
    assert result["updated_count"] == 1
    assert result["stale_skipped_count"] == 1
    assert result["inserted_count"] == 1
    total = result["quarantined_count"] + result["inserted_count"] + result["updated_count"] + result["unchanged_count"] + result["stale_skipped_count"]
    assert total == result["bronze_record_count"] == 4


def test_reconciliation_iso_currency_formula_holds_with_merge_and_quarantine(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    csv_body = (
        "UNITED STATES,US Dollar,USD,840,2,\n"
        "ECUADOR,US Dollar,USD,840,2,\n"
        "NOWHERE,,ZZZ,000,,\n"  # blank currency_name across whole group -> quarantined
    )
    _iso_run(bronze_root, "run_1", csv_body, record_count=3)
    result = iso_currency.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["valid_count"] == 1
    assert result["merged_duplicate_count"] == 1
    assert result["quarantined_count"] == 1
    assert result["valid_count"] + result["merged_duplicate_count"] + result["quarantined_count"] == result["bronze_record_count"] == 3
