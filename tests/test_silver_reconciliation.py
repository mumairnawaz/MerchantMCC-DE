"""S7 — reconciliation hardening.

Covers the new generic src/silver/common.reconcile_counts() helper in isolation,
plus a "reconciliation mismatch raises RuntimeError" regression for each of the
seven Silver datasets (previously only silver_legal_entity had this — see its own
test file), and the cross-cutting S7-mandated checks: the GLEIF jurisdiction
survival proof, merchant's vacant+unnamed overlap non-double-counting, ISO
currency's merged-duplicate accounting, and FX's immutability-conflict accounting.

Per-field/per-dataset transform and validation coverage already lives in each
dataset's own tests/test_silver_<dataset>.py — not duplicated here.
"""

import json
from pathlib import Path

import pytest

from src.silver import card_issuer, common, country, fx_rate, iso_currency, legal_entity, mcc, merchant, quarantine


# ==================================================================
# common.reconcile_counts() — the generic helper, dataset-agnostic
# ==================================================================


def test_reconcile_counts_exact_match_returns_result():
    result = common.reconcile_counts(10, {"accepted": 7, "quarantined": 3})
    assert result["passed"] is True
    assert result["bronze_count"] == 10
    assert result["accounted_for"] == 10
    assert result["dispositions"] == {"accepted": 7, "quarantined": 3}


def test_reconcile_counts_under_count_raises():
    with pytest.raises(RuntimeError):
        common.reconcile_counts(10, {"accepted": 7, "quarantined": 2})  # 9, missing 1


def test_reconcile_counts_over_count_raises():
    with pytest.raises(RuntimeError):
        common.reconcile_counts(10, {"accepted": 7, "quarantined": 4})  # 11, one too many


def test_reconcile_counts_zero_records_exact_match():
    result = common.reconcile_counts(0, {"accepted": 0, "quarantined": 0})
    assert result["passed"] is True
    assert result["accounted_for"] == 0


def test_reconcile_counts_zero_records_mismatch_raises():
    with pytest.raises(RuntimeError):
        common.reconcile_counts(0, {"accepted": 1})


def test_reconcile_counts_multiple_disposition_categories():
    result = common.reconcile_counts(
        100,
        {"accepted": 60, "quarantined": 10, "excluded_a": 15, "excluded_b": 5, "merged": 10},
    )
    assert result["passed"] is True
    assert result["accounted_for"] == 100


def test_reconcile_counts_single_wrong_category_among_many_raises():
    with pytest.raises(RuntimeError):
        common.reconcile_counts(100, {"accepted": 60, "quarantined": 10, "excluded_a": 15, "excluded_b": 4})  # 89


def test_reconcile_counts_error_message_includes_dataset_label():
    with pytest.raises(RuntimeError, match="mcc"):
        common.reconcile_counts(5, {"accepted": 1}, label="mcc")


def test_reconcile_counts_error_message_shows_dispositions():
    with pytest.raises(RuntimeError, match="accepted"):
        common.reconcile_counts(5, {"accepted": 1, "quarantined": 1})


# ==================================================================
# Per-dataset "reconciliation mismatch -> RuntimeError" regression
# (all use metadata.json with a deliberately wrong record_count)
# ==================================================================


def test_mcc_reconciliation_mismatch_raises(tmp_path):
    bronze_root = tmp_path / "bronze"
    run_dir = bronze_root / "mcc" / "run_1"
    run_dir.mkdir(parents=True)
    (run_dir / "mcc_codes.csv").write_text(
        "mcc,edited_description,combined_description,usda_description,irs_description,irs_reportable\n"
        "0742,Veterinary Services,,,,Yes\n",
        encoding="utf-8",
    )
    (run_dir / "metadata.json").write_text(
        json.dumps({"raw_file": "mcc_codes.csv", "record_count": 99, "ingestion_timestamp_utc": "2026-09-21T00:00:00Z"}),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError):
        mcc.run(bronze_root=bronze_root, silver_root=tmp_path / "silver", quarantine_root=tmp_path / "quarantine")


def test_country_reconciliation_mismatch_raises(tmp_path):
    bronze_root = tmp_path / "bronze"
    run_dir = bronze_root / "country" / "run_1"
    run_dir.mkdir(parents=True)
    record = {
        "name": {"common": "United Kingdom", "official": "United Kingdom"},
        "cca2": "GB",
        "cca3": "GBR",
        "currencies": {"GBP": {}},
        "capital": ["London"],
    }
    (run_dir / "countries.json").write_text(json.dumps([record]), encoding="utf-8")
    (run_dir / "metadata.json").write_text(
        json.dumps({"raw_file": "countries.json", "record_count": 99, "ingestion_timestamp_utc": "2026-09-20T08:38:48Z"}),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError):
        country.run(bronze_root=bronze_root, silver_root=tmp_path / "silver", quarantine_root=tmp_path / "quarantine")


def test_iso_currency_reconciliation_mismatch_raises(tmp_path):
    bronze_root = tmp_path / "bronze"
    run_dir = bronze_root / "iso_currency" / "run_1"
    run_dir.mkdir(parents=True)
    (run_dir / "iso_currency_codes.csv").write_text(
        "Entity,Currency,AlphabeticCode,NumericCode,MinorUnit,WithdrawalDate\n" "UNITED STATES,US Dollar,USD,840,2,\n",
        encoding="utf-8",
    )
    (run_dir / "metadata.json").write_text(
        json.dumps({"raw_file": "iso_currency_codes.csv", "record_count": 99, "ingestion_timestamp_utc": "2026-09-21T00:00:00Z"}),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError):
        iso_currency.run(bronze_root=bronze_root, silver_root=tmp_path / "silver", quarantine_root=tmp_path / "quarantine")


def test_card_issuer_reconciliation_mismatch_raises(tmp_path):
    bronze_root = tmp_path / "bronze"
    run_dir = bronze_root / "card_issuer" / "run_1"
    run_dir.mkdir(parents=True)
    (run_dir / "bin_list_data.csv").write_text(
        "BIN,Brand,Type,Category,Issuer,IssuerPhone,IssuerUrl,isoCode2,isoCode3,CountryName\n"
        "400000,VISA,DEBIT,CLASSIC,,,,US,USA,UNITED STATES\n",
        encoding="utf-8",
    )
    (run_dir / "metadata.json").write_text(
        json.dumps({"raw_file": "bin_list_data.csv", "record_count": 99, "ingestion_timestamp_utc": "2026-09-21T00:00:00Z"}),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError):
        card_issuer.run(bronze_root=bronze_root, silver_root=tmp_path / "silver", quarantine_root=tmp_path / "quarantine")


def test_fx_rate_reconciliation_mismatch_raises(tmp_path):
    bronze_root = tmp_path / "bronze"
    run_dir = bronze_root / "currency" / "run_1"
    run_dir.mkdir(parents=True)
    raw = {"base": "EUR", "date": "2026-09-18", "rates": {"USD": 1.1}}
    (run_dir / "rates.json").write_text(json.dumps(raw), encoding="utf-8")
    (run_dir / "metadata.json").write_text(
        json.dumps({"raw_file": "rates.json", "record_count": 99, "ingestion_timestamp_utc": "2026-09-18T08:31:49Z"}),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError):
        fx_rate.run(bronze_root=bronze_root, silver_root=tmp_path / "silver", quarantine_root=tmp_path / "quarantine")


def test_merchant_reconciliation_mismatch_raises(tmp_path):
    bronze_root = tmp_path / "bronze"
    run_dir = bronze_root / "merchant_osm" / "run_1"
    run_dir.mkdir(parents=True)
    el = {"type": "node", "id": 1, "lat": 1, "lon": 1, "tags": {"name": "Test Shop"}}
    raw = {"version": 0.6, "osm3s": {"timestamp_osm_base": "2026-09-20T08:30:16Z"}, "elements": [el]}
    (run_dir / "osm_places.json").write_text(json.dumps(raw), encoding="utf-8")
    (run_dir / "metadata.json").write_text(
        json.dumps({"raw_file": "osm_places.json", "record_count": 99, "ingestion_timestamp_utc": "2026-09-20T08:31:50Z"}),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError):
        merchant.run(bronze_root=bronze_root, silver_root=tmp_path / "silver", quarantine_root=tmp_path / "quarantine")


def test_legal_entity_reconciliation_mismatch_raises(tmp_path):
    bronze_root = tmp_path / "bronze"
    run_dir = bronze_root / "gleif" / "run_1"
    run_dir.mkdir(parents=True)
    record = {
        "type": "lei-records",
        "id": "549300ABCDEFGHIJKL12",
        "attributes": {
            "lei": "549300ABCDEFGHIJKL12",
            "entity": {
                "legalName": {"name": "Test Co"},
                "legalAddress": {},
                "headquartersAddress": {},
                "jurisdiction": "GB",
                "status": "ACTIVE",
            },
            "registration": {"status": "ISSUED", "lastUpdateDate": "2026-06-01T00:00:00Z"},
        },
    }
    page = {"meta": {"pagination": {"currentPage": 1, "lastPage": 1}}, "data": [record]}
    (run_dir / "page_0001.json").write_text(json.dumps(page), encoding="utf-8")
    (run_dir / "metadata.json").write_text(
        json.dumps(
            {
                "raw_file": "page_0001.json",
                "record_count": 99,
                "ingestion_timestamp_utc": "2026-09-21T05:44:17Z",
                "page_files": ["page_0001.json"],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError):
        legal_entity.run(bronze_root=bronze_root, silver_root=tmp_path / "silver", quarantine_root=tmp_path / "quarantine")


# ==================================================================
# ISO currency: merged-duplicate accounting (real 449 -> 307 semantics)
# ==================================================================


def test_iso_currency_merged_duplicates_are_accounted_for(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    run_dir = bronze_root / "iso_currency" / "run_1"
    run_dir.mkdir(parents=True)
    csv_body = (
        "Entity,Currency,AlphabeticCode,NumericCode,MinorUnit,WithdrawalDate\n"
        "UNITED STATES,US Dollar,USD,840,2,\n"
        "ECUADOR,US Dollar,USD,840,2,\n"
        "EL SALVADOR,US Dollar,USD,840,2,\n"
    )
    (run_dir / "iso_currency_codes.csv").write_text(csv_body, encoding="utf-8")
    (run_dir / "metadata.json").write_text(
        json.dumps({"raw_file": "iso_currency_codes.csv", "record_count": 3, "ingestion_timestamp_utc": "2026-09-21T00:00:00Z"}),
        encoding="utf-8",
    )
    result = iso_currency.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["valid_count"] == 1  # 3 rows collapse to 1 distinct currency_code
    assert result["merged_duplicate_count"] == 2
    total = result["valid_count"] + result["merged_duplicate_count"] + result["quarantined_count"]
    assert total == result["bronze_record_count"] == 3


def test_iso_currency_real_bronze_449_to_307_still_reconciles():
    """Verifies the documented real-data 449 -> 307 behaviour (docs/21 §3.3) is
    still correctly reconciled after the S7 refactor to common.reconcile_counts()."""
    result = iso_currency.run()
    assert result["bronze_record_count"] == 449
    assert result["valid_count"] == 307
    assert result["valid_count"] + result["merged_duplicate_count"] + result["quarantined_count"] == 449


# ==================================================================
# FX rate: immutability conflict accounted as quarantine
# ==================================================================


def test_fx_rate_immutability_conflict_is_accounted_as_quarantine(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"

    def make_run(run_id, rate):
        run_dir = bronze_root / "currency" / run_id
        run_dir.mkdir(parents=True)
        raw = {"base": "EUR", "date": "2026-09-18", "rates": {"USD": rate}}
        (run_dir / "rates.json").write_text(json.dumps(raw), encoding="utf-8")
        (run_dir / "metadata.json").write_text(
            json.dumps({"raw_file": "rates.json", "record_count": 1, "ingestion_timestamp_utc": "2026-09-18T08:31:49Z"}),
            encoding="utf-8",
        )

    make_run("run_1", 1.10)
    fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    make_run("run_2", 1.25)  # same (date, base, quote), conflicting rate
    result = fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["conflict_count"] == 1
    assert result["quarantined_count"] == 1  # the conflict IS the quarantine disposition
    assert result["appended_count"] + result["unchanged_count"] + result["quarantined_count"] == result["bronze_record_count"]

    rejected = quarantine.read_quarantine(result["quarantine_path"])
    assert rejected[0]["failing_check_name"] == "immutability_conflict"


# ==================================================================
# Merchant: vacant+unnamed overlap must not double-count
# ==================================================================


def test_merchant_vacant_and_unnamed_overlap_is_not_double_counted(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    run_dir = bronze_root / "merchant_osm" / "run_1"
    run_dir.mkdir(parents=True)
    named = {"type": "node", "id": 1, "lat": 1, "lon": 1, "tags": {"name": "Real Shop", "shop": "bakery"}}
    # Matches the real Bronze pattern exactly: shop=vacant nodes have no name tag at all.
    vacant_and_unnamed = {"type": "node", "id": 2, "lat": 1, "lon": 1, "tags": {"shop": "vacant"}}
    raw = {"version": 0.6, "osm3s": {"timestamp_osm_base": "2026-09-20T08:30:16Z"}, "elements": [named, vacant_and_unnamed]}
    (run_dir / "osm_places.json").write_text(json.dumps(raw), encoding="utf-8")
    (run_dir / "metadata.json").write_text(
        json.dumps({"raw_file": "osm_places.json", "record_count": 2, "ingestion_timestamp_utc": "2026-09-20T08:31:50Z"}),
        encoding="utf-8",
    )
    result = merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    # The element is vacant AND unnamed — it must be counted in exactly one bucket
    # (excluded_vacant_shop, checked first), never both.
    assert result["excluded_vacant_shop"] == 1
    assert result["excluded_unnamed"] == 0
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
    assert total == result["bronze_record_count"] == 2


def test_merchant_vacant_and_unnamed_partial_overlap_reconciles(tmp_path):
    """Controlled fixture equivalent of the real-Bronze finding (S5): some
    shop=vacant nodes are also unnamed (excluded as vacant, never double-counted
    as unnamed too), while other nodes are unnamed but NOT vacant (excluded
    separately as unnamed) — i.e. the vacant/unnamed sets only partially overlap.
    Deterministic — no dependency on the current contents of data/bronze/ (see
    the API Automation Test Failures audit; this test previously called
    merchant.run() with real paths and hardcoded the real snapshot's exact
    40/5 counts, which are now stale)."""
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    run_dir = bronze_root / "merchant_osm" / "run_1"
    run_dir.mkdir(parents=True)
    named = {"type": "node", "id": 1, "lat": 1, "lon": 1, "tags": {"name": "Real Shop", "shop": "bakery"}}
    vacant_and_unnamed = [
        {"type": "node", "id": 100 + i, "lat": 1, "lon": 1, "tags": {"shop": "vacant"}} for i in range(3)
    ]
    unnamed_not_vacant = [
        {"type": "node", "id": 200 + i, "lat": 1, "lon": 1, "tags": {"shop": "newsagent"}} for i in range(2)
    ]
    elements = [named, *vacant_and_unnamed, *unnamed_not_vacant]
    raw = {"version": 0.6, "osm3s": {"timestamp_osm_base": "2026-09-20T08:30:16Z"}, "elements": elements}
    (run_dir / "osm_places.json").write_text(json.dumps(raw), encoding="utf-8")
    (run_dir / "metadata.json").write_text(
        json.dumps(
            {"raw_file": "osm_places.json", "record_count": len(elements), "ingestion_timestamp_utc": "2026-09-20T08:31:50Z"}
        ),
        encoding="utf-8",
    )
    result = merchant.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["excluded_vacant_shop"] == 3
    assert result["excluded_unnamed"] == 2  # unnamed-but-not-vacant only — no double-counting
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
    assert total == result["bronze_record_count"] == len(elements)


# ==================================================================
# GLEIF: jurisdiction survival through the full Silver layer (mandatory, §11)
# ==================================================================


def test_gleif_jurisdiction_values_survive_the_full_silver_layer_with_correct_counts(tmp_path):
    """Controlled fixture equivalent of the real-GLEIF finding (S6):
    filter[entity.jurisdiction]=GB returns GB, GB-SCT, and GB-NIR records, and
    MULTIPLE records sharing the same jurisdiction value must all be counted
    correctly (not just each value individually preserved, as
    test_gleif_jurisdiction_not_folded_by_reconciliation_or_validation already
    covers with one record per jurisdiction). Deterministic — no dependency on
    the current contents of data/bronze/gleif or data/silver/legal_entity (see
    the API Automation Test Failures audit; this test previously called
    legal_entity.run() with real paths and hardcoded the real snapshot's exact
    9949/34/17 counts, which are now stale)."""
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    run_dir = bronze_root / "gleif" / "run_1"
    run_dir.mkdir(parents=True)

    def rec(lei, jurisdiction):
        return {
            "type": "lei-records",
            "id": lei,
            "attributes": {
                "lei": lei,
                "entity": {
                    "legalName": {"name": f"Company {lei}"},
                    "legalAddress": {},
                    "headquartersAddress": {},
                    "jurisdiction": jurisdiction,
                    "status": "ACTIVE",
                },
                "registration": {"status": "ISSUED", "lastUpdateDate": "2026-06-01T00:00:00Z"},
            },
        }

    gb_records = [rec(f"549300GB{i:012d}", "GB") for i in range(3)]
    gb_sct_records = [rec(f"549300SC{i:012d}", "GB-SCT") for i in range(2)]
    gb_nir_records = [rec(f"549300NI{i:012d}", "GB-NIR") for i in range(1)]
    records = gb_records + gb_sct_records + gb_nir_records

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
    result = legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    rows = common.read_parquet(result["silver_path"])
    jurisdictions = {r["entity_jurisdiction"] for r in rows}
    assert jurisdictions == {"GB", "GB-SCT", "GB-NIR"}  # no unexpected folding/splitting introduced

    gb_count = sum(1 for r in rows if r["entity_jurisdiction"] == "GB")
    gb_sct_count = sum(1 for r in rows if r["entity_jurisdiction"] == "GB-SCT")
    gb_nir_count = sum(1 for r in rows if r["entity_jurisdiction"] == "GB-NIR")
    assert gb_count == 3
    assert gb_sct_count == 2
    assert gb_nir_count == 1


def test_gleif_jurisdiction_not_folded_by_reconciliation_or_validation(tmp_path):
    """End-to-end proof (not just _transform_record in isolation): GB-SCT and
    GB-NIR go through the real run() pipeline — validation, reconciliation,
    quarantine — and still come out unfolded."""
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    run_dir = bronze_root / "gleif" / "run_1"
    run_dir.mkdir(parents=True)

    def rec(lei, jurisdiction):
        return {
            "type": "lei-records",
            "id": lei,
            "attributes": {
                "lei": lei,
                "entity": {
                    "legalName": {"name": f"Company {jurisdiction}"},
                    "legalAddress": {},
                    "headquartersAddress": {},
                    "jurisdiction": jurisdiction,
                    "status": "ACTIVE",
                },
                "registration": {"status": "ISSUED", "lastUpdateDate": "2026-06-01T00:00:00Z"},
            },
        }

    records = [
        rec("549300ABCDEFGHIJKL01", "GB"),
        rec("549300ABCDEFGHIJKL02", "GB-SCT"),
        rec("549300ABCDEFGHIJKL03", "GB-NIR"),
    ]
    page = {"meta": {"pagination": {"currentPage": 1, "lastPage": 1}}, "data": records}
    (run_dir / "page_0001.json").write_text(json.dumps(page), encoding="utf-8")
    (run_dir / "metadata.json").write_text(
        json.dumps(
            {
                "raw_file": "page_0001.json",
                "record_count": 3,
                "ingestion_timestamp_utc": "2026-09-21T05:44:17Z",
                "page_files": ["page_0001.json"],
            }
        ),
        encoding="utf-8",
    )
    result = legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["quarantined_count"] == 0
    assert result["inserted_count"] == 3
    rows = common.read_parquet(result["silver_path"])
    jurisdictions = {r["entity_jurisdiction"] for r in rows}
    assert jurisdictions == {"GB", "GB-SCT", "GB-NIR"}


# ==================================================================
# GLEIF: upsert dispositions reconcile correctly (insert/update/unchanged/stale/conflict together)
# ==================================================================


def test_gleif_upsert_dispositions_reconcile_together(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"

    def rec(lei, name, last_update):
        return {
            "type": "lei-records",
            "id": lei,
            "attributes": {
                "lei": lei,
                "entity": {
                    "legalName": {"name": name},
                    "legalAddress": {},
                    "headquartersAddress": {},
                    "jurisdiction": "GB",
                    "status": "ACTIVE",
                },
                "registration": {"status": "ISSUED", "lastUpdateDate": last_update},
            },
        }

    def make_run(run_id, records):
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

    lei_unchanged = "549300ABCDEFGHIJKL01"
    lei_updated = "549300ABCDEFGHIJKL02"
    lei_stale = "549300ABCDEFGHIJKL03"

    make_run(
        "run_1",
        [
            rec(lei_unchanged, "Stable Co", "2026-01-01T00:00:00Z"),
            rec(lei_updated, "Old Name", "2026-01-01T00:00:00Z"),
            rec(lei_stale, "Current Name", "2026-06-01T00:00:00Z"),
        ],
    )
    legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    make_run(
        "run_2",
        [
            rec(lei_unchanged, "Stable Co", "2026-01-01T00:00:00Z"),  # identical -> unchanged
            rec(lei_updated, "New Name", "2026-06-01T00:00:00Z"),  # newer + different -> update
            rec(lei_stale, "Stale Attempt", "2026-01-01T00:00:00Z"),  # older + different -> stale_skipped
            rec("549300ABCDEFGHIJKL04", "Brand New Co", "2026-06-01T00:00:00Z"),  # new -> insert
        ],
    )
    result = legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    assert result["inserted_count"] == 1
    assert result["updated_count"] == 1
    assert result["unchanged_count"] == 1
    assert result["stale_skipped_count"] == 1
    assert result["quarantined_count"] == 0
    total = (
        result["inserted_count"] + result["updated_count"] + result["unchanged_count"] + result["stale_skipped_count"] + result["quarantined_count"]
    )
    assert total == result["bronze_record_count"] == 4

    rows = common.read_parquet(result["silver_path"])
    by_lei = {r["lei"]: r for r in rows}
    assert by_lei[lei_updated]["legal_name"] == "New Name"
    assert by_lei[lei_stale]["legal_name"] == "Current Name"  # stale attempt did not overwrite
