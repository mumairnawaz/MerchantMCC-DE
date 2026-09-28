import json
from pathlib import Path

from src.silver import card_issuer, common, quarantine

BIN_HEADER = "BIN,Brand,Type,Category,Issuer,IssuerPhone,IssuerUrl,isoCode2,isoCode3,CountryName\n"


def _make_bronze_run(bronze_root: Path, run_id: str, csv_body: str, record_count: int | None = None) -> Path:
    run_dir = bronze_root / "card_issuer" / run_id
    run_dir.mkdir(parents=True)
    lines = csv_body.count("\n") if record_count is None else record_count
    metadata = {
        "raw_file": "bin_list_data.csv",
        "record_count": lines,
        "ingestion_timestamp_utc": "2026-09-21T00:00:00Z",
    }
    (run_dir / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    (run_dir / "bin_list_data.csv").write_text(BIN_HEADER + csv_body, encoding="utf-8")
    return run_dir


def _run(tmp_path, csv_body: str, record_count: int | None = None):
    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    quarantine_root = tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_20260921T000000Z", csv_body, record_count=record_count)
    result = card_issuer.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    return result, silver_root, quarantine_root


ROW_WITH_ISSUER = '002102,"PRIVATE LABEL",CREDIT,STANDARD,"CHINA MERCHANTS BANK",95555,https://english.cmbchina.com,CN,CHN,CHINA\n'
ROW_WITHOUT_ISSUER = "400000,VISA,DEBIT,CLASSIC,,,,US,USA,UNITED STATES\n"


# ---- transform correctness ----


def test_transform_maps_all_fields():
    record = card_issuer._transform_record(
        {
            "BIN": "002102",
            "Brand": "PRIVATE LABEL",
            "Type": "CREDIT",
            "Category": "STANDARD",
            "Issuer": "CHINA MERCHANTS BANK",
            "IssuerPhone": "95555",
            "IssuerUrl": "https://english.cmbchina.com",
            "isoCode2": "cn",
            "isoCode3": "chn",
            "CountryName": "CHINA",
        }
    )
    assert record["bin_range"] == "002102"
    assert record["issuer_country_alpha2"] == "CN"
    assert record["issuer_country_alpha3"] == "CHN"
    assert record["has_known_issuer"] is True


def test_transform_preserves_leading_zero_bin():
    record = card_issuer._transform_record({"BIN": "002102", "Brand": "VISA"})
    assert record["bin_range"] == "002102"
    assert isinstance(record["bin_range"], str)


def test_transform_blank_issuer_is_none_and_has_known_issuer_false():
    record = card_issuer._transform_record({"BIN": "400000", "Brand": "VISA", "Issuer": ""})
    assert record["issuer_name"] is None
    assert record["has_known_issuer"] is False


def test_transform_missing_issuer_key_is_none():
    record = card_issuer._transform_record({"BIN": "400000", "Brand": "VISA"})
    assert record["issuer_name"] is None
    assert record["has_known_issuer"] is False


# ---- validation ----


def test_validate_rejects_malformed_bin():
    record = card_issuer._transform_record({"BIN": "40000", "Brand": "VISA"})
    assert card_issuer._validate_record(record)["accepted"] is False


def test_validate_rejects_missing_brand():
    record = card_issuer._transform_record({"BIN": "400000", "Brand": ""})
    assert card_issuer._validate_record(record)["accepted"] is False


def test_validate_accepts_well_formed_record():
    record = card_issuer._transform_record({"BIN": "400000", "Brand": "VISA"})
    assert card_issuer._validate_record(record)["accepted"] is True


# ---- end-to-end run() ----


def test_run_writes_valid_records(tmp_path):
    result, silver_root, _ = _run(tmp_path, ROW_WITH_ISSUER + ROW_WITHOUT_ISSUER)
    rows = common.read_parquet(result["silver_path"])
    assert len(rows) == 2
    assert result["quarantined_count"] == 0


def test_run_quarantines_malformed_bin(tmp_path):
    result, _, _ = _run(tmp_path, ROW_WITH_ISSUER + "40000,VISA,DEBIT,CLASSIC,,,,US,USA,UNITED STATES\n")
    assert result["valid_count"] == 1
    assert result["quarantined_count"] == 1


def test_run_reconciliation_matches_bronze_record_count(tmp_path):
    result, _, _ = _run(tmp_path, ROW_WITH_ISSUER + "40000,VISA,DEBIT,CLASSIC,,,,US,USA,UNITED STATES\n")
    assert result["valid_count"] + result["quarantined_count"] == result["bronze_record_count"]


def test_run_lineage_present(tmp_path):
    result, silver_root, _ = _run(tmp_path, ROW_WITH_ISSUER)
    rows = common.read_parquet(result["silver_path"])
    for field in common.STANDARD_LINEAGE_FIELDS:
        assert field in rows[0]


def test_run_defensive_dedupe_quarantines_duplicate_bin(tmp_path):
    result, silver_root, _ = _run(tmp_path, ROW_WITH_ISSUER + "002102,VISA,DEBIT,CLASSIC,,,,US,USA,UNITED STATES\n")
    assert result["valid_count"] == 1
    assert result["quarantined_count"] == 1
    rejected = quarantine.read_quarantine(result["quarantine_path"])
    assert rejected[0]["failing_check_name"] == "uniqueness_bin_range"


def test_run_is_idempotent_except_processed_at(tmp_path):
    result, silver_root, _ = _run(tmp_path, ROW_WITH_ISSUER)
    first = common.read_parquet(result["silver_path"])

    bronze_root = tmp_path / "bronze"
    quarantine_root = tmp_path / "quarantine"
    result2 = card_issuer.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    second = common.read_parquet(result2["silver_path"])

    for f in first[0]:
        if f == "silver_processed_at_utc":
            continue
        assert first[0][f] == second[0][f]
