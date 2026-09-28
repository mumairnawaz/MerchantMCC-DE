import json
from pathlib import Path

from src.silver import common, country, quarantine

GB = {
    "name": {"common": "United Kingdom", "official": "United Kingdom of Great Britain and Northern Ireland"},
    "cca2": "gb",
    "cca3": "gbr",
    "ccn3": "826",
    "region": "Europe",
    "subregion": "Northern Europe",
    "capital": ["London"],
    "currencies": {"GBP": {"name": "British pound", "symbol": "£"}},
    "independent": True,
    "unMember": True,
}


def _make_bronze_run(bronze_root: Path, run_id: str, records: list, record_count: int | None = None) -> Path:
    run_dir = bronze_root / "country" / run_id
    run_dir.mkdir(parents=True)
    metadata = {
        "raw_file": "countries.json",
        "record_count": len(records) if record_count is None else record_count,
        "ingestion_timestamp_utc": "2026-09-20T08:38:48Z",
    }
    (run_dir / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    (run_dir / "countries.json").write_text(json.dumps(records), encoding="utf-8")
    return run_dir


def _run(tmp_path, records: list, record_count: int | None = None):
    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    quarantine_root = tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_20260921T000000Z", records, record_count=record_count)
    result = country.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    return result, silver_root, quarantine_root


# ---- transform correctness ----


def test_transform_uppercases_codes():
    record = country._transform_record(GB)
    assert record["country_code_alpha3"] == "GBR"
    assert record["country_code_alpha2"] == "GB"


def test_transform_maps_names():
    record = country._transform_record(GB)
    assert record["country_name"] == "United Kingdom"
    assert record["country_official_name"] == "United Kingdom of Great Britain and Northern Ireland"


def test_transform_capital_is_first_of_array():
    raw = dict(GB, capital=["London", "Other"])
    assert country._transform_record(raw)["capital"] == "London"


def test_transform_capital_empty_array_is_none():
    raw = dict(GB, capital=[])
    assert country._transform_record(raw)["capital"] is None


def test_transform_capital_missing_key_is_none():
    raw = {k: v for k, v in GB.items() if k != "capital"}
    assert country._transform_record(raw)["capital"] is None


def test_transform_default_currency_code_is_first_key():
    raw = dict(GB, currencies={"GBP": {"name": "British pound"}})
    assert country._transform_record(raw)["default_currency_code"] == "GBP"


def test_transform_default_currency_code_empty_dict_is_none():
    raw = dict(GB, currencies={})
    assert country._transform_record(raw)["default_currency_code"] is None


def test_transform_country_code_numeric_missing_is_none():
    raw = {k: v for k, v in GB.items() if k != "ccn3"}
    assert country._transform_record(raw)["country_code_numeric"] is None


def test_transform_booleans_preserved():
    record = country._transform_record(GB)
    assert record["is_independent"] is True
    assert record["is_un_member"] is True


# ---- validation ----


def test_validate_rejects_malformed_alpha3():
    record = country._transform_record(dict(GB, cca3="G1"))
    assert country._validate_record(record)["accepted"] is False


def test_validate_rejects_missing_country_name():
    raw = dict(GB, name={"common": "", "official": "x"})
    record = country._transform_record(raw)
    assert country._validate_record(record)["accepted"] is False


def test_validate_accepts_well_formed_record():
    assert country._validate_record(country._transform_record(GB))["accepted"] is True


# ---- end-to-end run() ----


def test_run_writes_valid_records(tmp_path):
    result, silver_root, _ = _run(tmp_path, [GB])
    rows = common.read_parquet(result["silver_path"])
    assert len(rows) == 1
    assert rows[0]["country_code_alpha3"] == "GBR"


def test_run_quarantines_malformed_record(tmp_path):
    bad = dict(GB, cca3="G1")
    result, _, _ = _run(tmp_path, [GB, bad])
    assert result["valid_count"] == 1
    assert result["quarantined_count"] == 1


def test_run_reconciliation_matches_bronze_record_count(tmp_path):
    bad = dict(GB, cca3="G1")
    result, _, _ = _run(tmp_path, [GB, bad])
    assert result["valid_count"] + result["quarantined_count"] == result["bronze_record_count"]


def test_run_lineage_present(tmp_path):
    result, silver_root, _ = _run(tmp_path, [GB])
    rows = common.read_parquet(result["silver_path"])
    for field in common.STANDARD_LINEAGE_FIELDS:
        assert field in rows[0]


def test_run_defensive_dedupe_quarantines_duplicate_alpha3(tmp_path):
    dup = dict(GB, name={"common": "Duplicate UK", "official": "x"})
    result, silver_root, _ = _run(tmp_path, [GB, dup])
    assert result["valid_count"] == 1
    assert result["quarantined_count"] == 1
    rejected = quarantine.read_quarantine(result["quarantine_path"])
    assert rejected[0]["failing_check_name"] == "uniqueness_country_code_alpha3"
    rows = common.read_parquet(result["silver_path"])
    assert rows[0]["country_name"] == "United Kingdom"


def test_run_is_idempotent_except_processed_at(tmp_path):
    result, silver_root, _ = _run(tmp_path, [GB])
    first = common.read_parquet(result["silver_path"])

    bronze_root = tmp_path / "bronze"
    quarantine_root = tmp_path / "quarantine"
    result2 = country.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    second = common.read_parquet(result2["silver_path"])

    for f in first[0]:
        if f == "silver_processed_at_utc":
            continue
        assert first[0][f] == second[0][f]
