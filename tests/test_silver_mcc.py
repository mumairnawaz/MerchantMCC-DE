import json
from pathlib import Path

from src.silver import common, mcc, quarantine

MCC_HEADER = "mcc,edited_description,combined_description,usda_description,irs_description,irs_reportable\n"


def _make_bronze_run(bronze_root: Path, run_id: str, csv_body: str, record_count: int | None = None) -> Path:
    run_dir = bronze_root / "mcc" / run_id
    run_dir.mkdir(parents=True)
    csv_content = MCC_HEADER + csv_body
    lines = csv_body.count("\n") if record_count is None else record_count
    metadata = {
        "raw_file": "mcc_codes.csv",
        "record_count": lines,
        "ingestion_timestamp_utc": "2026-09-21T00:00:00Z",
    }
    (run_dir / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    (run_dir / "mcc_codes.csv").write_text(csv_content, encoding="utf-8")
    return run_dir


def _run(tmp_path, csv_body: str, record_count: int | None = None):
    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    quarantine_root = tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_20260921T000000Z", csv_body, record_count=record_count)
    result = mcc.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    return result, silver_root, quarantine_root


# ---- transform correctness ----


def test_transform_maps_all_fields():
    raw = {
        "mcc": "0742",
        "edited_description": " Veterinary Services ",
        "combined_description": "Veterinary Services",
        "usda_description": "Veterinary Services",
        "irs_description": "Veterinary Services",
        "irs_reportable": "Yes",
    }
    record = mcc._transform_record(raw)
    assert record["mcc_code"] == "0742"
    assert record["description"] == "Veterinary Services"
    assert record["irs_reportable"] is True


def test_transform_preserves_leading_zero_mcc_code():
    record = mcc._transform_record({"mcc": "0742", "edited_description": "Vet"})
    assert record["mcc_code"] == "0742"
    assert isinstance(record["mcc_code"], str)


def test_transform_blank_optional_fields_become_none():
    record = mcc._transform_record({"mcc": "0742", "edited_description": "Vet", "combined_description": ""})
    assert record["description_combined"] is None


def test_transform_irs_reportable_no_maps_to_false():
    record = mcc._transform_record({"mcc": "0742", "edited_description": "Vet", "irs_reportable": "No"})
    assert record["irs_reportable"] is False


def test_transform_irs_reportable_blank_maps_to_none():
    record = mcc._transform_record({"mcc": "0742", "edited_description": "Vet", "irs_reportable": ""})
    assert record["irs_reportable"] is None


# ---- validation ----


def test_validate_rejects_malformed_mcc_code():
    record = mcc._transform_record({"mcc": "74X", "edited_description": "Vet"})
    verdict = mcc._validate_record(record)
    assert verdict["accepted"] is False


def test_validate_rejects_missing_description():
    record = mcc._transform_record({"mcc": "0742", "edited_description": ""})
    verdict = mcc._validate_record(record)
    assert verdict["accepted"] is False


def test_validate_accepts_well_formed_record():
    record = mcc._transform_record({"mcc": "0742", "edited_description": "Vet"})
    assert mcc._validate_record(record)["accepted"] is True


# ---- end-to-end run() ----


def test_run_writes_valid_records_to_parquet(tmp_path):
    result, silver_root, _ = _run(tmp_path, "0742,Veterinary Services,,,,Yes\n5812,Eating Places,,,,No\n")
    rows = common.read_parquet(result["silver_path"])
    assert len(rows) == 2
    assert result["valid_count"] == 2
    assert result["quarantined_count"] == 0


def test_run_quarantines_malformed_row_and_still_writes_valid_ones(tmp_path):
    result, silver_root, quarantine_root = _run(tmp_path, "0742,Veterinary Services,,,,Yes\n74X,Bad Code,,,,No\n")
    assert result["valid_count"] == 1
    assert result["quarantined_count"] == 1
    rejected = quarantine.read_quarantine(result["quarantine_path"])
    assert rejected[0]["source_record_identifier"] == "74X"
    assert rejected[0]["failing_check_name"] in {"pattern_mcc_code", "required_field_mcc_code"}


def test_run_reconciliation_matches_bronze_record_count(tmp_path):
    result, _, _ = _run(tmp_path, "0742,Veterinary Services,,,,Yes\n74X,Bad Code,,,,No\n")
    assert result["valid_count"] + result["quarantined_count"] == result["bronze_record_count"]


def test_run_lineage_fields_present_on_every_row(tmp_path):
    result, silver_root, _ = _run(tmp_path, "0742,Veterinary Services,,,,Yes\n")
    rows = common.read_parquet(result["silver_path"])
    for field in common.STANDARD_LINEAGE_FIELDS:
        assert field in rows[0]
    assert rows[0]["source_name"] == "mcc"
    assert rows[0]["bronze_run_id"] == "run_20260921T000000Z"


def test_run_overwrites_previous_snapshot(tmp_path):
    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    quarantine_root = tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_20260921T000000Z", "0742,Veterinary Services,,,,Yes\n5812,Eating Places,,,,No\n")
    mcc.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    import shutil

    shutil.rmtree(bronze_root / "mcc" / "run_20260921T000000Z")
    _make_bronze_run(bronze_root, "run_20260922T000000Z", "0742,Veterinary Services,,,,Yes\n")
    result = mcc.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)

    rows = common.read_parquet(result["silver_path"])
    assert len(rows) == 1


def test_run_is_idempotent_except_processed_at(tmp_path):
    result, silver_root, _ = _run(tmp_path, "0742,Veterinary Services,,,,Yes\n")
    first = common.read_parquet(result["silver_path"])

    bronze_root = tmp_path / "bronze"
    quarantine_root = tmp_path / "quarantine"
    result2 = mcc.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    second = common.read_parquet(result2["silver_path"])

    assert len(first) == len(second) == 1
    for f in first[0]:
        if f == "silver_processed_at_utc":
            continue
        assert first[0][f] == second[0][f]


def test_run_defensive_dedupe_quarantines_duplicate_mcc_code(tmp_path):
    result, silver_root, quarantine_root = _run(
        tmp_path, "0742,Veterinary Services,,,,Yes\n0742,Duplicate Vet,,,,Yes\n"
    )
    assert result["valid_count"] == 1
    assert result["quarantined_count"] == 1
    rejected = quarantine.read_quarantine(result["quarantine_path"])
    assert rejected[0]["failing_check_name"] == "uniqueness_mcc_code"
    rows = common.read_parquet(result["silver_path"])
    assert rows[0]["description"] == "Veterinary Services"


def test_run_raises_file_not_found_when_no_bronze_run_exists(tmp_path):
    import pytest

    bronze_root = tmp_path / "bronze"
    bronze_root.mkdir()
    with pytest.raises(FileNotFoundError):
        mcc.run(bronze_root=bronze_root, silver_root=tmp_path / "silver", quarantine_root=tmp_path / "quarantine")
