import json
from pathlib import Path

from src.silver import common, iso_currency, quarantine

CSV_HEADER = "Entity,Currency,AlphabeticCode,NumericCode,MinorUnit,WithdrawalDate\n"


def _make_bronze_run(bronze_root: Path, run_id: str, csv_body: str, record_count: int | None = None) -> Path:
    run_dir = bronze_root / "iso_currency" / run_id
    run_dir.mkdir(parents=True)
    lines = csv_body.count("\n") if record_count is None else record_count
    metadata = {
        "raw_file": "iso_currency_codes.csv",
        "record_count": lines,
        "ingestion_timestamp_utc": "2026-09-21T00:00:00Z",
    }
    (run_dir / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    (run_dir / "iso_currency_codes.csv").write_text(CSV_HEADER + csv_body, encoding="utf-8")
    return run_dir


def _run(tmp_path, csv_body: str, record_count: int | None = None):
    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    quarantine_root = tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_20260921T000000Z", csv_body, record_count=record_count)
    result = iso_currency.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    return result, silver_root, quarantine_root


# ---- row transform ----


def test_transform_row_uppercases_code():
    row = iso_currency._transform_row({"AlphabeticCode": "afn", "Currency": "Afghani", "NumericCode": "971", "MinorUnit": "2", "WithdrawalDate": ""})
    assert row["currency_code"] == "AFN"
    assert row["minor_unit"] == 2
    assert row["withdrawal_date"] is None


def test_transform_row_blank_minor_unit_is_none_not_defaulted():
    row = iso_currency._transform_row({"AlphabeticCode": "AFN", "Currency": "Afghani", "MinorUnit": ""})
    assert row["minor_unit"] is None


# ---- merge rule: first non-null wins per field, independently ----


def test_merge_group_currency_name_first_non_null_wins():
    rows = [
        {"currency_name": None, "currency_numeric_code": None, "minor_unit": None, "withdrawal_date": None},
        {"currency_name": "US Dollar", "currency_numeric_code": "840", "minor_unit": 2, "withdrawal_date": None},
    ]
    merged = iso_currency._merge_group("USD", rows)
    assert merged["currency_name"] == "US Dollar"
    assert merged["currency_numeric_code"] == "840"
    assert merged["minor_unit"] == 2


def test_merge_group_first_row_wins_when_both_populated():
    rows = [
        {"currency_name": "US Dollar", "currency_numeric_code": "840", "minor_unit": 2, "withdrawal_date": None},
        {"currency_name": "United States Dollar", "currency_numeric_code": "840", "minor_unit": 2, "withdrawal_date": None},
    ]
    merged = iso_currency._merge_group("USD", rows)
    assert merged["currency_name"] == "US Dollar"


def test_merge_group_all_null_field_stays_none():
    rows = [{"currency_name": "X", "currency_numeric_code": None, "minor_unit": None, "withdrawal_date": None}]
    merged = iso_currency._merge_group("XXX", rows)
    assert merged["minor_unit"] is None


def test_merge_group_is_active_true_when_withdrawal_date_none():
    rows = [{"currency_name": "X", "currency_numeric_code": None, "minor_unit": None, "withdrawal_date": None}]
    assert iso_currency._merge_group("XXX", rows)["is_active"] is True


def test_merge_group_is_active_false_when_withdrawal_date_present():
    rows = [{"currency_name": "X", "currency_numeric_code": None, "minor_unit": None, "withdrawal_date": "2015-01-01"}]
    assert iso_currency._merge_group("XXX", rows)["is_active"] is False


# ---- end-to-end run() ----


def test_run_deduplicates_multiple_entities_to_one_currency_row(tmp_path):
    csv_body = (
        "UNITED STATES,US Dollar,USD,840,2,\n"
        "ECUADOR,US Dollar,USD,840,2,\n"
        "EL SALVADOR,US Dollar,USD,840,2,\n"
    )
    result, silver_root, _ = _run(tmp_path, csv_body)
    rows = common.read_parquet(result["silver_path"])
    assert len(rows) == 1
    assert rows[0]["currency_code"] == "USD"
    assert result["merged_duplicate_count"] == 2


def test_run_merges_across_rows_with_partial_data(tmp_path):
    # First row for AFN is missing MinorUnit; second row (different Entity, same
    # currency) has it — the merge rule must rescue it rather than losing it.
    csv_body = "AFGHANISTAN,Afghani,AFN,971,,\n" "SOME OTHER USER,Afghani,AFN,971,2,\n"
    result, silver_root, _ = _run(tmp_path, csv_body)
    rows = common.read_parquet(result["silver_path"])
    assert len(rows) == 1
    assert rows[0]["minor_unit"] == 2


def test_run_reconciliation_matches_bronze_record_count(tmp_path):
    csv_body = "UNITED STATES,US Dollar,USD,840,2,\n" "ECUADOR,US Dollar,USD,840,2,\n" "AFGHANISTAN,Afghani,AFN,971,2,\n"
    result, _, _ = _run(tmp_path, csv_body)
    total = result["valid_count"] + result["merged_duplicate_count"] + result["quarantined_count"]
    assert total == result["bronze_record_count"]


def test_run_quarantines_row_with_malformed_currency_code(tmp_path):
    csv_body = "UNITED STATES,US Dollar,USD,840,2,\n" "NOWHERE,Bad,1X,000,,\n"
    result, _, quarantine_root = _run(tmp_path, csv_body)
    assert result["quarantined_count"] == 1
    assert result["valid_count"] == 1


def test_run_quarantines_group_when_currency_name_blank_for_every_row(tmp_path):
    csv_body = "UNITED STATES,US Dollar,USD,840,2,\n" "NOWHERE,,ZZZ,000,,\n" "ELSEWHERE,,ZZZ,000,,\n"
    result, _, _ = _run(tmp_path, csv_body)
    assert result["valid_count"] == 1
    assert result["quarantined_count"] == 2
    rejected = quarantine.read_quarantine(result["quarantine_path"])
    assert rejected[0]["failing_check_name"] == "required_field_currency_name"
    assert isinstance(rejected[0]["original_raw_record"], list)
    assert len(rejected[0]["original_raw_record"]) == 2


def test_run_lineage_present(tmp_path):
    result, silver_root, _ = _run(tmp_path, "UNITED STATES,US Dollar,USD,840,2,\n")
    rows = common.read_parquet(result["silver_path"])
    for field in common.STANDARD_LINEAGE_FIELDS:
        assert field in rows[0]


def test_run_is_idempotent_except_processed_at(tmp_path):
    csv_body = "UNITED STATES,US Dollar,USD,840,2,\n" "ECUADOR,US Dollar,USD,840,2,\n"
    result, silver_root, _ = _run(tmp_path, csv_body)
    first = common.read_parquet(result["silver_path"])

    bronze_root = tmp_path / "bronze"
    quarantine_root = tmp_path / "quarantine"
    result2 = iso_currency.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)
    second = common.read_parquet(result2["silver_path"])

    assert len(first) == len(second) == 1
    for f in first[0]:
        if f == "silver_processed_at_utc":
            continue
        assert first[0][f] == second[0][f]
