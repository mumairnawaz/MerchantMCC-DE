import json
from decimal import Decimal
from pathlib import Path

from src.silver import common, fx_rate, quarantine

FLAT_RAW = {
    "amount": 1.0,
    "base": "eur",
    "date": "2026-09-18",
    "rates": {"aud": 1.6095, "usd": 1.10},
}

RANGE_RAW = {
    "amount": 1.0,
    "base": "EUR",
    "start_date": "2026-09-15",
    "end_date": "2026-09-16",
    "rates": {
        "2026-09-15": {"USD": 1.10},
        "2026-09-16": {"USD": 1.11, "GBP": 0.86},
    },
}


def _make_bronze_run(bronze_root: Path, run_id: str, raw: dict, record_count: int | None = None) -> Path:
    run_dir = bronze_root / "currency" / run_id
    run_dir.mkdir(parents=True)
    metadata = {
        "raw_file": "rates.json",
        "record_count": len(fx_rate._flatten(raw)) if record_count is None else record_count,
        "ingestion_timestamp_utc": "2026-09-18T08:31:49Z",
    }
    (run_dir / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    (run_dir / "rates.json").write_text(json.dumps(raw), encoding="utf-8")
    return run_dir


def _run(bronze_root: Path, silver_root: Path, quarantine_root: Path):
    return fx_rate.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)


# ---- shape parsing (1, 2, 3, 4) ----


def test_flatten_flat_shape_one_record_per_currency():
    records = fx_rate._flatten(FLAT_RAW)
    assert len(records) == 2
    assert {"date": "2026-09-18", "base": "eur", "currency": "aud", "rate": 1.6095} in records


def test_flatten_range_shape_one_record_per_date_currency_pair():
    records = fx_rate._flatten(RANGE_RAW)
    assert len(records) == 3
    dates = {r["date"] for r in records}
    assert dates == {"2026-09-15", "2026-09-16"}


def test_is_range_response_detects_flat_vs_range():
    assert fx_rate._is_range_response(FLAT_RAW) is False
    assert fx_rate._is_range_response(RANGE_RAW) is True


# ---- transform (5, 6) ----


def test_transform_normalizes_currency_case():
    record = fx_rate._transform_record({"date": "2026-09-18", "base": "eur", "currency": "aud", "rate": 1.6095})
    assert record["base_currency"] == "EUR"
    assert record["quote_currency"] == "AUD"


def test_transform_converts_rate_to_decimal():
    record = fx_rate._transform_record({"date": "2026-09-18", "base": "EUR", "currency": "USD", "rate": 1.1})
    assert isinstance(record["exchange_rate"], Decimal)
    assert record["exchange_rate"] == Decimal("1.1")


def test_transform_preserves_source_date_exactly():
    record = fx_rate._transform_record({"date": "2026-09-18", "base": "EUR", "currency": "USD", "rate": 1.1})
    assert record["rate_date"].isoformat() == "2026-09-18"


# ---- validation (7, 8, 9) ----


def test_validate_rejects_missing_date():
    record = fx_rate._transform_record({"date": "", "base": "EUR", "currency": "USD", "rate": 1.1})
    assert fx_rate._validate_record(record)["accepted"] is False


def test_validate_rejects_missing_base():
    record = fx_rate._transform_record({"date": "2026-09-18", "base": "", "currency": "USD", "rate": 1.1})
    assert fx_rate._validate_record(record)["accepted"] is False


def test_validate_rejects_malformed_currency_code():
    record = fx_rate._transform_record({"date": "2026-09-18", "base": "EUR", "currency": "US", "rate": 1.1})
    assert fx_rate._validate_record(record)["accepted"] is False


def test_validate_rejects_non_numeric_rate():
    record = fx_rate._transform_record({"date": "2026-09-18", "base": "EUR", "currency": "USD", "rate": "not-a-number"})
    verdict = fx_rate._validate_record(record)
    assert verdict["accepted"] is False
    assert record["exchange_rate"] is None


def test_validate_rejects_zero_rate():
    record = fx_rate._transform_record({"date": "2026-09-18", "base": "EUR", "currency": "USD", "rate": 0})
    assert fx_rate._validate_record(record)["accepted"] is False


def test_validate_rejects_negative_rate():
    record = fx_rate._transform_record({"date": "2026-09-18", "base": "EUR", "currency": "USD", "rate": -1.5})
    assert fx_rate._validate_record(record)["accepted"] is False


def test_validate_accepts_well_formed_record():
    record = fx_rate._transform_record({"date": "2026-09-18", "base": "EUR", "currency": "USD", "rate": 1.1})
    assert fx_rate._validate_record(record)["accepted"] is True


# ---- end-to-end run(): first append, quarantine, reconciliation, lineage, partitioning (10-13, 18, 19) ----


def test_run_first_append_creates_silver_data(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_20260918T083149Z", FLAT_RAW)
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["appended_count"] == 2
    assert result["unchanged_count"] == 0
    assert result["conflict_count"] == 0
    assert result["quarantined_count"] == 0

    part_path = silver_root / "fx_rate" / "year=2026" / "month=09" / "part-run_20260918T083149Z.parquet"
    assert part_path in result["written_paths"]
    rows = common.read_parquet(part_path)
    assert len(rows) == 2
    assert {r["quote_currency"] for r in rows} == {"AUD", "USD"}
    assert all(r["base_currency"] == "EUR" for r in rows)


def test_run_partition_layout_is_year_month(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", FLAT_RAW)
    _run(bronze_root, silver_root, quarantine_root)
    assert (silver_root / "fx_rate" / "year=2026" / "month=09").is_dir()


def test_run_quarantines_malformed_row_and_still_appends_valid_ones(tmp_path):
    raw = {"base": "EUR", "date": "2026-09-20", "rates": {"USD": 1.1, "BAD": "invalid"}}
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", raw)
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["appended_count"] == 1
    assert result["quarantined_count"] == 1
    rejected = quarantine.read_quarantine(result["quarantine_path"])
    assert rejected[0]["source_record_identifier"] == "2026-09-20:EUR:BAD"


def test_run_reconciliation_matches_bronze_record_count(tmp_path):
    raw = {"base": "EUR", "date": "2026-09-20", "rates": {"USD": 1.1, "BAD": "invalid"}}
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", raw)
    result = _run(bronze_root, silver_root, quarantine_root)
    total = result["appended_count"] + result["unchanged_count"] + result["quarantined_count"]
    assert total == result["bronze_record_count"]


def test_run_lineage_fields_present(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", FLAT_RAW)
    result = _run(bronze_root, silver_root, quarantine_root)
    rows = common.read_parquet(result["written_paths"][0])
    for field in common.STANDARD_LINEAGE_FIELDS:
        assert field in rows[0]
    assert rows[0]["source_name"] == "currency"
    assert rows[0]["bronze_run_id"] == "run_1"


# ---- idempotency, append, immutability (11, 14, 15, 16, 17, 20) ----


def test_run_same_bronze_run_twice_is_a_no_op(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", FLAT_RAW)
    first = _run(bronze_root, silver_root, quarantine_root)
    second = _run(bronze_root, silver_root, quarantine_root)

    assert first["appended_count"] == 2
    assert second["appended_count"] == 0
    assert second["unchanged_count"] == 2
    assert second["written_paths"] == []

    part_path = silver_root / "fx_rate" / "year=2026" / "month=09" / "part-run_1.parquet"
    rows = common.read_parquet(part_path)
    assert len(rows) == 2  # not duplicated


def test_run_composite_key_uniqueness_holds_after_reprocessing(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", FLAT_RAW)
    _run(bronze_root, silver_root, quarantine_root)
    _run(bronze_root, silver_root, quarantine_root)

    partition_dir = silver_root / "fx_rate" / "year=2026" / "month=09"
    all_keys = []
    for part_file in partition_dir.glob("part-*.parquet"):
        for r in common.read_parquet(part_file):
            all_keys.append((r["rate_date"], r["base_currency"], r["quote_currency"]))
    assert len(all_keys) == len(set(all_keys))


def test_run_new_bronze_run_with_new_date_appends_without_touching_old_partition(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", FLAT_RAW)
    _run(bronze_root, silver_root, quarantine_root)

    new_date_raw = {"base": "EUR", "date": "2026-09-19", "rates": {"AUD": 1.61, "USD": 1.11}}
    _make_bronze_run(bronze_root, "run_2", new_date_raw)
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["appended_count"] == 2
    assert result["conflict_count"] == 0

    old_part = silver_root / "fx_rate" / "year=2026" / "month=09" / "part-run_1.parquet"
    old_rows = common.read_parquet(old_part)
    assert len(old_rows) == 2
    assert old_rows[0]["exchange_rate"] in (Decimal("1.609500"), Decimal("1.100000"), Decimal("1.6095"), Decimal("1.1"))

    new_part = silver_root / "fx_rate" / "year=2026" / "month=09" / "part-run_2.parquet"
    assert new_part.exists()


def test_run_existing_date_with_same_values_is_immutable_no_op(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", FLAT_RAW)
    _run(bronze_root, silver_root, quarantine_root)

    old_part = silver_root / "fx_rate" / "year=2026" / "month=09" / "part-run_1.parquet"
    before_mtime = old_part.stat().st_mtime_ns
    before_rows = common.read_parquet(old_part)

    # A second, later Bronze run re-observes the same already-published date with
    # identical values (e.g. a historical re-fetch) — must not overwrite.
    _make_bronze_run(bronze_root, "run_2", FLAT_RAW)
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["unchanged_count"] == 2
    assert result["appended_count"] == 0
    assert old_part.stat().st_mtime_ns == before_mtime
    assert common.read_parquet(old_part) == before_rows


def test_run_conflicting_existing_date_is_quarantined_not_overwritten(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", FLAT_RAW)
    _run(bronze_root, silver_root, quarantine_root)

    old_part = silver_root / "fx_rate" / "year=2026" / "month=09" / "part-run_1.parquet"
    before_rows = common.read_parquet(old_part)

    conflicting_raw = {"base": "EUR", "date": "2026-09-18", "rates": {"aud": 1.70, "usd": 1.10}}
    _make_bronze_run(bronze_root, "run_2", conflicting_raw)
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["conflict_count"] == 1
    assert result["unchanged_count"] == 1  # usd matched
    assert result["appended_count"] == 0
    assert common.read_parquet(old_part) == before_rows  # untouched

    rejected = quarantine.read_quarantine(result["quarantine_path"])
    conflict_entries = [r for r in rejected if r["failing_check_name"] == "immutability_conflict"]
    assert len(conflict_entries) == 1
    assert "AUD" in conflict_entries[0]["source_record_identifier"] or "aud" in str(conflict_entries[0]["original_raw_record"])


def test_run_raises_file_not_found_when_no_bronze_run_exists(tmp_path):
    import pytest

    bronze_root = tmp_path / "bronze"
    bronze_root.mkdir()
    with pytest.raises(FileNotFoundError):
        fx_rate.run(bronze_root=bronze_root, silver_root=tmp_path / "silver", quarantine_root=tmp_path / "quarantine")
