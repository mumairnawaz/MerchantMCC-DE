import json
from pathlib import Path

import pytest

from src.silver import common


def _make_bronze_run(bronze_root: Path, source: str, run_id: str, metadata: dict, raw_filename: str = "data.json", raw_content: str = "{}") -> Path:
    run_dir = bronze_root / source / run_id
    run_dir.mkdir(parents=True)
    (run_dir / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    (run_dir / raw_filename).write_text(raw_content, encoding="utf-8")
    return run_dir


# ---- Bronze run discovery ----


def test_list_bronze_runs_returns_empty_for_unknown_source(tmp_path):
    assert common.list_bronze_runs("nonexistent", bronze_root=tmp_path) == []


def test_list_bronze_runs_sorted_oldest_first(tmp_path):
    _make_bronze_run(tmp_path, "mcc", "run_20260101T000000Z", {"record_count": 1})
    _make_bronze_run(tmp_path, "mcc", "run_20260301T000000Z", {"record_count": 2})
    _make_bronze_run(tmp_path, "mcc", "run_20260201T000000Z", {"record_count": 3})

    runs = common.list_bronze_runs("mcc", bronze_root=tmp_path)

    assert [r.name for r in runs] == ["run_20260101T000000Z", "run_20260201T000000Z", "run_20260301T000000Z"]


def test_latest_bronze_run_returns_none_when_no_runs_exist(tmp_path):
    assert common.latest_bronze_run("mcc", bronze_root=tmp_path) is None


def test_latest_bronze_run_returns_most_recent(tmp_path):
    _make_bronze_run(tmp_path, "mcc", "run_20260101T000000Z", {})
    latest = _make_bronze_run(tmp_path, "mcc", "run_20260301T000000Z", {})
    _make_bronze_run(tmp_path, "mcc", "run_20260201T000000Z", {})

    assert common.latest_bronze_run("mcc", bronze_root=tmp_path) == latest


def test_bronze_run_id_is_the_directory_name(tmp_path):
    run_dir = _make_bronze_run(tmp_path, "mcc", "run_20260921T054417Z", {})
    assert common.bronze_run_id(run_dir) == "run_20260921T054417Z"


# ---- Bronze reading, including malformed/missing input ----


def test_read_bronze_metadata_success(tmp_path):
    run_dir = _make_bronze_run(tmp_path, "mcc", "run_1", {"record_count": 42, "ingestion_timestamp_utc": "2026-09-21T00:00:00Z"})
    metadata = common.read_bronze_metadata(run_dir)
    assert metadata["record_count"] == 42


def test_read_bronze_metadata_missing_file_raises(tmp_path):
    empty_dir = tmp_path / "empty_run"
    empty_dir.mkdir()
    with pytest.raises(FileNotFoundError):
        common.read_bronze_metadata(empty_dir)


def test_read_bronze_json_success(tmp_path):
    path = tmp_path / "data.json"
    path.write_text('{"a": 1}', encoding="utf-8")
    assert common.read_bronze_json(path) == {"a": 1}


def test_read_bronze_json_malformed_raises(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("{not valid json", encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        common.read_bronze_json(path)


def test_read_bronze_json_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        common.read_bronze_json(tmp_path / "does_not_exist.json")


def test_read_bronze_csv_success(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    records = common.read_bronze_csv(path)
    assert records == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]


def test_read_bronze_csv_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        common.read_bronze_csv(tmp_path / "does_not_exist.csv")


def test_read_bronze_csv_ragged_rows_does_not_raise(tmp_path):
    path = tmp_path / "ragged.csv"
    path.write_text("a,b,c\n1,2\n", encoding="utf-8")
    records = common.read_bronze_csv(path)
    assert isinstance(records, list)


# ---- Lineage ----


def test_build_lineage_contains_exactly_the_five_standard_fields(tmp_path):
    run_dir = _make_bronze_run(tmp_path, "mcc", "run_20260921T000000Z", {})
    lineage = common.build_lineage(
        source_name="mcc", run_dir=run_dir, ingestion_timestamp_utc="2026-09-21T00:00:00Z", transform_version="v1"
    )
    assert set(lineage.keys()) == set(common.STANDARD_LINEAGE_FIELDS)
    assert lineage["source_name"] == "mcc"
    assert lineage["bronze_run_id"] == "run_20260921T000000Z"
    assert lineage["ingestion_timestamp_utc"] == "2026-09-21T00:00:00Z"
    assert lineage["silver_transform_version"] == "v1"
    assert lineage["silver_processed_at_utc"]  # non-empty, generated


def test_build_lineage_processed_at_is_a_real_utc_timestamp(tmp_path):
    run_dir = _make_bronze_run(tmp_path, "mcc", "run_1", {})
    lineage = common.build_lineage(source_name="mcc", run_dir=run_dir, ingestion_timestamp_utc="2026-09-21T00:00:00Z", transform_version="v1")
    assert lineage["silver_processed_at_utc"].endswith("Z")


# ---- record hash ----


def test_compute_record_hash_deterministic_for_same_values():
    record = {"a": 1, "b": "x"}
    assert common.compute_record_hash(record, ["a", "b"]) == common.compute_record_hash(record, ["a", "b"])


def test_compute_record_hash_ignores_key_order():
    r1 = {"a": 1, "b": 2}
    r2 = {"b": 2, "a": 1}
    assert common.compute_record_hash(r1, ["a", "b"]) == common.compute_record_hash(r2, ["a", "b"])


def test_compute_record_hash_differs_for_different_values():
    r1 = {"a": 1}
    r2 = {"a": 2}
    assert common.compute_record_hash(r1, ["a"]) != common.compute_record_hash(r2, ["a"])


def test_compute_record_hash_only_considers_specified_fields():
    r1 = {"a": 1, "irrelevant": "x"}
    r2 = {"a": 1, "irrelevant": "y"}
    assert common.compute_record_hash(r1, ["a"]) == common.compute_record_hash(r2, ["a"])


# ---- merge_upsert ----


def test_merge_upsert_adds_new_key():
    existing = [{"id": 1, "ts": "2026-01-01"}]
    incoming = [{"id": 2, "ts": "2026-01-02"}]
    merged = common.merge_upsert(existing, incoming, key_field="id", compare_field="ts")
    assert {r["id"] for r in merged} == {1, 2}


def test_merge_upsert_replaces_when_incoming_is_newer():
    existing = [{"id": 1, "ts": "2026-01-01", "v": "old"}]
    incoming = [{"id": 1, "ts": "2026-01-02", "v": "new"}]
    merged = common.merge_upsert(existing, incoming, key_field="id", compare_field="ts")
    assert merged == [{"id": 1, "ts": "2026-01-02", "v": "new"}]


def test_merge_upsert_keeps_existing_when_incoming_is_older():
    existing = [{"id": 1, "ts": "2026-01-05", "v": "current"}]
    incoming = [{"id": 1, "ts": "2026-01-01", "v": "stale"}]
    merged = common.merge_upsert(existing, incoming, key_field="id", compare_field="ts")
    assert merged == [{"id": 1, "ts": "2026-01-05", "v": "current"}]


# ---- Parquet output ----


def test_write_and_read_parquet_round_trip(tmp_path):
    records = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    path = tmp_path / "out" / "data.parquet"
    common.write_parquet(records, path)
    assert path.exists()
    read_back = common.read_parquet(path)
    assert read_back == records


def test_write_parquet_creates_parent_directories(tmp_path):
    path = tmp_path / "deeply" / "nested" / "dir" / "data.parquet"
    common.write_parquet([{"a": 1}], path)
    assert path.exists()


def test_write_parquet_empty_records_without_schema_raises(tmp_path):
    with pytest.raises(ValueError):
        common.write_parquet([], tmp_path / "empty.parquet")


def test_write_parquet_empty_records_with_schema_succeeds(tmp_path):
    import pyarrow as pa

    schema = pa.schema([("a", pa.int64())])
    path = common.write_parquet([], tmp_path / "empty.parquet", schema=schema)
    assert path.exists()
    assert common.read_parquet(path) == []


def test_write_parquet_is_deterministic_on_rewrite(tmp_path):
    records = [{"a": 1, "b": "x"}]
    path = tmp_path / "data.parquet"
    common.write_parquet(records, path)
    first_read = common.read_parquet(path)
    common.write_parquet(records, path)
    second_read = common.read_parquet(path)
    assert first_read == second_read == records
