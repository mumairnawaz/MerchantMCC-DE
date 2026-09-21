import json

from src.ingestion import common


def test_new_run_dir_creates_unique_timestamped_dir(tmp_path):
    run_dir = common.new_run_dir("test_source", bronze_root=tmp_path)
    assert run_dir.exists()
    assert run_dir.parent.name == "test_source"
    assert run_dir.name.startswith("run_")


def test_write_raw_preserves_content_verbatim(tmp_path):
    run_dir = common.new_run_dir("test_source", bronze_root=tmp_path)
    content = "a,b\n1,2\n"
    path = common.write_raw(run_dir, "data.csv", content)
    assert path.read_text(encoding="utf-8") == content


def test_write_metadata_contains_expected_keys(tmp_path):
    run_dir = common.new_run_dir("test_source", bronze_root=tmp_path)
    validation_result = {"passed": True, "checks": []}
    path = common.write_metadata(
        run_dir,
        source_name="test_source",
        source_url="https://example.com",
        raw_filename="data.csv",
        record_count=2,
        discovered_fields=["a", "b"],
        validation_result=validation_result,
        http_status=200,
    )
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["source_name"] == "test_source"
    assert data["record_count"] == 2
    assert data["validation"]["passed"] is True
    assert "ingestion_timestamp_utc" in data


def test_write_metadata_merges_extra_fields(tmp_path):
    run_dir = common.new_run_dir("test_source", bronze_root=tmp_path)
    path = common.write_metadata(
        run_dir,
        source_name="test_source",
        source_url="https://example.com",
        raw_filename="data.csv",
        record_count=0,
        discovered_fields=[],
        validation_result={"passed": True, "checks": []},
        extra={"area_name": "Test Area"},
    )
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["area_name"] == "Test Area"
