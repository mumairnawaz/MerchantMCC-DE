import json

from src.ingestion import watermark


def test_read_watermark_returns_none_when_no_file_exists(tmp_path):
    assert watermark.read_watermark("merchant_osm", root=tmp_path) is None


def test_write_then_read_watermark_round_trips(tmp_path):
    watermark.write_watermark("merchant_osm", "2026-09-20T08:30:16Z", run_dir="data/bronze/merchant_osm/run_x", root=tmp_path)
    assert watermark.read_watermark("merchant_osm", root=tmp_path) == "2026-09-20T08:30:16Z"


def test_write_watermark_creates_root_directory_if_missing(tmp_path):
    root = tmp_path / "does" / "not" / "exist" / "yet"
    assert not root.exists()
    watermark.write_watermark("merchant_osm", "2026-09-20T08:30:16Z", run_dir="run_x", root=root)
    assert root.exists()


def test_write_watermark_file_contents(tmp_path):
    path = watermark.write_watermark(
        "merchant_osm", "2026-09-20T08:30:16Z", run_dir="data/bronze/merchant_osm/run_x", root=tmp_path
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["source"] == "merchant_osm"
    assert payload["watermark"] == "2026-09-20T08:30:16Z"
    assert payload["last_successful_run_dir"] == "data/bronze/merchant_osm/run_x"
    assert "updated_at_utc" in payload


def test_write_watermark_overwrites_previous_value(tmp_path):
    watermark.write_watermark("merchant_osm", "2026-09-19T00:00:00Z", run_dir="run_1", root=tmp_path)
    watermark.write_watermark("merchant_osm", "2026-09-20T08:30:16Z", run_dir="run_2", root=tmp_path)
    assert watermark.read_watermark("merchant_osm", root=tmp_path) == "2026-09-20T08:30:16Z"


def test_watermarks_are_isolated_per_source(tmp_path):
    watermark.write_watermark("merchant_osm", "2026-09-20T08:30:16Z", run_dir="run_1", root=tmp_path)
    assert watermark.read_watermark("frankfurter", root=tmp_path) is None
