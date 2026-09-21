import json
import time
from pathlib import Path

import pytest
import requests

from src.ingestion import merchant_osm, watermark
from src.ingestion.settings import settings

SAMPLE_JSON = json.dumps(
    {
        "version": 0.6,
        "generator": "Overpass API 0.7.62.11",
        "osm3s": {
            "timestamp_osm_base": "2026-09-20T08:30:16Z",
            "copyright": "The data included in this document is from www.openstreetmap.org.",
        },
        "elements": [
            {"type": "node", "id": 1, "lat": 53.8, "lon": -1.54, "tags": {"shop": "bakery", "name": "Test Bakery"}},
            {"type": "node", "id": 2, "lat": 53.8, "lon": -1.55, "tags": {"amenity": "cafe"}},
        ],
    }
)


class FakeResponse:
    def __init__(self, text, status_code=200, headers=None, raise_exc=None):
        self.text = text
        self.status_code = status_code
        self.headers = headers or {}
        self._raise_exc = raise_exc

    def raise_for_status(self):
        if self._raise_exc:
            raise self._raise_exc


def _stage_real_config_at(tmp_path: Path) -> None:
    """Copy the real configs/sources.json content (read before any chdir) into an
    isolated temp project layout, so _load_config() still finds it after the test
    chdir's into tmp_path for Bronze-path isolation.
    """
    real_config_content = merchant_osm.CONFIG_PATH.read_text(encoding="utf-8")
    (tmp_path / "configs").mkdir(exist_ok=True)
    (tmp_path / "configs" / "sources.json").write_text(real_config_content, encoding="utf-8")


# ---- parse_for_metadata: pure parsing, no network ----


def test_parse_for_metadata_returns_elements():
    records, fields = merchant_osm.parse_for_metadata(SAMPLE_JSON)
    assert len(records) == 2
    assert "tags" in fields
    assert "id" in fields


def test_parse_for_metadata_handles_missing_elements_key():
    records, fields = merchant_osm.parse_for_metadata(json.dumps({"version": 0.6}))
    assert records == []
    assert fields == []


def test_parse_for_metadata_handles_empty_elements_list():
    records, fields = merchant_osm.parse_for_metadata(json.dumps({"elements": []}))
    assert records == []
    assert fields == []


def test_parse_for_metadata_malformed_json_raises():
    with pytest.raises(json.JSONDecodeError):
        merchant_osm.parse_for_metadata("{not valid json")


def test_parse_for_metadata_does_not_require_optional_tags():
    # Real OSM records frequently omit optional tags (phone, website, addr:*).
    # Missing optional attributes must not be treated as malformed input.
    sparse = json.dumps({"elements": [{"type": "node", "id": 99, "lat": 1.0, "lon": 1.0, "tags": {"shop": "kiosk"}}]})
    records, _fields = merchant_osm.parse_for_metadata(sparse)
    assert len(records) == 1
    assert "phone" not in records[0]["tags"]


# ---- check_response_shape: structural validation ----


def test_check_response_shape_passes_for_valid_response():
    assert merchant_osm.check_response_shape(SAMPLE_JSON)["passed"] is True


def test_check_response_shape_fails_when_elements_missing():
    result = merchant_osm.check_response_shape(json.dumps({"version": 0.6}))
    assert result["passed"] is False


def test_check_response_shape_fails_when_elements_not_a_list():
    result = merchant_osm.check_response_shape(json.dumps({"elements": "not-a-list"}))
    assert result["passed"] is False


def test_check_response_shape_passes_for_empty_but_valid_elements_list():
    result = merchant_osm.check_response_shape(json.dumps({"elements": []}))
    assert result["passed"] is True


# ---- fetch_raw: request construction, all HTTP mocked ----


def test_fetch_raw_posts_to_correct_endpoint_with_query_and_user_agent(monkeypatch):
    captured = {}

    def fake_post(url, data, headers, timeout):
        captured.update(url=url, data=data, headers=headers, timeout=timeout)
        return FakeResponse(SAMPLE_JSON)

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)

    response = merchant_osm.fetch_raw("https://overpass-api.de/api/interpreter", "some query", "MyAgent/1.0", 60)

    assert captured["url"] == "https://overpass-api.de/api/interpreter"
    assert captured["data"] == {"data": "some query"}
    assert captured["headers"]["User-Agent"] == "MyAgent/1.0"
    assert captured["timeout"] == 60
    assert response.text == SAMPLE_JSON


def test_fetch_raw_propagates_http_error(monkeypatch):
    def fake_post(url, data, headers, timeout):
        return FakeResponse("", status_code=406, raise_exc=requests.HTTPError("406 Client Error"))

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)

    with pytest.raises(requests.HTTPError):
        merchant_osm.fetch_raw("https://overpass-api.de/api/interpreter", "query", "Agent/1.0", 60)


# ---- run(): full integration, HTTP mocked, Bronze isolated to tmp_path ----


def test_run_writes_bronze_and_metadata_with_mocked_response(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)

    def fake_post(url, data, headers, timeout):
        return FakeResponse(
            SAMPLE_JSON,
            status_code=200,
            headers={"Content-Length": str(len(SAMPLE_JSON)), "ETag": 'W/"abc123"'},
        )

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)

    run_dir = merchant_osm.run()

    raw_file = run_dir / merchant_osm.RAW_FILENAME
    metadata_file = run_dir / "metadata.json"
    assert raw_file.exists()
    assert metadata_file.exists()
    assert raw_file.read_text(encoding="utf-8") == SAMPLE_JSON  # Bronze preserves the raw response verbatim

    metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
    assert metadata["source_name"] == "merchant_osm"
    assert metadata["record_count"] == 2
    assert metadata["validation"]["passed"] is True
    assert metadata["source_type"] == "live_api"
    assert metadata["extraction_mode"] == "full"
    assert metadata["user_agent_used"] == settings.user_agent
    assert metadata["osm_dataset_timestamp"] == "2026-09-20T08:30:16Z"
    assert metadata["http_headers"]["ETag"] == 'W/"abc123"'


def test_run_sends_centralized_user_agent_not_a_hardcoded_string(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    captured = {}

    def fake_post(url, data, headers, timeout):
        captured["headers"] = headers
        return FakeResponse(SAMPLE_JSON)

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)

    merchant_osm.run()

    assert captured["headers"]["User-Agent"] == settings.user_agent
    assert "FinPay" in settings.user_agent  # sanity: confirms it's the centralized settings value


def test_run_handles_empty_elements_without_crashing(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    empty_response = json.dumps({"version": 0.6, "osm3s": {}, "elements": []})

    def fake_post(url, data, headers, timeout):
        return FakeResponse(empty_response)

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)

    run_dir = merchant_osm.run()
    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))

    assert metadata["record_count"] == 0
    # Structurally valid but empty is not a crash — non_empty correctly fails validation,
    # and Bronze still gets written as an honest record of what the source returned.
    assert metadata["validation"]["passed"] is False
    checks_by_name = {c["name"]: c for c in metadata["validation"]["checks"]}
    assert checks_by_name["key_is_list_elements"]["passed"] is True
    assert checks_by_name["non_empty"]["passed"] is False


def test_run_propagates_http_error_and_writes_no_bronze(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)

    def fake_post(url, data, headers, timeout):
        return FakeResponse("", status_code=406, raise_exc=requests.HTTPError("406 Client Error: Not Acceptable"))

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)

    with pytest.raises(requests.HTTPError):
        merchant_osm.run()

    assert not (tmp_path / "data" / "bronze" / "merchant_osm").exists()


def test_run_uses_configured_timeout_not_hardcoded(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    captured = {}

    def fake_post(url, data, headers, timeout):
        captured["timeout"] = timeout
        return FakeResponse(SAMPLE_JSON)

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)

    merchant_osm.run()

    config = merchant_osm._load_config()
    assert captured["timeout"] == config["request_timeout_seconds"]


# ==========================================================================
# STEP 4 — incremental ingestion with watermark
# ==========================================================================


def test_build_incremental_query_embeds_watermark_unmodified():
    config = merchant_osm._load_config()
    query = merchant_osm._build_incremental_query(config, "2026-09-20T08:30:16Z")
    assert 'newer:"2026-09-20T08:30:16Z"' in query
    # geographic/business scope unchanged: exact same bbox and filters as the full query
    assert "53.7965,-1.5486,53.8010,-1.5390" in query
    assert '"shop"' in query
    assert "restaurant|cafe|fast_food|bank|pharmacy" in query


def test_build_incremental_query_missing_template_raises():
    config = {"url": "https://overpass-api.de/api/interpreter"}  # no incremental_query_template
    with pytest.raises(KeyError):
        merchant_osm._build_incremental_query(config, "2026-09-20T08:30:16Z")


def test_full_load_used_when_no_previous_watermark(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    captured = {}

    def fake_post(url, data, headers, timeout):
        captured["query"] = data["data"]
        return FakeResponse(SAMPLE_JSON)

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)

    config = merchant_osm._load_config()
    run_dir = merchant_osm.run()

    assert captured["query"] == config["query"]  # the full-extraction query, unmodified
    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["extraction_mode"] == "full"
    assert metadata["previous_watermark"] is None


def test_incremental_load_used_when_watermark_exists(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("merchant_osm", "2026-09-19T00:00:00Z", run_dir="seed_run")
    captured = {}

    def fake_post(url, data, headers, timeout):
        captured["query"] = data["data"]
        captured["timeout"] = timeout
        return FakeResponse(SAMPLE_JSON)

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)

    config = merchant_osm._load_config()
    run_dir = merchant_osm.run()

    assert 'newer:"2026-09-19T00:00:00Z"' in captured["query"]
    assert captured["timeout"] == config["incremental_request_timeout_seconds"]
    assert "53.7965,-1.5486,53.8010,-1.5390" in captured["query"]  # scope unchanged
    assert '"shop"' in captured["query"]
    assert "restaurant|cafe|fast_food|bank|pharmacy" in captured["query"]

    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["extraction_mode"] == "incremental"
    assert metadata["previous_watermark"] == "2026-09-19T00:00:00Z"


def test_incremental_uses_centralized_user_agent(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("merchant_osm", "2026-09-19T00:00:00Z", run_dir="seed_run")
    captured = {}

    def fake_post(url, data, headers, timeout):
        captured["headers"] = headers
        return FakeResponse(SAMPLE_JSON)

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)
    merchant_osm.run()

    assert captured["headers"]["User-Agent"] == settings.user_agent


def test_incremental_creates_new_bronze_run_and_preserves_previous(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)

    def fake_post(url, data, headers, timeout):
        return FakeResponse(SAMPLE_JSON)

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)

    first_run_dir = merchant_osm.run()  # full load, sets the watermark
    first_raw_content = (first_run_dir / merchant_osm.RAW_FILENAME).read_text(encoding="utf-8")

    # common.new_run_dir() has second-level timestamp precision — a real, separate
    # limitation this step surfaced (see STEP 4 report, item P). Sleeping past the
    # second boundary here avoids a directory-name collision; not a fix to that
    # limitation, which is out of this step's scope.
    time.sleep(1.1)

    second_run_dir = merchant_osm.run()  # incremental load, watermark now present

    assert second_run_dir != first_run_dir
    assert first_run_dir.exists()
    assert (first_run_dir / merchant_osm.RAW_FILENAME).read_text(encoding="utf-8") == first_raw_content
    assert (second_run_dir / merchant_osm.RAW_FILENAME).exists()


def test_watermark_advances_after_successful_full_load(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)

    def fake_post(url, data, headers, timeout):
        return FakeResponse(SAMPLE_JSON)

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)

    assert watermark.read_watermark("merchant_osm") is None
    merchant_osm.run()
    assert watermark.read_watermark("merchant_osm") == "2026-09-20T08:30:16Z"


def test_watermark_advances_after_successful_incremental_load(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("merchant_osm", "2026-09-19T00:00:00Z", run_dir="seed_run")

    later_response = json.dumps(
        {
            "version": 0.6,
            "osm3s": {"timestamp_osm_base": "2026-09-20T10:00:00Z"},
            "elements": [{"type": "node", "id": 5, "tags": {"shop": "newsagent"}}],
        }
    )

    def fake_post(url, data, headers, timeout):
        return FakeResponse(later_response)

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)
    merchant_osm.run()

    assert watermark.read_watermark("merchant_osm") == "2026-09-20T10:00:00Z"


def test_watermark_not_advanced_when_http_request_fails(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("merchant_osm", "2026-09-19T00:00:00Z", run_dir="seed_run")

    def fake_post(url, data, headers, timeout):
        return FakeResponse("", status_code=406, raise_exc=requests.HTTPError("406 Client Error"))

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)

    with pytest.raises(requests.HTTPError):
        merchant_osm.run()

    assert watermark.read_watermark("merchant_osm") == "2026-09-19T00:00:00Z"  # unchanged


def test_watermark_not_advanced_when_validation_fails(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("merchant_osm", "2026-09-19T00:00:00Z", run_dir="seed_run")

    # Structurally invalid: 'elements' missing entirely -> check_response_shape fails
    malformed_shape_response = json.dumps({"version": 0.6, "osm3s": {"timestamp_osm_base": "2026-09-20T10:00:00Z"}})

    def fake_post(url, data, headers, timeout):
        return FakeResponse(malformed_shape_response)

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)

    run_dir = merchant_osm.run()  # does not raise — Bronze still written as an honest record

    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["validation"]["passed"] is False
    assert metadata["watermark_updated"] is False
    assert watermark.read_watermark("merchant_osm") == "2026-09-19T00:00:00Z"  # unchanged


def test_incremental_empty_result_is_valid_no_change_run(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("merchant_osm", "2026-09-19T00:00:00Z", run_dir="seed_run")

    no_change_response = json.dumps(
        {"version": 0.6, "osm3s": {"timestamp_osm_base": "2026-09-20T10:00:00Z"}, "elements": []}
    )

    def fake_post(url, data, headers, timeout):
        return FakeResponse(no_change_response)

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)

    run_dir = merchant_osm.run()
    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))

    assert metadata["record_count"] == 0
    assert metadata["validation"]["passed"] is True  # empty is valid for incremental, not a failure
    assert metadata["watermark_updated"] is True
    assert watermark.read_watermark("merchant_osm") == "2026-09-20T10:00:00Z"


def test_incremental_duplicate_ids_handled_by_existing_validation(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("merchant_osm", "2026-09-19T00:00:00Z", run_dir="seed_run")

    dup_response = json.dumps(
        {
            "version": 0.6,
            "osm3s": {"timestamp_osm_base": "2026-09-20T10:00:00Z"},
            "elements": [
                {"type": "node", "id": 7, "tags": {"shop": "bakery"}},
                {"type": "node", "id": 7, "tags": {"shop": "bakery"}},
            ],
        }
    )

    def fake_post(url, data, headers, timeout):
        return FakeResponse(dup_response)

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)
    run_dir = merchant_osm.run()
    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))

    checks_by_name = {c["name"]: c for c in metadata["validation"]["checks"]}
    assert checks_by_name["duplicate_key_check"]["passed"] is False
    assert checks_by_name["duplicate_key_check"]["details"]["duplicate_count"] == 1
    assert metadata["watermark_updated"] is False  # duplicate elements fail validation -> watermark held back


def test_incremental_malformed_json_raises_cleanly(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("merchant_osm", "2026-09-19T00:00:00Z", run_dir="seed_run")

    def fake_post(url, data, headers, timeout):
        return FakeResponse("{not valid json")

    monkeypatch.setattr(merchant_osm.requests, "post", fake_post)

    with pytest.raises(json.JSONDecodeError):
        merchant_osm.run()

    assert not (tmp_path / "data" / "bronze" / "merchant_osm").exists()
    assert watermark.read_watermark("merchant_osm") == "2026-09-19T00:00:00Z"
