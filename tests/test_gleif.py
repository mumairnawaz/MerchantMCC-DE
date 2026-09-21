import json
from pathlib import Path

import pytest
import requests

from src.ingestion import gleif, watermark
from src.ingestion.settings import settings


class FakeResponse:
    def __init__(self, text, status_code=200, headers=None, raise_exc=None):
        self.text = text
        self.status_code = status_code
        self.headers = headers or {}
        self._raise_exc = raise_exc

    def raise_for_status(self):
        if self._raise_exc:
            raise self._raise_exc


def _make_record(lei, last_update="2026-09-19T09:01:35Z", jurisdiction="GB", status="ACTIVE", reg_status="ISSUED", bic=None):
    """Mirrors the real structure verified live in Steps 6B/6D."""
    return {
        "type": "lei-records",
        "id": lei,
        "attributes": {
            "lei": lei,
            "entity": {
                "legalName": {"name": f"COMPANY {lei}"},
                "legalAddress": {
                    "country": jurisdiction,
                    "city": "Bristol",
                    "region": None,
                    "postalCode": "BS2 0ZX",
                    "addressLines": ["One, Glass Wharf"],
                },
                "headquartersAddress": {
                    "country": jurisdiction,
                    "city": "Bristol",
                    "region": None,
                    "postalCode": "BS2 0ZX",
                    "addressLines": ["One, Glass Wharf"],
                },
                "legalForm": {"id": "H0PO"},
                "category": "GENERAL",
                "status": status,
                "jurisdiction": jurisdiction,
                "creationDate": "2026-05-25T00:00:00Z",
            },
            "registration": {
                "initialRegistrationDate": "2026-09-19T09:01:35Z",
                "lastUpdateDate": last_update,
                "status": reg_status,
                "nextRenewalDate": "2027-09-19T09:01:35Z",
            },
            "bic": bic,
        },
    }


def _make_page(records, current_page, last_page, total=95078, per_page=200):
    return json.dumps(
        {
            "meta": {
                "pagination": {
                    "currentPage": current_page,
                    "perPage": per_page,
                    "from": (current_page - 1) * per_page + 1,
                    "to": (current_page - 1) * per_page + len(records),
                    "total": total,
                    "lastPage": last_page,
                }
            },
            "links": {},
            "data": records,
        }
    )


def _stage_real_config_at(tmp_path: Path) -> None:
    real_config_content = gleif.CONFIG_PATH.read_text(encoding="utf-8")
    (tmp_path / "configs").mkdir(exist_ok=True)
    (tmp_path / "configs" / "sources.json").write_text(real_config_content, encoding="utf-8")


# ---- _normalize_record: pure extraction, no network ----


def test_normalize_record_extracts_exactly_the_approved_fields():
    raw = _make_record("648800S7EEVXFMHKW092")
    normalized = gleif._normalize_record(raw)
    assert set(normalized.keys()) == set(gleif.NORMALIZED_FIELDS)
    assert normalized["lei"] == "648800S7EEVXFMHKW092"
    assert normalized["entity_legal_name"] == "COMPANY 648800S7EEVXFMHKW092"
    assert normalized["entity_legal_address_country"] == "GB"
    assert normalized["entity_status"] == "ACTIVE"
    assert normalized["registration_status"] == "ISSUED"


def test_normalize_record_handles_missing_optional_subobjects():
    raw = {"attributes": {"lei": "X", "entity": {}, "registration": {}}}
    normalized = gleif._normalize_record(raw)
    assert normalized["lei"] == "X"
    assert normalized["entity_legal_name"] is None
    assert normalized["bic"] is None


def test_normalize_record_does_not_invent_fields_beyond_approved_list():
    raw = _make_record("X1")
    normalized = gleif._normalize_record(raw)
    assert len(normalized) == len(gleif.NORMALIZED_FIELDS)


# ---- _build_params: filter/sort/page construction ----


def test_build_params_initial_load_has_no_lastupdatedate_filter():
    config = gleif._load_config()
    params = gleif._build_params(config, page_number=1, incremental_watermark=None)
    assert params["filter[entity.jurisdiction]"] == "GB"
    assert params["filter[entity.status]"] == "ACTIVE"
    assert params["filter[registration.status]"] == "ISSUED"
    assert params["sort"] == "lei"
    assert params["page[size]"] == 200
    assert params["page[number]"] == 1
    assert "filter[registration.lastUpdateDate]" not in params


def test_build_params_incremental_adds_lastupdatedate_filter():
    config = gleif._load_config()
    params = gleif._build_params(config, page_number=3, incremental_watermark="2026-09-19T09:01:35Z")
    assert params["filter[registration.lastUpdateDate]"] == ">=2026-09-19T09:01:35Z"
    assert params["page[number]"] == 3
    # scope filters unchanged for incremental
    assert params["filter[entity.jurisdiction]"] == "GB"
    assert params["filter[entity.status]"] == "ACTIVE"
    assert params["filter[registration.status]"] == "ISSUED"


# ---- fetch_page: request construction, HTTP mocked ----


def test_fetch_page_sends_get_with_params_and_user_agent(monkeypatch):
    captured = {}

    def fake_get(url, params, headers, timeout):
        captured.update(url=url, params=params, headers=headers, timeout=timeout)
        return FakeResponse(_make_page([_make_record("A1")], 1, 1))

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    response = gleif.fetch_page("https://api.gleif.org/api/v1/lei-records", {"sort": "lei"}, "MyAgent/1.0", 30)

    assert captured["url"] == "https://api.gleif.org/api/v1/lei-records"
    assert captured["params"] == {"sort": "lei"}
    assert captured["headers"]["User-Agent"] == "MyAgent/1.0"
    assert captured["timeout"] == 30
    assert "Authorization" not in captured["headers"]  # no key/credential ever sent
    assert response.status_code == 200


def test_fetch_page_propagates_http_error(monkeypatch):
    def fake_get(url, params, headers, timeout):
        return FakeResponse("", status_code=500, raise_exc=requests.HTTPError("500 Server Error"))

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    with pytest.raises(requests.HTTPError):
        gleif.fetch_page("https://api.gleif.org/api/v1/lei-records", {}, "Agent/1.0", 30)


# ---- _fetch_pages: pagination logic, HTTP mocked ----


def test_initial_load_stops_at_50_pages_never_fetches_page_51(monkeypatch):
    config = gleif._load_config()
    calls = {"count": 0, "pages_requested": []}

    def fake_get(url, params, headers, timeout):
        calls["count"] += 1
        page_num = params["page[number]"]
        calls["pages_requested"].append(page_num)
        # Simulate a population far larger than the cap (real lastPage was 476/19016)
        return FakeResponse(_make_page([_make_record(f"LEI{page_num:04d}")], page_num, last_page=476))

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    pages_raw, records, statuses = gleif._fetch_pages(config, "initial", None, "Agent/1.0", 30)

    assert calls["count"] == 50
    assert max(calls["pages_requested"]) == 50
    assert 51 not in calls["pages_requested"]
    assert len(pages_raw) == 50
    assert len(statuses) == 50


def test_initial_load_stops_early_if_api_has_fewer_pages_than_cap(monkeypatch):
    config = gleif._load_config()
    calls = {"count": 0}

    def fake_get(url, params, headers, timeout):
        calls["count"] += 1
        page_num = params["page[number]"]
        return FakeResponse(_make_page([_make_record(f"LEI{page_num}")], page_num, last_page=3))

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    pages_raw, records, statuses = gleif._fetch_pages(config, "initial", None, "Agent/1.0", 30)

    assert calls["count"] == 3  # never over-fetches beyond what the API actually has
    assert len(pages_raw) == 3


def test_incremental_load_not_subject_to_50_page_cap(monkeypatch):
    config = gleif._load_config()
    calls = {"count": 0}

    def fake_get(url, params, headers, timeout):
        calls["count"] += 1
        page_num = params["page[number]"]
        # 60 pages -- more than the initial-load cap of 50 -- proves the cap does not apply
        return FakeResponse(_make_page([_make_record(f"LEI{page_num}")], page_num, last_page=60))

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    pages_raw, records, statuses = gleif._fetch_pages(config, "incremental", "2026-09-19T00:00:00Z", "Agent/1.0", 30)

    assert calls["count"] == 60
    assert len(pages_raw) == 60


def test_incremental_sends_watermark_filter_on_every_page(monkeypatch):
    config = gleif._load_config()
    captured_params = []

    def fake_get(url, params, headers, timeout):
        captured_params.append(dict(params))
        page_num = params["page[number]"]
        return FakeResponse(_make_page([_make_record(f"LEI{page_num}")], page_num, last_page=2))

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    gleif._fetch_pages(config, "incremental", "2026-09-19T09:01:35Z", "Agent/1.0", 30)

    assert all(p["filter[registration.lastUpdateDate]"] == ">=2026-09-19T09:01:35Z" for p in captured_params)


def test_initial_load_never_sends_lastupdatedate_filter(monkeypatch):
    config = gleif._load_config()
    captured_params = []

    def fake_get(url, params, headers, timeout):
        captured_params.append(dict(params))
        page_num = params["page[number]"]
        return FakeResponse(_make_page([_make_record(f"LEI{page_num}")], page_num, last_page=2))

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    gleif._fetch_pages(config, "initial", None, "Agent/1.0", 30)

    assert all("filter[registration.lastUpdateDate]" not in p for p in captured_params)


def test_fetch_pages_raises_immediately_on_mid_pagination_http_failure(monkeypatch):
    config = gleif._load_config()
    call_count = {"n": 0}

    def fake_get(url, params, headers, timeout):
        call_count["n"] += 1
        page_num = params["page[number]"]
        if page_num == 3:
            return FakeResponse("", status_code=503, raise_exc=requests.HTTPError("503 Service Unavailable"))
        return FakeResponse(_make_page([_make_record(f"LEI{page_num}")], page_num, last_page=10))

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    with pytest.raises(requests.HTTPError):
        gleif._fetch_pages(config, "initial", None, "Agent/1.0", 30)

    assert call_count["n"] == 3  # stopped exactly at the failing page, no retries attempted


# ---- validation ----


def test_validate_initial_empty_result_fails():
    result = gleif._validate([], [_make_page([], 1, 1)], "initial")
    assert result["passed"] is False


def test_validate_incremental_empty_result_is_valid():
    result = gleif._validate([], [_make_page([], 1, 1)], "incremental")
    assert result["passed"] is True


def test_validate_detects_duplicate_lei():
    records = [gleif._normalize_record(_make_record("DUP1")), gleif._normalize_record(_make_record("DUP1"))]
    result = gleif._validate(records, [_make_page([], 1, 1)], "initial")
    assert result["passed"] is False
    dup_check = next(c for c in result["checks"] if c["name"] == "duplicate_key_check")
    assert dup_check["details"]["duplicate_count"] == 1


def test_validate_fails_on_malformed_page_shape():
    malformed_page = json.dumps({"meta": {}, "data": "not-a-list"})
    result = gleif._validate([], [malformed_page], "initial")
    assert result["passed"] is False


def test_validate_passes_for_well_formed_non_duplicate_records():
    records = [gleif._normalize_record(_make_record("A1")), gleif._normalize_record(_make_record("A2"))]
    page = _make_page([_make_record("A1"), _make_record("A2")], 1, 1)
    result = gleif._validate(records, [page], "initial")
    assert result["passed"] is True


# ---- _max_last_update_date ----


def test_max_last_update_date_picks_the_true_maximum():
    records = [
        {"registration_last_update_date": "2026-09-18T00:00:00Z"},
        {"registration_last_update_date": "2026-09-20T00:00:00Z"},
        {"registration_last_update_date": "2026-09-19T00:00:00Z"},
    ]
    assert gleif._max_last_update_date(records) == "2026-09-20T00:00:00Z"


def test_max_last_update_date_empty_records_returns_none():
    assert gleif._max_last_update_date([]) is None


# ---- run(): full integration, HTTP mocked, Bronze/watermark isolated to tmp_path ----


def test_run_initial_load_writes_50_bronze_pages_and_metadata(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)

    def fake_get(url, params, headers, timeout):
        page_num = params["page[number]"]
        return FakeResponse(_make_page([_make_record(f"LEI{page_num:04d}")], page_num, last_page=476))

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    run_dir = gleif.run()

    page_files = sorted(run_dir.glob("page_*.json"))
    assert len(page_files) == 50
    assert (run_dir / "page_0001.json").exists()
    assert (run_dir / "page_0050.json").exists()
    assert not (run_dir / "page_0051.json").exists()

    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["record_count"] == 50
    assert metadata["page_count"] == 50
    assert metadata["extraction_mode"] == "initial"
    assert metadata["initial_cap_applied"] is True
    assert metadata["initial_record_cap_configured"] == 10000
    assert metadata["sort_used"] == "lei"
    assert "10,000-record" in metadata["sampling_scope_note"]
    assert "not statistically" in metadata["sampling_limitation"].lower() or "NOT statistically" in metadata["sampling_limitation"]
    assert metadata["filters_used"]["entity.jurisdiction"] == "GB"
    assert metadata["filters_used"]["entity.status"] == "ACTIVE"
    assert metadata["filters_used"]["registration.status"] == "ISSUED"
    assert metadata["filters_used"]["registration.lastUpdateDate"] is None
    assert metadata["user_agent_used"] == settings.user_agent


def test_run_bronze_pages_preserve_raw_json_verbatim(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    page1_content = _make_page([_make_record("VERBATIM1")], 1, 2)
    page2_content = _make_page([_make_record("VERBATIM2")], 2, 2)
    responses = {1: page1_content, 2: page2_content}

    def fake_get(url, params, headers, timeout):
        return FakeResponse(responses[params["page[number]"]])

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    run_dir = gleif.run()

    assert (run_dir / "page_0001.json").read_text(encoding="utf-8") == page1_content
    assert (run_dir / "page_0002.json").read_text(encoding="utf-8") == page2_content


def test_run_never_overwrites_previous_bronze_run(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    import time

    def fake_get(url, params, headers, timeout):
        page_num = params["page[number]"]
        return FakeResponse(_make_page([_make_record(f"LEI{page_num}")], page_num, last_page=1))

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    first_run_dir = gleif.run()
    first_content = (first_run_dir / "page_0001.json").read_text(encoding="utf-8")

    time.sleep(1.1)  # avoid same-second run-dir collision, per Step 4 report item P

    def fake_get_incremental(url, params, headers, timeout):
        page_num = params["page[number]"]
        return FakeResponse(_make_page([_make_record(f"LEI-INC-{page_num}")], page_num, last_page=1))

    monkeypatch.setattr(gleif.requests, "get", fake_get_incremental)
    second_run_dir = gleif.run()

    assert second_run_dir != first_run_dir
    assert first_run_dir.exists()
    assert (first_run_dir / "page_0001.json").read_text(encoding="utf-8") == first_content


def test_run_incremental_used_when_watermark_exists_and_ignores_cap(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("gleif", "2026-09-19T00:00:00Z", run_dir="seed_run")

    def fake_get(url, params, headers, timeout):
        page_num = params["page[number]"]
        assert params["filter[registration.lastUpdateDate]"] == ">=2026-09-19T00:00:00Z"
        # 60 pages -- proves the incremental run is not capped at 50
        return FakeResponse(_make_page([_make_record(f"LEI{page_num}")], page_num, last_page=60))

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    run_dir = gleif.run()

    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["extraction_mode"] == "incremental"
    assert metadata["page_count"] == 60  # exceeds the 50-page initial cap
    assert metadata["initial_cap_applied"] is False
    assert metadata["previous_watermark"] == "2026-09-19T00:00:00Z"


def test_run_watermark_advances_after_successful_initial_load(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)

    def fake_get(url, params, headers, timeout):
        page_num = params["page[number]"]
        return FakeResponse(
            _make_page([_make_record(f"LEI{page_num}", last_update="2026-09-19T09:01:35Z")], page_num, last_page=1)
        )

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    assert watermark.read_watermark("gleif") is None
    gleif.run()
    assert watermark.read_watermark("gleif") == "2026-09-19T09:01:35Z"


def test_run_watermark_advances_to_true_max_after_incremental_load(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("gleif", "2026-09-18T00:00:00Z", run_dir="seed_run")

    records_by_page = {
        1: [_make_record("A1", last_update="2026-09-19T10:00:00Z")],
        2: [_make_record("A2", last_update="2026-09-20T15:30:00Z")],  # true max
    }

    def fake_get(url, params, headers, timeout):
        page_num = params["page[number]"]
        return FakeResponse(_make_page(records_by_page[page_num], page_num, last_page=2))

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    gleif.run()
    assert watermark.read_watermark("gleif") == "2026-09-20T15:30:00Z"


def test_run_empty_incremental_result_is_valid_watermark_held(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("gleif", "2026-09-19T00:00:00Z", run_dir="seed_run")

    def fake_get(url, params, headers, timeout):
        return FakeResponse(_make_page([], 1, last_page=1))

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    run_dir = gleif.run()
    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))

    assert metadata["record_count"] == 0
    assert metadata["validation"]["passed"] is True
    assert metadata["watermark_updated"] is False
    assert watermark.read_watermark("gleif") == "2026-09-19T00:00:00Z"  # unchanged


def test_run_watermark_not_advanced_on_http_failure_and_no_bronze_written(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("gleif", "2026-09-19T00:00:00Z", run_dir="seed_run")

    def fake_get(url, params, headers, timeout):
        return FakeResponse("", status_code=500, raise_exc=requests.HTTPError("500 Server Error"))

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    with pytest.raises(requests.HTTPError):
        gleif.run()

    assert watermark.read_watermark("gleif") == "2026-09-19T00:00:00Z"
    assert not (tmp_path / "data" / "bronze" / "gleif").exists()


def test_run_watermark_not_advanced_on_validation_failure_but_bronze_still_written(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("gleif", "2026-09-19T00:00:00Z", run_dir="seed_run")

    # Same LEI appears on two different pages -> duplicate -> validation fails
    def fake_get(url, params, headers, timeout):
        page_num = params["page[number]"]
        return FakeResponse(_make_page([_make_record("DUPLICATE_LEI")], page_num, last_page=2))

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    run_dir = gleif.run()  # does not raise -- Bronze still written honestly

    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["validation"]["passed"] is False
    assert metadata["watermark_updated"] is False
    assert watermark.read_watermark("gleif") == "2026-09-19T00:00:00Z"  # unchanged
    assert (run_dir / "page_0001.json").exists()  # Bronze preserved despite validation failure


def test_run_malformed_json_raises_cleanly_and_writes_no_bronze(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)

    def fake_get(url, params, headers, timeout):
        return FakeResponse("{not valid json")

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    with pytest.raises(json.JSONDecodeError):
        gleif.run()

    assert not (tmp_path / "data" / "bronze" / "gleif").exists()
    assert watermark.read_watermark("gleif") is None


def test_run_sends_centralized_user_agent_and_configured_timeout(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    captured = {}

    def fake_get(url, params, headers, timeout):
        captured["headers"] = headers
        captured["timeout"] = timeout
        return FakeResponse(_make_page([_make_record("A1")], 1, 1))

    monkeypatch.setattr(gleif.requests, "get", fake_get)
    gleif.run()

    assert captured["headers"]["User-Agent"] == settings.user_agent
    assert "Authorization" not in captured["headers"]
    config = gleif._load_config()
    expected_timeout = config.get("request_timeout_seconds", settings.request_timeout_seconds)
    assert captured["timeout"] == expected_timeout
