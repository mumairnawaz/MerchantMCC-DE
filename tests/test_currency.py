import json
from pathlib import Path

import pytest
import requests

from src.ingestion import currency, watermark
from src.ingestion.settings import settings

# Realistic multi-currency, multi-date range response (mirrors the real shape
# verified live during earlier research: Frankfurter clips to dates it actually has).
RANGE_JSON = json.dumps(
    {
        "amount": 1.0,
        "base": "EUR",
        "start_date": "2026-09-17",
        "end_date": "2026-09-18",
        "rates": {
            "2026-09-17": {"USD": 1.148, "GBP": 0.8583, "JPY": 178.75},
            "2026-09-18": {"USD": 1.146, "GBP": 0.8588, "JPY": 180.94},
        },
    }
)

# Flat single-date shape (/latest or /{date}) — parse_for_metadata must handle both.
SINGLE_DATE_JSON = json.dumps({"amount": 1.0, "base": "EUR", "date": "2026-09-19", "rates": {"USD": 1.08, "GBP": 0.85}})


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
    real_config_content = currency.CONFIG_PATH.read_text(encoding="utf-8")
    (tmp_path / "configs").mkdir(exist_ok=True)
    (tmp_path / "configs" / "sources.json").write_text(real_config_content, encoding="utf-8")


# ---- parse_for_metadata: pure parsing, no network, both response shapes ----


def test_parse_for_metadata_handles_range_shape_multiple_dates_multiple_currencies():
    records, fields = currency.parse_for_metadata(RANGE_JSON)
    assert len(records) == 6  # 2 dates x 3 currencies
    assert {"date", "currency", "rate"} == set(fields)
    assert {r["currency"] for r in records} == {"USD", "GBP", "JPY"}
    assert {r["date"] for r in records} == {"2026-09-17", "2026-09-18"}


def test_parse_for_metadata_handles_single_date_shape():
    records, _fields = currency.parse_for_metadata(SINGLE_DATE_JSON)
    assert len(records) == 2
    assert all(r["date"] == "2026-09-19" for r in records)
    assert {r["currency"] for r in records} == {"USD", "GBP"}


def test_parse_for_metadata_does_not_hardcode_a_currency_list():
    exotic = json.dumps(
        {"amount": 1.0, "base": "EUR", "start_date": "2026-09-18", "end_date": "2026-09-18", "rates": {"2026-09-18": {"ZAR": 18.65, "ISK": 139.4}}}
    )
    records, _fields = currency.parse_for_metadata(exotic)
    assert {r["currency"] for r in records} == {"ZAR", "ISK"}


def test_parse_for_metadata_empty_rates_range_shape():
    empty = json.dumps({"amount": 1.0, "base": "EUR", "start_date": "2026-09-20", "end_date": "2026-09-20", "rates": {}})
    records, _fields = currency.parse_for_metadata(empty)
    assert records == []


def test_parse_for_metadata_malformed_json_raises():
    with pytest.raises(json.JSONDecodeError):
        currency.parse_for_metadata("{not valid json")


# ---- structural / shape checks ----


def test_check_response_shape_passes_for_valid_response():
    assert currency.check_response_shape(RANGE_JSON)["passed"] is True


def test_check_response_shape_fails_when_rates_missing():
    result = currency.check_response_shape(json.dumps({"base": "EUR"}))
    assert result["passed"] is False


def test_check_response_shape_fails_when_rates_not_a_dict():
    result = currency.check_response_shape(json.dumps({"rates": ["not", "a", "dict"]}))
    assert result["passed"] is False


def test_check_response_shape_passes_for_empty_but_valid_rates():
    result = currency.check_response_shape(json.dumps({"rates": {}}))
    assert result["passed"] is True


# ---- latest-rate-date extraction (watermark source value) ----


def test_extract_latest_rate_date_range_shape_uses_max_actual_date():
    # Frankfurter's own end_date could theoretically differ from the true max data
    # key in edge cases; we trust the actual rates keys, not the envelope field.
    assert currency._extract_latest_rate_date(RANGE_JSON) == "2026-09-18"


def test_extract_latest_rate_date_single_shape():
    assert currency._extract_latest_rate_date(SINGLE_DATE_JSON) == "2026-09-19"


def test_extract_latest_rate_date_empty_rates_returns_none():
    empty = json.dumps({"start_date": "2026-09-20", "end_date": "2026-09-20", "rates": {}})
    assert currency._extract_latest_rate_date(empty) is None


# ---- date-range arithmetic ----


def test_compute_incremental_range_starts_day_after_watermark():
    start, end = currency._compute_incremental_range("2026-09-18")
    assert start == "2026-09-19"


def test_compute_initial_backfill_range_spans_configured_days():
    start, end = currency._compute_initial_backfill_range(90)
    from datetime import date as _date

    assert (_date.fromisoformat(end) - _date.fromisoformat(start)).days == 90


# ---- fetch_raw: request construction, all HTTP mocked ----


def test_fetch_raw_uses_get_and_sends_user_agent(monkeypatch):
    captured = {}

    def fake_get(url, headers, timeout):
        captured.update(url=url, headers=headers, timeout=timeout)
        return FakeResponse(RANGE_JSON)

    monkeypatch.setattr(currency.requests, "get", fake_get)
    response = currency.fetch_raw("https://api.frankfurter.dev/v1/2026-09-17..2026-09-19", "MyAgent/1.0", 30)

    assert captured["url"] == "https://api.frankfurter.dev/v1/2026-09-17..2026-09-19"
    assert captured["headers"]["User-Agent"] == "MyAgent/1.0"
    assert captured["timeout"] == 30
    assert response.text == RANGE_JSON


def test_fetch_raw_propagates_http_error(monkeypatch):
    def fake_get(url, headers, timeout):
        return FakeResponse("", status_code=500, raise_exc=requests.HTTPError("500 Server Error"))

    monkeypatch.setattr(currency.requests, "get", fake_get)
    with pytest.raises(requests.HTTPError):
        currency.fetch_raw("https://api.frankfurter.dev/v1/2026-09-17..2026-09-19", "Agent/1.0", 30)


# ---- run(): initial backfill ----


def test_initial_backfill_used_when_no_watermark_and_uses_range_endpoint(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    captured = {}

    def fake_get(url, headers, timeout):
        captured["url"] = url
        return FakeResponse(RANGE_JSON)

    monkeypatch.setattr(currency.requests, "get", fake_get)
    config = currency._load_config()

    run_dir = currency.run()

    assert captured["url"].startswith("https://api.frankfurter.dev/v1/")
    assert ".." in captured["url"]  # range endpoint, not /latest
    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["extraction_mode"] == "initial_backfill"
    assert metadata["previous_watermark"] is None
    assert metadata["source_type"] == "live_api"
    assert metadata["record_count"] == 6


def test_initial_backfill_range_spans_configured_backfill_days(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)

    def fake_get(url, headers, timeout):
        return FakeResponse(RANGE_JSON)

    monkeypatch.setattr(currency.requests, "get", fake_get)
    config = currency._load_config()
    run_dir = currency.run()

    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
    from datetime import date as _date

    span = (_date.fromisoformat(metadata["requested_end_date"]) - _date.fromisoformat(metadata["requested_start_date"])).days
    assert span == config["initial_backfill_days"]


def test_watermark_advances_after_successful_initial_backfill(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)

    def fake_get(url, headers, timeout):
        return FakeResponse(RANGE_JSON)

    monkeypatch.setattr(currency.requests, "get", fake_get)
    assert watermark.read_watermark("currency") is None
    currency.run()
    assert watermark.read_watermark("currency") == "2026-09-18"  # max actual date, not requested end


# ---- run(): incremental ----


def test_incremental_used_when_watermark_exists_and_requests_correct_range(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("currency", "2026-09-16", run_dir="seed_run")
    captured = {}

    def fake_get(url, headers, timeout):
        captured["url"] = url
        return FakeResponse(RANGE_JSON)

    monkeypatch.setattr(currency.requests, "get", fake_get)
    run_dir = currency.run()

    assert "2026-09-17.." in captured["url"]  # watermark + 1 day
    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["extraction_mode"] == "incremental"
    assert metadata["previous_watermark"] == "2026-09-16"


def test_incremental_sends_centralized_user_agent(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("currency", "2026-09-16", run_dir="seed_run")
    captured = {}

    def fake_get(url, headers, timeout):
        captured["headers"] = headers
        return FakeResponse(RANGE_JSON)

    monkeypatch.setattr(currency.requests, "get", fake_get)
    currency.run()
    assert captured["headers"]["User-Agent"] == settings.user_agent


def test_watermark_advances_after_successful_incremental_load(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("currency", "2026-09-16", run_dir="seed_run")

    def fake_get(url, headers, timeout):
        return FakeResponse(RANGE_JSON)

    monkeypatch.setattr(currency.requests, "get", fake_get)
    currency.run()
    assert watermark.read_watermark("currency") == "2026-09-18"


def test_incremental_weekend_no_new_rate_is_valid_not_a_failure(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("currency", "2026-09-18", run_dir="seed_run")

    # Requested range spans a weekend with no publication — Frankfurter's real,
    # verified behavior is to return empty rates, not an error.
    no_new_rate = json.dumps({"amount": 1.0, "base": "EUR", "start_date": "2026-09-19", "end_date": "2026-09-19", "rates": {}})

    def fake_get(url, headers, timeout):
        return FakeResponse(no_new_rate)

    monkeypatch.setattr(currency.requests, "get", fake_get)
    run_dir = currency.run()
    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))

    assert metadata["record_count"] == 0
    assert metadata["validation"]["passed"] is True  # empty is valid for incremental
    assert metadata["watermark_updated"] is False  # nothing new actually observed
    assert watermark.read_watermark("currency") == "2026-09-18"  # unchanged, not fabricated forward


def test_incremental_skips_request_when_watermark_already_current(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    from datetime import datetime, timezone

    today = datetime.now(timezone.utc).date().isoformat()
    watermark.write_watermark("currency", today, run_dir="seed_run")

    called = {"count": 0}

    def fake_get(url, headers, timeout):
        called["count"] += 1
        return FakeResponse(RANGE_JSON)

    monkeypatch.setattr(currency.requests, "get", fake_get)
    result = currency.run()

    assert result is None
    assert called["count"] == 0  # no request sent for an inverted/empty range
    assert watermark.read_watermark("currency") == today  # unchanged


# ---- failure behavior ----


def test_watermark_not_advanced_when_http_request_fails(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("currency", "2026-09-16", run_dir="seed_run")

    def fake_get(url, headers, timeout):
        return FakeResponse("", status_code=500, raise_exc=requests.HTTPError("500 Server Error"))

    monkeypatch.setattr(currency.requests, "get", fake_get)
    with pytest.raises(requests.HTTPError):
        currency.run()

    assert watermark.read_watermark("currency") == "2026-09-16"


def test_watermark_not_advanced_when_validation_fails(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    watermark.write_watermark("currency", "2026-09-16", run_dir="seed_run")

    malformed_shape = json.dumps({"base": "EUR"})  # 'rates' missing entirely

    def fake_get(url, headers, timeout):
        return FakeResponse(malformed_shape)

    monkeypatch.setattr(currency.requests, "get", fake_get)
    run_dir = currency.run()  # does not raise — Bronze still written honestly

    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["validation"]["passed"] is False
    assert metadata["watermark_updated"] is False
    assert watermark.read_watermark("currency") == "2026-09-16"


def test_malformed_json_raises_cleanly_and_writes_no_bronze(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)

    def fake_get(url, headers, timeout):
        return FakeResponse("{not valid json")

    monkeypatch.setattr(currency.requests, "get", fake_get)
    with pytest.raises(json.JSONDecodeError):
        currency.run()

    assert not (tmp_path / "data" / "bronze" / "currency").exists()
    assert watermark.read_watermark("currency") is None


# ---- Bronze conventions ----


def test_run_never_overwrites_previous_bronze_run(tmp_path, monkeypatch):
    _stage_real_config_at(tmp_path)
    monkeypatch.chdir(tmp_path)
    import time

    def fake_get(url, headers, timeout):
        return FakeResponse(RANGE_JSON)

    monkeypatch.setattr(currency.requests, "get", fake_get)

    first_run_dir = currency.run()
    first_content = (first_run_dir / currency.RAW_FILENAME).read_text(encoding="utf-8")

    time.sleep(1.1)  # avoid same-second run-dir collision (see Step 4 report, item P)
    second_run_dir = currency.run()

    assert second_run_dir != first_run_dir
    assert first_run_dir.exists()
    assert (first_run_dir / currency.RAW_FILENAME).read_text(encoding="utf-8") == first_content
