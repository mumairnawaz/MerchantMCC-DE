"""Ingest FX rate data from Frankfurter (ECB-sourced) into Bronze.

Real live external API (Reconciliation / multi-currency domain) — not reference/static
data. `run()` is watermark-aware, matching the pattern established for OSM in Step 4:

  - No persisted watermark  -> initial historical backfill (the configured
    `initial_backfill_days` window) via the date-range endpoint.
  - Watermark present       -> incremental load: request {watermark+1}..{today} via
    the same date-range endpoint.

Frankfurter returns two different shapes depending on which endpoint is hit:
  - /latest or /{date}   -> flat: {amount, base, date, rates: {currency: rate}}
  - /{start}..{end}      -> nested: {amount, base, start_date, end_date,
                                      rates: {date: {currency: rate}}}
This module only ever calls the date-range endpoint (for both initial and incremental
loads), but parse_for_metadata() understands both shapes for robustness.

Known source behavior, not an ingestion defect: Frankfurter publishes once per ECB
business day. A requested date with no publication (weekends, EU holidays) is silently
omitted from the response rather than erroring — verified live by requesting a range
spanning a non-publication day and observing the response's own end_date clip back to
the last date that actually had data. An incremental run that finds nothing new is a
valid, expected outcome, not a failure.
"""

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from src.ingestion import common, validation, watermark
from src.ingestion.settings import settings

SOURCE_NAME = "currency"
CONFIG_PATH = Path("configs/sources.json")
RAW_FILENAME = "rates.json"


def _load_config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))[SOURCE_NAME]


def fetch_raw(url: str, user_agent: str, timeout: int) -> requests.Response:
    """GET the given Frankfurter URL. No API key is required (verified live). A
    User-Agent is sent as good practice, consistent with the OSM module, even though
    Frankfurter has not been observed to require one.
    """
    response = requests.get(url, headers={"User-Agent": user_agent}, timeout=timeout)
    response.raise_for_status()
    return response


def _is_range_response(data: dict) -> bool:
    return "start_date" in data and "end_date" in data


def parse_for_metadata(raw_json: str) -> tuple[list[dict], list[str]]:
    """Flatten either response shape into per (date, currency) records, for
    metadata/validation only — never a fixed/invented currency list, always exactly
    whatever the response actually contains.
    """
    data = json.loads(raw_json)
    rates = data.get("rates", {})
    if _is_range_response(data):
        records = [
            {"date": rate_date, "currency": code, "rate": rate}
            for rate_date, currency_map in rates.items()
            for code, rate in currency_map.items()
        ]
    else:
        rate_date = data.get("date")
        records = [{"date": rate_date, "currency": code, "rate": rate} for code, rate in rates.items()]
    fields = ["date", "currency", "rate"]
    return records, fields


def check_response_shape(raw_json: str) -> dict[str, Any]:
    """Structural check: is 'rates' present and dict-valued? Independent of whether
    it happens to be empty (that's check_non_empty's job)."""
    data = json.loads(raw_json)
    return validation.check_key_is_dict(data, "rates")


def _extract_latest_rate_date(raw_json: str) -> str | None:
    """The latest date the response actually contains a rate for — never a requested
    date that turned out to have no publication. ISO 'YYYY-MM-DD' strings sort
    correctly lexicographically, so max() is safe without date parsing.
    """
    data = json.loads(raw_json)
    if _is_range_response(data):
        dates = list(data.get("rates", {}).keys())
        return max(dates) if dates else None
    return data.get("date")


def _compute_initial_backfill_range(backfill_days: int) -> tuple[str, str]:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=backfill_days)
    return start.isoformat(), end.isoformat()


def _compute_incremental_range(previous_watermark: str) -> tuple[str, str]:
    start = date.fromisoformat(previous_watermark) + timedelta(days=1)
    end = datetime.now(timezone.utc).date()
    return start.isoformat(), end.isoformat()


def _validate(records: list[dict], raw_text: str, load_type: str) -> dict[str, Any]:
    """Initial backfill keeps a hard non-empty requirement — a 90-day window should
    never legitimately come back with zero published rates. Incremental loads treat
    zero new dates as valid (see module docstring); per-record checks only run when
    there's something to check. No duplicate-key check: the record shape (one row per
    (date, currency) pair, where each date's rates dict has structurally unique
    currency keys, and each date appears once as an outer rates key) makes duplicates
    impossible by construction, not just improbable.
    """
    if load_type == "initial_backfill":
        checks = [
            check_response_shape(raw_text),
            validation.check_non_empty(records),
            validation.check_required_fields(records, ["date", "currency", "rate"]),
        ]
    else:
        checks = [check_response_shape(raw_text), validation.check_non_empty(records, informational=True)]
        if records:
            checks.append(validation.check_required_fields(records, ["date", "currency", "rate"]))
    return validation.combine(checks)


def run() -> Path | None:
    config = _load_config()
    previous_watermark = watermark.read_watermark(SOURCE_NAME)

    if previous_watermark is None:
        load_type = "initial_backfill"
        start, end = _compute_initial_backfill_range(config["initial_backfill_days"])
    else:
        load_type = "incremental"
        start, end = _compute_incremental_range(previous_watermark)
        if start > end:
            # Watermark is already current (e.g. a second run the same day after a
            # successful capture of today's rate) — an inverted range is not a
            # request we've verified Frankfurter's real behavior for, so we don't
            # send it. Nothing to fetch; no Bronze run, watermark untouched.
            print(f"currency: watermark {previous_watermark} is already current — nothing to fetch.")
            return None

    url = config["date_range_url_template"].format(start=start, end=end)
    timeout = config.get("request_timeout_seconds", settings.request_timeout_seconds)

    response = fetch_raw(url, settings.user_agent, timeout)
    records, fields = parse_for_metadata(response.text)
    validation_result = _validate(records, response.text, load_type)

    latest_rate_date = _extract_latest_rate_date(response.text)
    should_advance_watermark = validation_result["passed"] and bool(records) and latest_rate_date is not None

    run_dir = common.new_run_dir(SOURCE_NAME)
    common.write_raw(run_dir, RAW_FILENAME, response.text)
    common.write_metadata(
        run_dir,
        source_name=SOURCE_NAME,
        source_url=url,
        raw_filename=RAW_FILENAME,
        record_count=len(records),
        discovered_fields=fields,
        validation_result=validation_result,
        http_status=response.status_code,
        http_headers={
            k: v for k, v in response.headers.items() if k.lower() in ("etag", "last-modified", "content-length")
        },
        extra={
            "source_type": config.get("source_type", "live_api"),
            "extraction_mode": load_type,
            "requested_start_date": start,
            "requested_end_date": end,
            "latest_rate_date_observed": latest_rate_date,
            "previous_watermark": previous_watermark,
            "watermark_updated": should_advance_watermark,
            "new_watermark": latest_rate_date if should_advance_watermark else None,
            "user_agent_used": settings.user_agent,
            "request_timeout_seconds": timeout,
        },
    )

    if should_advance_watermark:
        watermark.write_watermark(SOURCE_NAME, latest_rate_date, run_dir=str(run_dir))

    return run_dir


if __name__ == "__main__":
    result_dir = run()
    print(f"Currency ingestion complete -> {result_dir}")
