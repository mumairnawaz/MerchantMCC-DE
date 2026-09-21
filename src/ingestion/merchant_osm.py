"""Ingest merchant/place data from OpenStreetMap (Overpass API) into Bronze.

Real live external API (Merchant Intelligence domain) — not reference/static data.

`run()` is watermark-aware: no persisted watermark -> full extraction; a watermark
present -> incremental extraction using Overpass QL's `newer:` filter, scoped to
the exact same bbox/shop/amenity filters as the full query (never expanded).

Known limitation, by design, not an oversight: `newer:` surfaces additions and
edits but does not reliably surface deletions within our bounded extract. This
module does not attempt to solve that. A periodic full extraction (triggered by
clearing data/watermarks/merchant_osm.json) remains the reconciliation mechanism
for deletions — see docs/13-incremental-ingestion.md.
"""

import json
from pathlib import Path
from typing import Any

import requests

from src.ingestion import common, validation, watermark
from src.ingestion.settings import settings

SOURCE_NAME = "merchant_osm"
CONFIG_PATH = Path("configs/sources.json")
RAW_FILENAME = "osm_places.json"


def _load_config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))[SOURCE_NAME]


def fetch_raw(url: str, query: str, user_agent: str, timeout: int) -> requests.Response:
    """POST the Overpass QL query. A descriptive User-Agent is required by the live
    endpoint — verified live: a request without one returns HTTP 406. TLS
    verification is left at its default (on); no API key is sent because none is
    required.
    """
    response = requests.post(url, data={"data": query}, headers={"User-Agent": user_agent}, timeout=timeout)
    response.raise_for_status()
    return response


def parse_for_metadata(raw_json: str) -> tuple[list[dict], list[str]]:
    """Introspect the raw response to discover fields/record count. Does not reshape
    or clean the data — no MCC assignment, category standardization, deduplication,
    or missing-value imputation happens here; that belongs to Silver.
    """
    data = json.loads(raw_json)
    records = data.get("elements", [])
    fields = sorted({key for record in records for key in record.keys()})
    return records, fields


def check_response_shape(raw_json: str) -> dict[str, Any]:
    """Structural check: does this look like a valid Overpass response at all —
    i.e. an 'elements' key present and list-valued? Independent of whether that
    list happens to be empty (that's check_non_empty's job).
    """
    data = json.loads(raw_json)
    return validation.check_key_is_list(data, "elements")


def _extract_osm_dataset_timestamp(raw_json: str) -> str | None:
    """The OSM database snapshot timestamp the query was run against
    (osm3s.timestamp_osm_base) — distinct from our own ingestion_timestamp_utc.
    Not guaranteed present; never invented if absent. This is the value used as
    the next run's watermark.
    """
    data = json.loads(raw_json)
    return data.get("osm3s", {}).get("timestamp_osm_base")


def _build_incremental_query(config: dict, watermark_value: str) -> str:
    template = config.get("incremental_query_template")
    if not template:
        raise KeyError(
            "configs/sources.json merchant_osm.incremental_query_template is missing — "
            "cannot build an incremental query without it."
        )
    return template.format(watermark=watermark_value)


def _validate(records: list[dict], raw_text: str, load_type: str) -> dict[str, Any]:
    """Full loads keep the exact Step 3 behaviour (empty is a hard failure — our
    known-populated bbox should never legitimately return zero on a full extract).
    Incremental loads treat zero new/changed records as valid and expected; the
    per-record checks only run when there's something to check, since "no records
    to check" is not itself a defect on an incremental run.
    """
    if load_type == "full":
        checks = [
            check_response_shape(raw_text),
            validation.check_non_empty(records),
            validation.check_required_fields(records, ["id", "type"]),
            validation.check_duplicates(records, "id"),
        ]
    else:
        checks = [check_response_shape(raw_text), validation.check_non_empty(records, informational=True)]
        if records:
            checks.append(validation.check_required_fields(records, ["id", "type"]))
            checks.append(validation.check_duplicates(records, "id"))
    return validation.combine(checks)


def run() -> Path:
    config = _load_config()
    previous_watermark = watermark.read_watermark(SOURCE_NAME)
    load_type = "incremental" if previous_watermark else "full"

    if load_type == "incremental":
        query = _build_incremental_query(config, previous_watermark)
        timeout = config.get("incremental_request_timeout_seconds", settings.request_timeout_seconds)
    else:
        query = config["query"]
        timeout = config.get("request_timeout_seconds", settings.request_timeout_seconds)

    response = fetch_raw(config["url"], query, settings.user_agent, timeout)
    records, fields = parse_for_metadata(response.text)
    validation_result = _validate(records, response.text, load_type)

    new_snapshot_timestamp = _extract_osm_dataset_timestamp(response.text)
    should_advance_watermark = validation_result["passed"] and new_snapshot_timestamp is not None

    run_dir = common.new_run_dir(SOURCE_NAME)
    common.write_raw(run_dir, RAW_FILENAME, response.text)
    common.write_metadata(
        run_dir,
        source_name=SOURCE_NAME,
        source_url=config["url"],
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
            "area_name": config.get("area_name"),
            "bbox": config.get("bbox"),
            "query": query,
            "user_agent_used": settings.user_agent,
            "request_timeout_seconds": timeout,
            "extraction_mode": load_type,
            "osm_dataset_timestamp": new_snapshot_timestamp,
            "previous_watermark": previous_watermark,
            "watermark_updated": should_advance_watermark,
            "new_watermark": new_snapshot_timestamp if should_advance_watermark else None,
            "deletion_detection_limitation": (
                "newer: surfaces additions/edits only. Deletions within the bbox are not "
                "detected by incremental runs; periodic full extraction remains required "
                "for reconciliation."
            ),
        },
    )

    if should_advance_watermark:
        watermark.write_watermark(SOURCE_NAME, new_snapshot_timestamp, run_dir=str(run_dir))

    return run_dir


if __name__ == "__main__":
    result_dir = run()
    print(f"Merchant OSM ingestion complete -> {result_dir}")
