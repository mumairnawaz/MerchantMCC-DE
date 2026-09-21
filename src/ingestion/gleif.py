"""Ingest legal-entity reference data from GLEIF into Bronze.

Real live external API (Issuer / Institution / Program-Owner domain). `run()` is
watermark-aware, matching the pattern established for OSM (Step 4) and Frankfurter
(Step 5):

  - No persisted watermark -> INITIAL load: a deterministic, capped 10,000-record
    (50-page at page[size]=200) portfolio-scoped extract from the GB + ACTIVE +
    ISSUED population, ordered by sort=lei for reproducibility. This is NOT complete
    GB coverage and NOT a statistically representative sample — see
    docs/06-data-source-catalog.md and configs/sources.json's `sampling_limitation`.
    sort=lei is deterministic (verified live) but clusters records by issuing Local
    Operating Unit prefix, not by any business attribute.

  - Watermark present      -> INCREMENTAL load: same GB/ACTIVE/ISSUED filters plus
    `registration.lastUpdateDate >= watermark`, paginated through the COMPLETE
    matching result set — the 10,000-record cap applies only to the initial load.

Every page's raw JSON:API response is written to Bronze verbatim, one file per page
(page_0001.json, page_0002.json, ...). Nothing is flattened, cleaned, renamed,
deduplicated, or enriched in Bronze — a normalized flat record shape is derived only
in memory, for metadata/validation purposes, exactly matching the pattern already
used by every other ingestion module in this project.

All-or-nothing per run: every required page is fetched successfully before anything
is written to Bronze. An HTTP failure on any page aborts the whole run with no
partial Bronze write and no watermark change — there is no partial/incremental
Bronze write within a single run.
"""

import json
from pathlib import Path
from typing import Any

import requests

from src.ingestion import common, validation, watermark
from src.ingestion.settings import settings

SOURCE_NAME = "gleif"
CONFIG_PATH = Path("configs/sources.json")

# Purely a runaway-loop safety net for incremental runs, which have no business-scope
# page cap — not a semantic limit. Real observed daily change volume for this filtered
# population is expected to be far smaller than this.
_INCREMENTAL_SAFETY_MAX_PAGES = 5000

NORMALIZED_FIELDS = [
    "lei",
    "entity_legal_name",
    "entity_legal_address_country",
    "entity_legal_address_city",
    "entity_legal_address_region",
    "entity_legal_address_postal_code",
    "entity_legal_address_lines",
    "entity_hq_address_country",
    "entity_hq_address_city",
    "entity_hq_address_region",
    "entity_hq_address_postal_code",
    "entity_hq_address_lines",
    "entity_legal_form_id",
    "entity_category",
    "entity_status",
    "entity_jurisdiction",
    "entity_creation_date",
    "registration_initial_registration_date",
    "registration_last_update_date",
    "registration_status",
    "registration_next_renewal_date",
    "bic",
]


def _load_config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))[SOURCE_NAME]


def _build_params(config: dict, page_number: int, incremental_watermark: str | None) -> dict:
    params = {
        "filter[entity.jurisdiction]": config["jurisdiction_filter"],
        "filter[entity.status]": config["entity_status_filter"],
        "filter[registration.status]": config["registration_status_filter"],
        "sort": config["sort"],
        "page[size]": config["page_size"],
        "page[number]": page_number,
    }
    if incremental_watermark is not None:
        params["filter[registration.lastUpdateDate]"] = f">={incremental_watermark}"
    return params


def fetch_page(base_url: str, params: dict, user_agent: str, timeout: int) -> requests.Response:
    """GET one page. No API key is sent — verified live that none is required. No
    Authorization header of any kind is added."""
    response = requests.get(base_url, params=params, headers={"User-Agent": user_agent}, timeout=timeout)
    response.raise_for_status()
    return response


def _normalize_record(raw_record: dict) -> dict:
    """Extract exactly the approved field set into a flat dict, for metadata/
    validation introspection only — never written to Bronze, never reshapes what
    Bronze stores. No field beyond the approved list is invented."""
    attrs = raw_record.get("attributes", {})
    entity = attrs.get("entity") or {}
    legal_addr = entity.get("legalAddress") or {}
    hq_addr = entity.get("headquartersAddress") or {}
    legal_name = entity.get("legalName") or {}
    legal_form = entity.get("legalForm") or {}
    registration = attrs.get("registration") or {}
    return {
        "lei": attrs.get("lei"),
        "entity_legal_name": legal_name.get("name"),
        "entity_legal_address_country": legal_addr.get("country"),
        "entity_legal_address_city": legal_addr.get("city"),
        "entity_legal_address_region": legal_addr.get("region"),
        "entity_legal_address_postal_code": legal_addr.get("postalCode"),
        "entity_legal_address_lines": legal_addr.get("addressLines"),
        "entity_hq_address_country": hq_addr.get("country"),
        "entity_hq_address_city": hq_addr.get("city"),
        "entity_hq_address_region": hq_addr.get("region"),
        "entity_hq_address_postal_code": hq_addr.get("postalCode"),
        "entity_hq_address_lines": hq_addr.get("addressLines"),
        "entity_legal_form_id": legal_form.get("id"),
        "entity_category": entity.get("category"),
        "entity_status": entity.get("status"),
        "entity_jurisdiction": entity.get("jurisdiction"),
        "entity_creation_date": entity.get("creationDate"),
        "registration_initial_registration_date": registration.get("initialRegistrationDate"),
        "registration_last_update_date": registration.get("lastUpdateDate"),
        "registration_status": registration.get("status"),
        "registration_next_renewal_date": registration.get("nextRenewalDate"),
        "bic": attrs.get("bic"),
    }


def _fetch_pages(
    config: dict, load_type: str, previous_watermark: str | None, user_agent: str, timeout: int
) -> tuple[list[str], list[dict], list[int]]:
    """Fetch every required page for this run. Raises immediately on any HTTP
    failure — the caller must not write Bronze or touch the watermark if this
    raises; no page fetched so far is returned to the caller in that case.
    """
    pages_raw: list[str] = []
    normalized_records: list[dict] = []
    status_codes: list[int] = []

    page_number = 1
    watermark_for_request = previous_watermark if load_type == "incremental" else None
    max_pages = config["initial_load_max_pages"] if load_type == "initial" else _INCREMENTAL_SAFETY_MAX_PAGES
    max_records = config["initial_load_max_records"] if load_type == "initial" else None

    while True:
        params = _build_params(config, page_number, watermark_for_request)
        response = fetch_page(config["url"], params, user_agent, timeout)
        pages_raw.append(response.text)
        status_codes.append(response.status_code)

        data = json.loads(response.text)
        page_records = data.get("data", [])
        normalized_records.extend(_normalize_record(r) for r in page_records)

        pagination_meta = (data.get("meta") or {}).get("pagination") or {}
        last_page = pagination_meta.get("lastPage")

        stop = False
        if last_page is not None and page_number >= last_page:
            stop = True  # API itself has no more pages — never over-fetch beyond real data
        if page_number >= max_pages:
            stop = True  # initial: the 10,000/50-page cap; incremental: safety net only
        if max_records is not None and len(normalized_records) >= max_records:
            stop = True  # initial cap reached, possibly before the page-count cap
        if load_type == "incremental" and not page_records and last_page is None:
            stop = True  # defensive: empty page with no pagination metadata to trust

        if stop:
            break
        page_number += 1

    return pages_raw, normalized_records, status_codes


def _validate(normalized_records: list[dict], pages_raw: list[str], load_type: str) -> dict[str, Any]:
    """Initial load keeps a hard non-empty requirement — the configured population
    (95,078 confirmed live) should never legitimately yield zero records. Incremental
    treats zero new/changed records as valid (see module docstring). Per-record
    checks (required fields, duplicate LEI, null visibility) only run when there are
    records to check.
    """
    shape_checks = []
    for i, raw in enumerate(pages_raw, 1):
        data = json.loads(raw)
        check = validation.check_key_is_list(data, "data")
        check = {**check, "details": {**check["details"], "page": i}}
        shape_checks.append(check)
    combined_shape_check = {
        "name": "key_is_list_data_all_pages",
        "passed": all(c["passed"] for c in shape_checks),
        "details": {"page_count": len(pages_raw), "per_page": shape_checks},
    }

    required_fields = ["lei", "entity_status", "entity_jurisdiction", "registration_status"]

    if load_type == "initial":
        checks = [
            combined_shape_check,
            validation.check_non_empty(normalized_records),
            validation.check_required_fields(normalized_records, required_fields),
            validation.check_duplicates(normalized_records, "lei"),
            validation.check_nulls(normalized_records, required_fields),
        ]
    else:
        checks = [combined_shape_check, validation.check_non_empty(normalized_records, informational=True)]
        if normalized_records:
            checks.append(validation.check_required_fields(normalized_records, required_fields))
            checks.append(validation.check_duplicates(normalized_records, "lei"))
            checks.append(validation.check_nulls(normalized_records, required_fields))
    return validation.combine(checks)


def _max_last_update_date(normalized_records: list[dict]) -> str | None:
    """ISO8601 '...Z' timestamps sort correctly lexicographically, same reasoning as
    Frankfurter's date max() — no datetime parsing needed."""
    dates = [r["registration_last_update_date"] for r in normalized_records if r.get("registration_last_update_date")]
    return max(dates) if dates else None


def run() -> Path:
    config = _load_config()
    previous_watermark = watermark.read_watermark(SOURCE_NAME)
    load_type = "initial" if previous_watermark is None else "incremental"
    timeout = config.get("request_timeout_seconds", settings.request_timeout_seconds)

    pages_raw, normalized_records, status_codes = _fetch_pages(
        config, load_type, previous_watermark, settings.user_agent, timeout
    )

    validation_result = _validate(normalized_records, pages_raw, load_type)
    new_watermark = _max_last_update_date(normalized_records)
    should_advance_watermark = validation_result["passed"] and new_watermark is not None

    run_dir = common.new_run_dir(SOURCE_NAME)
    page_files = []
    for i, raw in enumerate(pages_raw, 1):
        filename = f"page_{i:04d}.json"
        common.write_raw(run_dir, filename, raw)
        page_files.append(filename)

    if load_type == "initial":
        sampling_scope_note = (
            "A deterministic 10,000-record portfolio-scoped extract from the GB + "
            "ACTIVE + ISSUED GLEIF population."
        )
    else:
        sampling_scope_note = (
            "Incremental run — not subject to the initial-load record cap; paginates "
            "through the complete matching incremental result set."
        )
    sampling_limitation = (
        "sort=lei is deterministic (reproducible paging) but NOT statistically "
        "representative: LEI codes encode the issuing Local Operating Unit (LOU) in "
        "their prefix, so ascending-LEI order clusters records by issuing LOU rather "
        "than by any business attribute. Must never be described as 'all UK entities', "
        "'all GB entities', 'complete GB coverage', or 'representative statistical "
        "sampling'."
    )

    common.write_metadata(
        run_dir,
        source_name=SOURCE_NAME,
        source_url=config["url"],
        raw_filename=page_files[0] if page_files else "page_0001.json",
        record_count=len(normalized_records),
        discovered_fields=NORMALIZED_FIELDS,
        validation_result=validation_result,
        http_status=status_codes[-1] if status_codes else None,
        http_headers={},
        extra={
            "source_type": config.get("source_type", "live_api"),
            "extraction_mode": load_type,
            "page_files": page_files,
            "page_count": len(pages_raw),
            "page_size": config["page_size"],
            "http_status_codes": status_codes,
            "initial_record_cap_configured": config["initial_load_max_records"],
            "initial_page_cap_configured": config["initial_load_max_pages"],
            "initial_cap_applied": load_type == "initial",
            "filters_used": {
                "entity.jurisdiction": config["jurisdiction_filter"],
                "entity.status": config["entity_status_filter"],
                "registration.status": config["registration_status_filter"],
                "registration.lastUpdateDate": (
                    f">={previous_watermark}" if load_type == "incremental" else None
                ),
            },
            "sort_used": config["sort"],
            "previous_watermark": previous_watermark,
            "watermark_updated": should_advance_watermark,
            "new_watermark": new_watermark if should_advance_watermark else None,
            "user_agent_used": settings.user_agent,
            "request_timeout_seconds": timeout,
            "sampling_scope_note": sampling_scope_note,
            "sampling_limitation": sampling_limitation,
        },
    )

    if should_advance_watermark:
        watermark.write_watermark(SOURCE_NAME, new_watermark, run_dir=str(run_dir))

    return run_dir


if __name__ == "__main__":
    result_dir = run()
    print(f"GLEIF ingestion complete -> {result_dir}")
