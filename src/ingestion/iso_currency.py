"""Ingest the public ISO 4217 currency reference dataset into Bronze.

Note: the raw file has one row per (Entity, Currency) pair, so AlphabeticCode is
NOT unique here — e.g. EUR appears once per Eurozone country. This is expected,
documented behavior, not a data defect. Silver will dedupe to one row per
currency_code; Bronze preserves the raw file exactly as received.
"""

import csv
import io
import json
from pathlib import Path

import requests

from src.ingestion import common, validation

SOURCE_NAME = "iso_currency"
CONFIG_PATH = Path("configs/sources.json")


def _load_config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))[SOURCE_NAME]


def fetch_raw(url: str) -> requests.Response:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response


def parse_for_metadata(raw_csv: str) -> tuple[list[dict], list[str]]:
    reader = csv.DictReader(io.StringIO(raw_csv))
    records = list(reader)
    fields = reader.fieldnames or []
    return records, fields


def run() -> Path:
    config = _load_config()
    response = fetch_raw(config["url"])
    records, fields = parse_for_metadata(response.text)

    checks = [
        validation.check_non_empty(records),
        validation.check_required_fields(records, ["AlphabeticCode", "Currency", "MinorUnit"]),
        # AlphabeticCode duplicates are expected (one row per entity) — informational only,
        # must not fail validation on a known, documented property of this source.
        validation.check_duplicates(records, "AlphabeticCode", informational=True),
        validation.check_field_pattern(records, "AlphabeticCode", r"^[A-Z]{3}$", "iso_alpha_code"),
    ]
    validation_result = validation.combine(checks)

    blank_minor_unit = sum(1 for r in records if not r.get("MinorUnit", "").strip())
    distinct_currency_codes = len({r.get("AlphabeticCode") for r in records if r.get("AlphabeticCode")})

    run_dir = common.new_run_dir(SOURCE_NAME)
    common.write_raw(run_dir, "iso_currency_codes.csv", response.text)
    common.write_metadata(
        run_dir,
        source_name=SOURCE_NAME,
        source_url=config["url"],
        raw_filename="iso_currency_codes.csv",
        record_count=len(records),
        discovered_fields=fields,
        validation_result=validation_result,
        http_status=response.status_code,
        http_headers={
            k: v for k, v in response.headers.items() if k.lower() in ("etag", "last-modified", "content-length")
        },
        extra={
            "license": config.get("license"),
            "distinct_currency_codes": distinct_currency_codes,
            "rows_with_blank_minor_unit": blank_minor_unit,
            "silver_dedup_rule": (
                "Group raw rows by AlphabeticCode; take one representative row per code "
                "(first non-null Currency/MinorUnit); Entity is dropped from dim_currency "
                "or moved to a separate country-currency bridge table — not yet built."
            ),
            "silver_minor_unit_rule_proposal": (
                "If MinorUnit is blank and WithdrawalDate is also blank (currency still active), "
                "default to 2 decimal places per common ISO 4217 convention. If WithdrawalDate is "
                "populated (historic/withdrawn currency), leave minor_unit NULL rather than assuming "
                "a convention for a defunct currency. This is a PROPOSAL for Phase 5, not yet implemented."
            ),
        },
    )
    return run_dir


if __name__ == "__main__":
    result_dir = run()
    print(f"ISO 4217 currency ingestion complete -> {result_dir}")
