"""Ingest the public BIN/IIN card-issuer reference dataset into Bronze.

Full-dataset ingestion (~374,788 rows / ~26MB) approved 2026-09-20 after verifying the
source is fully, freely accessible (no auth) and the size is workstation-safe. BIN is
always treated as a string (verified: all real values are exactly 6 digits — never
coerce to int, leading zeros are real). ~48% of real rows have a blank Issuer — this
is preserved as-is in Bronze; Silver must decide exclude-vs-"Unknown" handling
(not yet implemented, proposed rule documented below), never inventing a name.

This data enriches SYNTHETIC tokenized cardholder records with realistic issuer/brand/
country metadata. It is never linked to any real cardholder, PAN, or account.
"""

import csv
import io
import json
from pathlib import Path

import requests

from src.ingestion import common, validation

SOURCE_NAME = "card_issuer"
CONFIG_PATH = Path("configs/sources.json")

BIN_PATTERN = r"^\d{6}$"  # verified against the full real dataset — all 374,788 BINs are exactly 6 digits
ISO2_PATTERN = r"^[A-Z]{2}$"


def _load_config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))[SOURCE_NAME]


def fetch_raw(url: str) -> requests.Response:
    # Larger, real-world file (~26MB) — longer timeout than the small reference sources.
    response = requests.get(url, timeout=120)
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
        # Issuer is deliberately NOT required — ~48% of real rows lack it; that's a
        # documented data-quality characteristic of this source, not a parse failure.
        validation.check_required_fields(records, ["BIN", "Brand"]),
        validation.check_duplicates(records, "BIN"),
        validation.check_field_pattern(records, "BIN", BIN_PATTERN, "bin_6_digit"),
        validation.check_field_pattern(records, "isoCode2", ISO2_PATTERN, "iso_alpha2_country"),
    ]
    validation_result = validation.combine(checks)

    missing_issuer = sum(1 for r in records if not r.get("Issuer", "").strip())
    distinct_brands = len({r.get("Brand") for r in records if r.get("Brand")})
    distinct_countries = len({r.get("isoCode2") for r in records if r.get("isoCode2")})

    run_dir = common.new_run_dir(SOURCE_NAME)
    common.write_raw(run_dir, "bin_list_data.csv", response.text)
    common.write_metadata(
        run_dir,
        source_name=SOURCE_NAME,
        source_url=config["url"],
        raw_filename="bin_list_data.csv",
        record_count=len(records),
        discovered_fields=fields,
        validation_result=validation_result,
        http_status=response.status_code,
        http_headers={
            k: v for k, v in response.headers.items() if k.lower() in ("etag", "last-modified", "content-length")
        },
        extra={
            "license": config.get("license"),
            "attribution": config.get("attribution"),
            "rows_with_missing_issuer": missing_issuer,
            "rows_with_missing_issuer_pct": round(missing_issuer / len(records) * 100, 1) if records else None,
            "distinct_brands": distinct_brands,
            "distinct_countries": distinct_countries,
            "silver_missing_issuer_rule_proposal": (
                "Do NOT exclude rows with a blank Issuer — Brand and country are still real "
                "and usable for enriching synthetic cardholder tokens. Proposed rule: set "
                "issuer_name to an explicit 'Unknown' sentinel (never an invented name) when "
                "blank, and carry a has_known_issuer boolean flag downstream. This is a "
                "PROPOSAL for Phase 5, not yet implemented."
            ),
        },
    )
    return run_dir


if __name__ == "__main__":
    result_dir = run()
    print(f"Card issuer (BIN/IIN) ingestion complete -> {result_dir}")
