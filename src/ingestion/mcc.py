"""Ingest the public MCC (Merchant Category Code) reference list into Bronze."""

import csv
import io
import json
from pathlib import Path

import requests

from src.ingestion import common, validation

SOURCE_NAME = "mcc"
CONFIG_PATH = Path("configs/sources.json")


def _load_config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))[SOURCE_NAME]


def fetch_raw(url: str) -> requests.Response:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response


def parse_for_metadata(raw_csv: str) -> tuple[list[dict], list[str]]:
    """Introspect the raw CSV to discover fields/record count. Does not reshape data."""
    reader = csv.DictReader(io.StringIO(raw_csv))
    records = list(reader)
    fields = reader.fieldnames or []
    return records, fields


def _find_mcc_code_field(fields: list[str]) -> str | None:
    for field in fields:
        if field.strip().lower() == "mcc":
            return field
    return None


def run() -> Path:
    config = _load_config()
    response = fetch_raw(config["url"])
    records, fields = parse_for_metadata(response.text)

    mcc_field = _find_mcc_code_field(fields)
    checks = [validation.check_non_empty(records)]
    if mcc_field:
        checks.append(validation.check_required_fields(records, [mcc_field]))
        checks.append(validation.check_duplicates(records, mcc_field))
    else:
        checks.append(
            {
                "name": "required_fields_present",
                "passed": False,
                "details": {"reason": "no column matching 'mcc' found", "discovered_fields": fields},
            }
        )
    validation_result = validation.combine(checks)

    run_dir = common.new_run_dir(SOURCE_NAME)
    common.write_raw(run_dir, "mcc_codes.csv", response.text)
    common.write_metadata(
        run_dir,
        source_name=SOURCE_NAME,
        source_url=config["url"],
        raw_filename="mcc_codes.csv",
        record_count=len(records),
        discovered_fields=fields,
        validation_result=validation_result,
        http_status=response.status_code,
        http_headers={
            k: v for k, v in response.headers.items() if k.lower() in ("etag", "last-modified", "content-length")
        },
    )
    return run_dir


if __name__ == "__main__":
    result_dir = run()
    print(f"MCC ingestion complete -> {result_dir}")
