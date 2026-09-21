"""Ingest public country reference data (REST Countries) into Bronze."""

import json
from pathlib import Path

import requests

from src.ingestion import common, validation

SOURCE_NAME = "country"
CONFIG_PATH = Path("configs/sources.json")


def _load_config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))[SOURCE_NAME]


def fetch_raw(url: str) -> requests.Response:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response


def parse_for_metadata(raw_json: str) -> tuple[list[dict], list[str]]:
    data = json.loads(raw_json)
    records = data if isinstance(data, list) else [data]
    fields = sorted({key for record in records for key in record.keys()})
    return records, fields


def run() -> Path:
    config = _load_config()
    response = fetch_raw(config["url"])
    records, fields = parse_for_metadata(response.text)

    checks = [
        validation.check_non_empty(records),
        validation.check_required_fields(records, ["cca2", "cca3", "name"]),
        validation.check_duplicates(records, "cca3"),
    ]
    validation_result = validation.combine(checks)

    run_dir = common.new_run_dir(SOURCE_NAME)
    common.write_raw(run_dir, "countries.json", response.text)
    common.write_metadata(
        run_dir,
        source_name=SOURCE_NAME,
        source_url=config["url"],
        raw_filename="countries.json",
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
    print(f"Country ingestion complete -> {result_dir}")
