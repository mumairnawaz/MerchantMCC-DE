"""Lightweight ingestion-time validation hooks.

These checks answer only "did we receive something structurally usable from the
source?" They are deliberately minimal. They are NOT the Silver-layer data-quality
gate (Phase 4, Pandera/dbt tests) — that layer will own business-rule validation,
type coercion, and cross-field checks. This module only protects against an empty,
malformed, or unexpectedly-shaped response at the point of ingestion.
"""

import re
from typing import Any


def check_non_empty(records: list[dict[str, Any]], informational: bool = False) -> dict[str, Any]:
    """If `informational` is True, an empty result never fails validation.

    Use this for incremental loads, where zero new/changed records is a valid,
    expected outcome — not a data-quality problem the way an empty *full* load
    would be.
    """
    passed = True if informational else len(records) > 0
    return {
        "name": "non_empty",
        "passed": passed,
        "details": {"record_count": len(records), "informational_only": informational},
    }


def check_required_fields(records: list[dict[str, Any]], required_fields: list[str]) -> dict[str, Any]:
    if not records:
        return {
            "name": "required_fields_present",
            "passed": False,
            "details": {"reason": "no records to check", "required_fields": required_fields},
        }
    missing_per_field = {}
    for field in required_fields:
        missing_count = sum(1 for r in records if field not in r)
        if missing_count:
            missing_per_field[field] = missing_count
    return {
        "name": "required_fields_present",
        "passed": len(missing_per_field) == 0,
        "details": {"required_fields": required_fields, "missing_counts": missing_per_field},
    }


def check_nulls(records: list[dict[str, Any]], fields: list[str]) -> dict[str, Any]:
    null_counts = {}
    for field in fields:
        null_counts[field] = sum(
            1 for r in records if r.get(field) is None or str(r.get(field, "")).strip() == ""
        )
    return {
        "name": "null_check",
        "passed": True,  # informational — Bronze does not reject on nulls, only reports them
        "details": {"null_counts": null_counts},
    }


def check_duplicates(records: list[dict[str, Any]], key_field: str, informational: bool = False) -> dict[str, Any]:
    """If `informational` is True, duplicates are reported but never fail validation.

    Use this for sources where repeated keys are an expected, documented property of
    the raw data (e.g. ISO 4217's AlphabeticCode repeating once per entity) rather than
    a defect — failing validation on a known, harmless condition would be misleading.
    """
    keys = [r.get(key_field) for r in records if key_field in r]
    duplicate_count = len(keys) - len(set(keys))
    passed = True if informational else duplicate_count == 0
    return {
        "name": "duplicate_key_check",
        "passed": passed,
        "details": {
            "key_field": key_field,
            "duplicate_count": duplicate_count,
            "informational_only": informational,
        },
    }


def check_field_pattern(records: list[dict[str, Any]], field: str, pattern: str, pattern_name: str) -> dict[str, Any]:
    """Check that every non-empty value of `field` matches `pattern`, e.g. BIN digit length."""
    compiled = re.compile(pattern)
    non_matching = 0
    for r in records:
        value = r.get(field)
        if value and not compiled.match(str(value)):
            non_matching += 1
    return {
        "name": f"field_pattern_{pattern_name}",
        "passed": non_matching == 0,
        "details": {"field": field, "pattern": pattern, "non_matching_count": non_matching},
    }


def check_key_is_list(data: dict[str, Any], key: str) -> dict[str, Any]:
    """Structural check on a raw parsed response itself — e.g. Overpass's top-level
    'elements' key — distinct from the record-level checks above which operate on
    the extracted record list.
    """
    present = key in data
    is_list = isinstance(data.get(key), list)
    return {
        "name": f"key_is_list_{key}",
        "passed": present and is_list,
        "details": {"key": key, "present": present, "is_list": is_list},
    }


def check_key_is_dict(data: dict[str, Any], key: str) -> dict[str, Any]:
    """Structural check on a raw parsed response itself — e.g. Frankfurter's top-level
    'rates' object (currency->rate for a single date, or date->currency->rate for a
    date-range response) — distinct from the record-level checks above.
    """
    present = key in data
    is_dict = isinstance(data.get(key), dict)
    return {
        "name": f"key_is_dict_{key}",
        "passed": present and is_dict,
        "details": {"key": key, "present": present, "is_dict": is_dict},
    }


def combine(checks: list[dict[str, Any]]) -> dict[str, Any]:
    return {"passed": all(c["passed"] for c in checks), "checks": checks}
