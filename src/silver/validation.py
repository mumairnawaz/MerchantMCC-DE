"""Reusable Silver-layer validation hooks.

Distinct from src/ingestion/validation.py (Bronze-tier structural checks on a raw
response). This module implements the HARD FAIL vs WARNING classification frozen in
docs/21-silver-data-contracts.md §6 — a check's outcome is either "reject the
record" (hard fail) or "load it anyway, note the issue" (warning). Nothing here
silently discards a record: classification happens here, disposal happens in
src/silver/quarantine.py.
"""

import re
from typing import Any

HARD_FAIL = "hard_fail"
WARNING = "warning"


def check_required_field(record: dict[str, Any], field: str) -> dict[str, Any]:
    value = record.get(field)
    passed = field in record and value is not None and str(value).strip() != ""
    return {"name": f"required_field_{field}", "passed": passed, "severity": HARD_FAIL, "details": {"field": field}}


def check_optional_field_present(record: dict[str, Any], field: str) -> dict[str, Any]:
    """Informational-only in effect (WARNING severity) — a missing optional field is
    never grounds for rejection, only for a logged note."""
    value = record.get(field)
    passed = field in record and value is not None and str(value).strip() != ""
    return {"name": f"optional_field_{field}", "passed": passed, "severity": WARNING, "details": {"field": field}}


def check_pattern(record: dict[str, Any], field: str, pattern: str) -> dict[str, Any]:
    value = record.get(field)
    matched = bool(value) and re.match(pattern, str(value)) is not None
    return {
        "name": f"pattern_{field}",
        "passed": matched,
        "severity": HARD_FAIL,
        "details": {"field": field, "pattern": pattern, "value_present": value is not None},
    }


def check_type(record: dict[str, Any], field: str, expected_type: type) -> dict[str, Any]:
    """A missing/None value is not itself a type failure — pair with
    check_required_field for fields that must also be present."""
    value = record.get(field)
    passed = value is None or isinstance(value, expected_type)
    return {
        "name": f"type_{field}",
        "passed": passed,
        "severity": HARD_FAIL,
        "details": {"field": field, "expected_type": expected_type.__name__, "actual_type": type(value).__name__},
    }


def check_numeric_range(record: dict[str, Any], field: str, minimum: float | None = None, maximum: float | None = None) -> dict[str, Any]:
    value = record.get(field)
    passed = False
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        passed = (minimum is None or value >= minimum) and (maximum is None or value <= maximum)
    return {
        "name": f"range_{field}",
        "passed": passed,
        "severity": HARD_FAIL,
        "details": {"field": field, "min": minimum, "max": maximum, "value": value},
    }


def check_positive(record: dict[str, Any], field: str) -> dict[str, Any]:
    """Strictly greater than zero — used for exchange_rate per docs/21 §6."""
    value = record.get(field)
    passed = isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0
    return {"name": f"positive_{field}", "passed": passed, "severity": HARD_FAIL, "details": {"field": field, "value": value}}


def check_uniqueness(records: list[dict[str, Any]], key_field: str) -> dict[str, Any]:
    """Batch-level check across a full record set (e.g. one Silver run's output)."""
    keys = [r.get(key_field) for r in records if key_field in r]
    duplicate_count = len(keys) - len(set(keys))
    return {
        "name": f"uniqueness_{key_field}",
        "passed": duplicate_count == 0,
        "severity": HARD_FAIL,
        "details": {"key_field": key_field, "duplicate_count": duplicate_count},
    }


def check_referential_integrity(record: dict[str, Any], field: str, valid_values: set[Any]) -> dict[str, Any]:
    """A NULL foreign-key value is not itself a referential violation — only a
    populated-but-unmatched value is (WARNING per docs/21 §6, not blocking)."""
    value = record.get(field)
    passed = value is None or value in valid_values
    return {"name": f"referential_{field}", "passed": passed, "severity": WARNING, "details": {"field": field, "value": value}}


def dedupe_keep_first(records: list[dict[str, Any]], key_field: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Deterministic first-occurrence-wins deduplication. Returns (kept, duplicates).
    Used defensively by datasets whose contract expects uniqueness already (mcc,
    country, card_issuer — all confirmed 0 duplicates at Bronze) but must still
    handle the case safely rather than assume it. Not used by iso_currency, whose
    contract defines a different, specific merge rule (first non-null field wins,
    not first-row-wins) — see src/silver/iso_currency.py.
    """
    seen: set[Any] = set()
    kept: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    for r in records:
        k = r.get(key_field)
        if k in seen:
            duplicates.append(r)
        else:
            seen.add(k)
            kept.append(r)
    return kept, duplicates


def classify(checks: list[dict[str, Any]]) -> dict[str, Any]:
    """Combine per-record checks into a verdict. Any failed HARD_FAIL check rejects
    the record; only WARNING failures still accept it, with the warnings recorded
    for visibility."""
    failed_hard = [c for c in checks if not c["passed"] and c["severity"] == HARD_FAIL]
    failed_warnings = [c for c in checks if not c["passed"] and c["severity"] == WARNING]
    return {
        "accepted": len(failed_hard) == 0,
        "hard_failures": failed_hard,
        "warnings": failed_warnings,
        "checks": checks,
    }
