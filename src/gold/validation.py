"""Gold-level data quality validation (§16 of this phase) — focused on
ANALYTICAL-MODEL integrity, not re-checking business-column rules already
enforced by API Silver / CDC Silver (that would duplicate, not add, value).
Checks here are specific to what a dimensional model can uniquely break:
  - surrogate-key uniqueness within a dimension (registry corruption)
  - natural-key uniqueness within a dimension (a real-world duplicate that
    slipped through Silver would otherwise silently produce two surrogate
    keys for one business entity)
  - fact grain (no two fact rows share the same grain-defining key)
  - FK integrity (every non-UNKNOWN fact FK must resolve to a real dimension
    row — an orphan means a join bug, not a business condition)
"""

from typing import Any

from src.gold.config import UNKNOWN_KEY


class GoldDataQualityError(RuntimeError):
    pass


def _fail(message: str) -> None:
    raise GoldDataQualityError(f"gold DQ violation: {message}")


def validate_surrogate_key_uniqueness(dimension_name: str, rows: list[dict[str, Any]], key_field: str) -> None:
    keys = [r[key_field] for r in rows]
    if len(keys) != len(set(keys)):
        dupes = {k for k in keys if keys.count(k) > 1}
        _fail(f"{dimension_name}: duplicate {key_field} values: {dupes}")


def validate_natural_key_uniqueness(dimension_name: str, rows: list[dict[str, Any]], natural_key_field: str) -> None:
    keys = [r[natural_key_field] for r in rows]
    if len(keys) != len(set(keys)):
        dupes = {k for k in keys if keys.count(k) > 1}
        _fail(f"{dimension_name}: duplicate {natural_key_field} values: {dupes}")


def validate_fact_grain(fact_name: str, rows: list[dict[str, Any]], grain_fields: tuple[str, ...]) -> None:
    keys = [tuple(r[f] for f in grain_fields) for r in rows]
    if len(keys) != len(set(keys)):
        dupes = {k for k in keys if keys.count(k) > 1}
        _fail(f"{fact_name}: grain {grain_fields} violated — repeated keys: {dupes}")


def validate_fk_integrity(fact_name: str, rows: list[dict[str, Any]], fk_field: str, valid_keys: set) -> None:
    """UNKNOWN_KEY (-1) is always valid by design (§10) — it is the
    documented unknown-member row, not an orphan."""
    orphans = {r[fk_field] for r in rows if r[fk_field] != UNKNOWN_KEY and r[fk_field] not in valid_keys}
    if orphans:
        _fail(f"{fact_name}.{fk_field}: orphan surrogate keys with no matching dimension row: {orphans}")


def validate_no_null_measures(fact_name: str, rows: list[dict[str, Any]], measure_fields: tuple[str, ...]) -> None:
    for field in measure_fields:
        if any(r[field] is None for r in rows):
            _fail(f"{fact_name}.{field}: NULL value found in a required additive measure")


def validate_dimensions(dimensions: dict[str, tuple[list[dict[str, Any]], str, str]]) -> None:
    """dimensions: {dimension_name: (rows, surrogate_key_field, natural_key_field)}"""
    for name, (rows, sk_field, nk_field) in dimensions.items():
        validate_surrogate_key_uniqueness(name, rows, sk_field)
        validate_natural_key_uniqueness(name, rows, nk_field)


def validate_facts(
    facts: dict[str, tuple[list[dict[str, Any]], tuple[str, ...]]],
    fk_checks: list[tuple[str, str, str, set]],
) -> None:
    """facts: {fact_name: (rows, grain_fields)}.
    fk_checks: list of (fact_name, fk_field, dimension_name_for_error, valid_keys)."""
    for name, (rows, grain_fields) in facts.items():
        validate_fact_grain(name, rows, grain_fields)
    for fact_name, fk_field, _dim_label, valid_keys in fk_checks:
        rows = facts[fact_name][0]
        validate_fk_integrity(fact_name, rows, fk_field, valid_keys)
