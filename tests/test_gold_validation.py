"""S14 — Gold-level data quality checks (src/gold/validation.py). Pure
in-memory positive and negative (corruption) cases — no file IO.
"""

import pytest

from src.gold.config import UNKNOWN_KEY
from src.gold.validation import (
    GoldDataQualityError,
    validate_facts,
    validate_fact_grain,
    validate_fk_integrity,
    validate_natural_key_uniqueness,
    validate_surrogate_key_uniqueness,
)


def test_surrogate_key_uniqueness_passes_on_clean_data():
    rows = [{"k": 1}, {"k": 2}, {"k": 3}]
    validate_surrogate_key_uniqueness("dim_x", rows, "k")  # no raise


def test_surrogate_key_uniqueness_fails_on_duplicate():
    rows = [{"k": 1}, {"k": 1}, {"k": 2}]
    with pytest.raises(GoldDataQualityError):
        validate_surrogate_key_uniqueness("dim_x", rows, "k")


def test_natural_key_uniqueness_passes_on_clean_data():
    rows = [{"nk": "A"}, {"nk": "B"}]
    validate_natural_key_uniqueness("dim_x", rows, "nk")


def test_natural_key_uniqueness_fails_on_duplicate():
    rows = [{"nk": "A"}, {"nk": "A"}]
    with pytest.raises(GoldDataQualityError):
        validate_natural_key_uniqueness("dim_x", rows, "nk")


def test_fact_grain_passes_on_unique_composite_key():
    rows = [{"a": 1, "b": "x"}, {"a": 1, "b": "y"}, {"a": 2, "b": "x"}]
    validate_fact_grain("fact_x", rows, ("a", "b"))


def test_fact_grain_fails_on_repeated_composite_key():
    rows = [{"a": 1, "b": "x"}, {"a": 1, "b": "x"}]
    with pytest.raises(GoldDataQualityError):
        validate_fact_grain("fact_x", rows, ("a", "b"))


def test_fk_integrity_passes_when_all_keys_resolve():
    rows = [{"fk": 1}, {"fk": 2}]
    validate_fk_integrity("fact_x", rows, "fk", valid_keys={1, 2, 3})


def test_fk_integrity_allows_unknown_key_without_a_matching_dimension_row():
    rows = [{"fk": UNKNOWN_KEY}]
    validate_fk_integrity("fact_x", rows, "fk", valid_keys={1, 2, 3})  # no raise


def test_fk_integrity_fails_on_true_orphan():
    rows = [{"fk": 999}]
    with pytest.raises(GoldDataQualityError):
        validate_fk_integrity("fact_x", rows, "fk", valid_keys={1, 2, 3})


def test_validate_facts_runs_grain_and_fk_checks_together():
    facts = {
        "fact_x": ([{"id": "A", "fk": 1}, {"id": "B", "fk": UNKNOWN_KEY}], ("id",)),
    }
    fk_checks = [("fact_x", "fk", "dim_y", {1, 2})]
    validate_facts(facts, fk_checks)  # no raise


def test_validate_facts_fails_when_grain_broken_even_if_fk_clean():
    facts = {
        "fact_x": ([{"id": "A", "fk": 1}, {"id": "A", "fk": 2}], ("id",)),
    }
    with pytest.raises(GoldDataQualityError):
        validate_facts(facts, [("fact_x", "fk", "dim_y", {1, 2})])


def test_validate_facts_fails_when_fk_orphaned_even_if_grain_clean():
    facts = {
        "fact_x": ([{"id": "A", "fk": 999}], ("id",)),
    }
    with pytest.raises(GoldDataQualityError):
        validate_facts(facts, [("fact_x", "fk", "dim_y", {1, 2})])
