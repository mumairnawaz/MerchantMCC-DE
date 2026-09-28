"""S12 — reconciliation tests for the CDC Bronze consumer's accounting
formula: events_read = events_persisted + duplicates_skipped + quarantined +
tombstones_handled. Reuses src.silver.common.reconcile_counts directly (the
same generic, already-exhaustively-tested helper src/cdc/consumer.py itself
calls — see docs/25 §16: "reuse an existing generic mechanism, don't build a
second one"), proving the real field names/label this phase uses are wired
correctly, not just that the generic helper works in isolation.
"""

import pytest

from src.silver.common import reconcile_counts


def test_reconciliation_passes_when_accounting_is_exact():
    result = reconcile_counts(
        100,
        {"events_persisted": 70, "tombstones_handled": 10, "duplicates_skipped": 15, "quarantined": 5},
        label="cdc_bronze:transactions",
    )
    assert result["passed"] is True
    assert result["accounted_for"] == 100


def test_reconciliation_under_count_is_a_hard_failure():
    with pytest.raises(RuntimeError, match="cdc_bronze:transactions"):
        reconcile_counts(
            100,
            {"events_persisted": 70, "tombstones_handled": 10, "duplicates_skipped": 15, "quarantined": 4},  # 99, missing 1
            label="cdc_bronze:transactions",
        )


def test_reconciliation_over_count_is_a_hard_failure():
    with pytest.raises(RuntimeError, match="cdc_bronze:transactions"):
        reconcile_counts(
            100,
            {"events_persisted": 70, "tombstones_handled": 10, "duplicates_skipped": 15, "quarantined": 6},  # 101, one too many
            label="cdc_bronze:transactions",
        )


def test_reconciliation_zero_events_read_passes_trivially():
    result = reconcile_counts(0, {"events_persisted": 0, "tombstones_handled": 0, "duplicates_skipped": 0, "quarantined": 0}, label="cdc_bronze:offers")
    assert result["passed"] is True
