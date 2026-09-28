"""S13 — reconciliation tests for CDC Silver's accounting formula:
events_read = accepted + deleted + stale_skipped + duplicates_skipped +
quarantined + tombstones_handled. Reuses src.silver.common.reconcile_counts
directly (the same helper src/cdc/silver.py itself calls), proving the real
field names/label this phase uses are wired correctly — not re-testing the
generic helper itself (already exhaustively tested since S7).
"""

import pytest

from src.silver.common import reconcile_counts


def test_cdc_silver_reconciliation_passes_when_exact():
    result = reconcile_counts(
        100,
        {"accepted": 60, "deleted": 10, "stale_skipped": 5, "duplicates_skipped": 15, "quarantined": 5, "tombstones_handled": 5},
        label="cdc_silver:silver_cdc_transaction",
    )
    assert result["passed"] is True
    assert result["accounted_for"] == 100


def test_cdc_silver_reconciliation_under_count_hard_fails():
    with pytest.raises(RuntimeError, match="cdc_silver:silver_cdc_transaction"):
        reconcile_counts(
            100,
            {"accepted": 60, "deleted": 10, "stale_skipped": 5, "duplicates_skipped": 15, "quarantined": 5, "tombstones_handled": 4},
            label="cdc_silver:silver_cdc_transaction",
        )


def test_cdc_silver_reconciliation_over_count_hard_fails():
    with pytest.raises(RuntimeError, match="cdc_silver:silver_cdc_transaction"):
        reconcile_counts(
            100,
            {"accepted": 60, "deleted": 10, "stale_skipped": 5, "duplicates_skipped": 15, "quarantined": 5, "tombstones_handled": 6},
            label="cdc_silver:silver_cdc_transaction",
        )
