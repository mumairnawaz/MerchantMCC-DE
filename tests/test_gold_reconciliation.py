"""S14 — Gold reconciliation (src/gold/reconciliation.py). Reuses
src.silver.common.reconcile_counts (RuntimeError on mismatch) plus the
control-total identity, computed fresh from Decimal fact data every time
(never a hardcoded literal) — positive and negative cases.
"""

from decimal import Decimal

import pytest

from src.gold.reconciliation import (
    compute_control_totals,
    reconcile_event_log_fact,
    reconcile_fact_reconciliation,
    reconcile_fact_settlements,
    reconcile_fact_transactions,
    verify_control_total_identity,
)


def test_reconcile_fact_transactions_passes_on_matching_counts():
    result = reconcile_fact_transactions(3, [{"a": 1}, {"a": 2}, {"a": 3}])
    assert result["passed"] is True


def test_reconcile_fact_transactions_raises_on_mismatch():
    with pytest.raises(RuntimeError):
        reconcile_fact_transactions(5, [{"a": 1}, {"a": 2}])


def test_reconcile_fact_settlements_raises_on_mismatch():
    with pytest.raises(RuntimeError):
        reconcile_fact_settlements(2, [{"a": 1}])


def test_reconcile_fact_reconciliation_raises_on_mismatch():
    with pytest.raises(RuntimeError):
        reconcile_fact_reconciliation(2, [{"a": 1}])


def test_reconcile_event_log_fact_passes_when_deletes_account_for_the_gap():
    result = reconcile_event_log_fact(10, [{"a": i} for i in range(8)], delete_marker_count=2, label="fact_x")
    assert result["passed"] is True


def test_reconcile_event_log_fact_raises_when_gap_is_unexplained():
    with pytest.raises(RuntimeError):
        reconcile_event_log_fact(10, [{"a": i} for i in range(8)], delete_marker_count=1, label="fact_x")


def test_compute_control_totals_sums_only_approved_transactions():
    fact_transactions = [
        {"amount": Decimal("100.00"), "transaction_status": "APPROVED"},
        {"amount": Decimal("50.00"), "transaction_status": "DECLINED"},
        {"amount": Decimal("25.00"), "transaction_status": "APPROVED"},
    ]
    fact_settlements = [{"settlement_amount": Decimal("125.00")}]
    fact_reconciliation = [{"expected_amount": Decimal("125.00")}]
    totals = compute_control_totals(fact_transactions, fact_settlements, fact_reconciliation)
    assert totals["approved_transaction_total"] == Decimal("125.00")
    assert totals["settlement_total"] == Decimal("125.00")
    assert totals["reconciliation_expected_total"] == Decimal("125.00")


def test_verify_control_total_identity_passes_when_all_three_match():
    verify_control_total_identity(
        {
            "approved_transaction_total": Decimal("100.00"),
            "settlement_total": Decimal("100.00"),
            "reconciliation_expected_total": Decimal("100.00"),
        }
    )  # no raise


def test_verify_control_total_identity_raises_on_mismatch():
    with pytest.raises(RuntimeError):
        verify_control_total_identity(
            {
                "approved_transaction_total": Decimal("100.00"),
                "settlement_total": Decimal("99.99"),
                "reconciliation_expected_total": Decimal("100.00"),
            }
        )


def test_control_totals_preserve_decimal_type_not_float():
    fact_transactions = [{"amount": Decimal("10.10"), "transaction_status": "APPROVED"}]
    totals = compute_control_totals(fact_transactions, [], [])
    assert isinstance(totals["approved_transaction_total"], Decimal)
