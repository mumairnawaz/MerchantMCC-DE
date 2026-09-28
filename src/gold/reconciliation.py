"""Gold-level reconciliation (§22/§23 of this phase). Reuses
src.silver.common.reconcile_counts — the SAME generic mechanism used by
every prior layer (Silver, CDC Bronze, CDC Silver) — rather than a new
framework, per repeated explicit project instruction.

Two distinct kinds of check happen here:
  1. Row-count reconciliation per fact: source Silver rows = Gold fact rows
     + documented exclusions (only the event_log delete-marker exclusion,
     see facts.py). Every other fact is a 1:1 row carry-over.
  2. The control-total identity (§14, never hardcoded): approved transaction
     total == settlement total == reconciliation expected total. This is
     computed fresh from Gold fact data every run, not compared against the
     literal figure written in any prior phase's docs.
"""

from decimal import Decimal
from typing import Any

from src.silver.common import reconcile_counts


def reconcile_fact_transactions(silver_row_count: int, fact_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return reconcile_counts(silver_row_count, {"gold_fact_transactions": len(fact_rows)}, label="gold.fact_transactions")


def reconcile_fact_settlements(silver_row_count: int, fact_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return reconcile_counts(silver_row_count, {"gold_fact_settlements": len(fact_rows)}, label="gold.fact_settlements")


def reconcile_fact_reconciliation(silver_row_count: int, fact_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return reconcile_counts(silver_row_count, {"gold_fact_reconciliation": len(fact_rows)}, label="gold.fact_reconciliation")


def reconcile_event_log_fact(silver_row_count: int, fact_rows: list[dict[str, Any]], delete_marker_count: int, *, label: str) -> dict[str, Any]:
    """Event-log facts exclude operation='d' delete markers (facts.py) — the
    ONLY documented exclusion in this phase. Verifies the identity exactly:
    silver_row_count == len(fact_rows) + delete_marker_count."""
    return reconcile_counts(
        silver_row_count,
        {f"gold_{label}": len(fact_rows), "excluded_delete_markers": delete_marker_count},
        label=f"gold.{label}",
    )


def compute_control_totals(
    fact_transactions: list[dict[str, Any]],
    fact_settlements: list[dict[str, Any]],
    fact_reconciliation: list[dict[str, Any]],
) -> dict[str, Decimal]:
    """The control-total identity (§14): approved transaction amount total ==
    settlement amount total == reconciliation expected-amount total. Computed
    fresh from Gold fact Decimal columns every run — never a hardcoded
    literal anywhere in this module."""
    approved_transaction_total = sum(
        (r["amount"] for r in fact_transactions if r["transaction_status"] == "APPROVED"),
        Decimal("0"),
    )
    settlement_total = sum((r["settlement_amount"] for r in fact_settlements), Decimal("0"))
    reconciliation_expected_total = sum((r["expected_amount"] for r in fact_reconciliation), Decimal("0"))
    return {
        "approved_transaction_total": approved_transaction_total,
        "settlement_total": settlement_total,
        "reconciliation_expected_total": reconciliation_expected_total,
    }


def verify_control_total_identity(totals: dict[str, Decimal]) -> None:
    a = totals["approved_transaction_total"]
    s = totals["settlement_total"]
    r = totals["reconciliation_expected_total"]
    if not (a == s == r):
        raise RuntimeError(
            "Gold control-total identity violated: "
            f"approved_transaction_total={a}, settlement_total={s}, "
            f"reconciliation_expected_total={r}"
        )
