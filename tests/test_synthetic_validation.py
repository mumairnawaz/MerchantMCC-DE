"""S9 — negative-fixture DQ tests. The primary generated dataset (tested in
tests/test_synthetic_generation.py) is expected to be clean by construction —
these tests deliberately corrupt ONE field of a copy of that valid baseline and
prove src.synthetic.validation.validate_all() rejects it, per the S9 instruction
to keep negative fixtures separate from the primary generated dataset.
"""

import copy

import pytest

from src.synthetic import pipeline, reference
from src.synthetic.validation import SyntheticDataQualityError, validate_all


@pytest.fixture(scope="module")
def ref():
    return reference.load_reference()


@pytest.fixture(scope="module")
def baseline(ref):
    result = pipeline.run(write_output=False)
    return result["tables"]


def _mutate(tables: dict, table_name: str, index: int, **overrides) -> dict:
    """A deep-enough copy: only the target table's record list is copied, and
    only the target row within it — every other table/row is shared (fast,
    since we're not mutating them)."""
    new_tables = dict(tables)
    new_list = list(tables[table_name])
    new_list[index] = dict(new_list[index], **overrides)
    new_tables[table_name] = new_list
    return new_tables


def test_valid_baseline_passes(baseline, ref):
    assert validate_all(baseline, ref)["passed"] is True


def test_duplicate_primary_key_is_rejected(baseline, ref):
    broken = _mutate(baseline, "clients", 1, client_id=baseline["clients"][0]["client_id"])
    with pytest.raises(SyntheticDataQualityError, match="duplicate"):
        validate_all(broken, ref)


def test_duplicate_settlement_transaction_id_is_rejected(baseline, ref):
    broken = _mutate(baseline, "settlements", 1, transaction_id=baseline["settlements"][0]["transaction_id"])
    with pytest.raises(SyntheticDataQualityError):
        validate_all(broken, ref)


def test_invalid_merchant_reference_is_rejected(baseline, ref):
    broken = _mutate(baseline, "transactions", 0, merchant_id="node:not-a-real-merchant")
    with pytest.raises(SyntheticDataQualityError, match="merchant_id"):
        validate_all(broken, ref)


def test_invalid_mcc_reference_is_rejected(baseline, ref):
    broken = _mutate(baseline, "transactions", 0, mcc_code="9999")
    with pytest.raises(SyntheticDataQualityError, match="mcc_code"):
        validate_all(broken, ref)


def test_invalid_currency_reference_is_rejected(baseline, ref):
    broken = _mutate(baseline, "transactions", 0, currency_code="ZZZ")
    with pytest.raises(SyntheticDataQualityError, match="currency_code"):
        validate_all(broken, ref)


def test_invalid_client_lei_reference_is_rejected(baseline, ref):
    broken = _mutate(baseline, "clients", 0, lei="00000000000000000000")
    with pytest.raises(SyntheticDataQualityError, match="lei"):
        validate_all(broken, ref)


def test_non_positive_transaction_amount_is_rejected(baseline, ref):
    broken = _mutate(baseline, "transactions", 0, amount=0.0)
    with pytest.raises(SyntheticDataQualityError, match="amount"):
        validate_all(broken, ref)


def test_negative_transaction_amount_is_rejected(baseline, ref):
    broken = _mutate(baseline, "transactions", 0, amount=-5.0)
    with pytest.raises(SyntheticDataQualityError, match="amount"):
        validate_all(broken, ref)


def test_missing_synthetic_tag_is_rejected(baseline, ref):
    broken = _mutate(baseline, "clients", 0, source_type="real")
    with pytest.raises(SyntheticDataQualityError, match="source_type"):
        validate_all(broken, ref)


def test_settlement_amount_mismatch_is_rejected(baseline, ref):
    broken = _mutate(baseline, "settlements", 0, settlement_amount=baseline["settlements"][0]["settlement_amount"] + 100)
    with pytest.raises(SyntheticDataQualityError, match="settlement"):
        validate_all(broken, ref)


def test_settlement_net_amount_arithmetic_break_is_rejected(baseline, ref):
    broken = _mutate(baseline, "settlements", 0, net_amount=999999.99)
    with pytest.raises(SyntheticDataQualityError, match="net_amount"):
        validate_all(broken, ref)


def test_reconciliation_variance_arithmetic_break_is_rejected(baseline, ref):
    broken = _mutate(baseline, "reconciliation", 0, variance=999.99)
    with pytest.raises(SyntheticDataQualityError, match="variance"):
        validate_all(broken, ref)


def test_reconciliation_matched_with_nonzero_variance_is_rejected(baseline, ref):
    matched = next(i for i, r in enumerate(baseline["reconciliation"]) if r["match_status"] == "MATCHED")
    broken = _mutate(baseline, "reconciliation", matched, actual_amount=baseline["reconciliation"][matched]["expected_amount"] + 5, variance=5)
    with pytest.raises(SyntheticDataQualityError, match="MATCHED"):
        validate_all(broken, ref)


def test_reconciliation_exception_with_zero_variance_is_rejected(baseline, ref):
    exception_idx = next(i for i, r in enumerate(baseline["reconciliation"]) if r["match_status"] == "EXCEPTION")
    row = baseline["reconciliation"][exception_idx]
    broken = _mutate(baseline, "reconciliation", exception_idx, actual_amount=row["expected_amount"], variance=0)
    with pytest.raises(SyntheticDataQualityError, match="EXCEPTION"):
        validate_all(broken, ref)


def test_reward_qualified_with_zero_amount_is_rejected(baseline, ref):
    idx = next(i for i, r in enumerate(baseline["reward_events"]) if r["qualification_status"] == "QUALIFIED")
    broken = _mutate(baseline, "reward_events", idx, reward_amount=0.0)
    with pytest.raises(SyntheticDataQualityError, match="QUALIFIED"):
        validate_all(broken, ref)


def test_reward_not_qualified_with_nonzero_amount_is_rejected(baseline, ref):
    idx = next(i for i, r in enumerate(baseline["reward_events"]) if r["qualification_status"] == "NOT_QUALIFIED")
    broken = _mutate(baseline, "reward_events", idx, reward_amount=12.34)
    with pytest.raises(SyntheticDataQualityError, match="NOT_QUALIFIED"):
        validate_all(broken, ref)


def test_declined_transaction_with_capture_event_is_rejected(baseline, ref):
    declined_txn = next(t for t in baseline["transactions"] if t["status"] == "DECLINED")
    fake_capture = {
        "event_id": "EVT-9999999",
        "transaction_id": declined_txn["transaction_id"],
        "event_type": "CAPTURE",
        "event_timestamp": declined_txn["transaction_timestamp"],
        "event_status": "CAPTURED",
        "source_type": "synthetic",
    }
    new_tables = dict(baseline)
    new_tables["transaction_events"] = baseline["transaction_events"] + [fake_capture]
    with pytest.raises(SyntheticDataQualityError, match="CAPTURE"):
        validate_all(new_tables, ref)


def test_transaction_missing_authorization_event_is_rejected(baseline, ref):
    victim = baseline["transactions"][0]
    new_tables = dict(baseline)
    new_tables["transaction_events"] = [e for e in baseline["transaction_events"] if not (e["transaction_id"] == victim["transaction_id"] and e["event_type"] == "AUTHORIZATION")]
    with pytest.raises(SyntheticDataQualityError, match="AUTHORIZATION"):
        validate_all(new_tables, ref)


def test_orphaned_settlement_transaction_reference_is_rejected(baseline, ref):
    broken = _mutate(baseline, "settlements", 0, transaction_id="TXN-9999999")
    with pytest.raises(SyntheticDataQualityError, match="transaction_id"):
        validate_all(broken, ref)


def test_orphaned_reconciliation_settlement_reference_is_rejected(baseline, ref):
    broken = _mutate(baseline, "reconciliation", 0, settlement_id="STL-9999999")
    with pytest.raises(SyntheticDataQualityError, match="settlement_id"):
        validate_all(broken, ref)


def test_invalid_program_owner_reference_is_rejected(baseline, ref):
    issuer_client = next(c for c in baseline["clients"] if c["client_type"] != "PROGRAM_OWNER")
    broken = _mutate(baseline, "programs", 0, client_id=issuer_client["client_id"])
    with pytest.raises(SyntheticDataQualityError, match="client_id"):
        validate_all(broken, ref)
