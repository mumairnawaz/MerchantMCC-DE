"""S13 — unit tests for src/cdc/silver.py's typed extraction and validation."""

from decimal import Decimal

from src.cdc.silver import _extract_typed_record, _validate_typed_record
from src.cdc.silver_tables import TABLE_SPECS

TXN_SPEC = TABLE_SPECS["transactions"]

VALID_RAW = {
    "transaction_id": "TXN-1",
    "token_id": "TKN-1",
    "merchant_id": "node:1",
    "mcc_code": "5999",
    "mcc_confidence": "0.50",
    "currency_code": "GBP",
    "amount": "42.00",
    "transaction_timestamp": 1783955166000000,
    "status": "APPROVED",
    "decline_reason": None,
    "auth_code": "ABC123",
    "source_type": "synthetic",
}


def test_extract_converts_numeric_string_to_decimal():
    typed = _extract_typed_record(TXN_SPEC.columns, VALID_RAW)
    assert typed["amount"] == Decimal("42.00")
    assert isinstance(typed["amount"], Decimal)


def test_extract_converts_epoch_micros_to_datetime():
    typed = _extract_typed_record(TXN_SPEC.columns, VALID_RAW)
    assert typed["transaction_timestamp"].year == 2026


def test_extract_nested_json_field_not_dropped():
    typed = _extract_typed_record(TXN_SPEC.columns, VALID_RAW)
    assert typed["merchant_id"] == "node:1"


def test_valid_record_passes():
    typed = _extract_typed_record(TXN_SPEC.columns, VALID_RAW)
    ok, reason = _validate_typed_record(typed, TXN_SPEC)
    assert ok is True
    assert reason is None


def test_missing_required_field_is_rejected():
    raw = dict(VALID_RAW, currency_code=None)
    typed = _extract_typed_record(TXN_SPEC.columns, raw)
    ok, reason = _validate_typed_record(typed, TXN_SPEC)
    assert ok is False
    assert reason == "required_field_currency_code"


def test_non_positive_amount_is_rejected():
    raw = dict(VALID_RAW, amount="-5.00")
    typed = _extract_typed_record(TXN_SPEC.columns, raw)
    ok, reason = _validate_typed_record(typed, TXN_SPEC)
    assert ok is False
    assert reason == "positive_amount"


def test_zero_amount_is_rejected():
    raw = dict(VALID_RAW, amount="0.00")
    typed = _extract_typed_record(TXN_SPEC.columns, raw)
    ok, reason = _validate_typed_record(typed, TXN_SPEC)
    assert ok is False
    assert reason == "positive_amount"


def test_invalid_status_enum_is_rejected():
    raw = dict(VALID_RAW, status="BOGUS")
    typed = _extract_typed_record(TXN_SPEC.columns, raw)
    ok, reason = _validate_typed_record(typed, TXN_SPEC)
    assert ok is False
    assert reason == "invalid_value_status"


def test_missing_pk_is_rejected():
    raw = dict(VALID_RAW, transaction_id=None)
    typed = _extract_typed_record(TXN_SPEC.columns, raw)
    ok, reason = _validate_typed_record(typed, TXN_SPEC)
    assert ok is False
    assert reason == "missing_pk_transaction_id"


def test_optional_field_absent_does_not_fail_validation():
    raw = dict(VALID_RAW, decline_reason=None, auth_code=None)
    typed = _extract_typed_record(TXN_SPEC.columns, raw)
    ok, _ = _validate_typed_record(typed, TXN_SPEC)
    assert ok is True


# ---- delete-mode validation (§13 defect fix — only PK matters for a delete) ----


def test_delete_mode_accepts_a_sparse_before_image_with_only_pk_populated():
    """Real historical data (docs/26 §17): some pre-REPLICA-IDENTITY-FULL
    delete events have every non-PK column blank/zero. A delete only needs
    to identify WHICH row to remove — this must still be accepted."""
    sparse_before = {
        "transaction_id": "TXN-1",
        "token_id": "",
        "merchant_id": "",
        "mcc_code": "",
        "mcc_confidence": "0.00",
        "currency_code": "",
        "amount": "0.00",
        "transaction_timestamp": 0,
        "status": "",
        "decline_reason": None,
        "auth_code": None,
        "source_type": "synthetic",
    }
    typed = _extract_typed_record(TXN_SPEC.columns, sparse_before)
    ok, reason = _validate_typed_record(typed, TXN_SPEC, is_delete=True)
    assert ok is True, reason


def test_delete_mode_still_rejects_a_missing_pk():
    sparse_before = dict(VALID_RAW, transaction_id=None)
    typed = _extract_typed_record(TXN_SPEC.columns, sparse_before)
    ok, reason = _validate_typed_record(typed, TXN_SPEC, is_delete=True)
    assert ok is False
    assert reason == "missing_pk_transaction_id"


def test_non_delete_mode_still_fully_validates_business_fields():
    """Proves is_delete=True doesn't accidentally weaken validation for
    r/c/u operations — only deletes get the relaxed check."""
    raw = dict(VALID_RAW, amount="-5.00")
    typed = _extract_typed_record(TXN_SPEC.columns, raw)
    ok, reason = _validate_typed_record(typed, TXN_SPEC, is_delete=False)
    assert ok is False
    assert reason == "positive_amount"
