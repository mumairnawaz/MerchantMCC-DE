"""S13 — unit tests for src/cdc/silver_types.py, verified against the real
Debezium encodings independently confirmed in this phase (docs/26 §2):
NUMERIC -> decimal string, DATE -> epoch days (int32), TIMESTAMP -> epoch
microseconds (int64).
"""

from datetime import date, datetime, timezone
from decimal import Decimal

from src.cdc.silver_types import as_string, debezium_date_to_date, debezium_decimal_string_to_decimal, debezium_micros_to_datetime


def test_debezium_date_real_value_from_inspected_data():
    # 19955 was the real onboarding_date observed for CLI-0001 in S12/S13 inspection
    assert debezium_date_to_date(19955) == date(2024, 8, 20)


def test_debezium_date_none_stays_none():
    assert debezium_date_to_date(None) is None


def test_debezium_date_epoch_zero_is_1970_01_01():
    assert debezium_date_to_date(0) == date(1970, 1, 1)


def test_debezium_micros_real_value_from_inspected_data():
    # 1783955166000000 was the real transaction_timestamp observed for TXN-0000001
    result = debezium_micros_to_datetime(1783955166000000)
    assert result == datetime(2026, 7, 13, 15, 6, 6, tzinfo=timezone.utc)


def test_debezium_micros_none_stays_none():
    assert debezium_micros_to_datetime(None) is None


def test_debezium_decimal_string_preserves_exact_value():
    assert debezium_decimal_string_to_decimal("173.53") == Decimal("173.53")


def test_debezium_decimal_string_none_stays_none():
    assert debezium_decimal_string_to_decimal(None) is None


def test_debezium_decimal_string_malformed_becomes_none_not_a_crash():
    assert debezium_decimal_string_to_decimal("not-a-number") is None


def test_as_string_passthrough():
    assert as_string("ACTIVE") == "ACTIVE"
    assert as_string(None) is None
