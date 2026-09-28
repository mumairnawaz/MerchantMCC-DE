"""Debezium-native value converters — verified against real CDC Bronze data
(docs/26 §2), not assumed from Debezium's documentation. Real, confirmed
encodings for this deployment:

  - NUMERIC columns  -> decimal strings ("42.00")   — decimal.handling.mode=string (S11)
  - DATE columns     -> int32 days since epoch       — io.debezium.time.Date
  - TIMESTAMP columns -> int64 microseconds since epoch — io.debezium.time.MicroTimestamp
  - everything else  -> plain JSON string/bool/null

These mirror src/oltp/loader.py's own float64/ISO-string -> Postgres-type
conversions (S10), just for the opposite direction and a different source
encoding (Debezium's Kafka Connect logical types, not Parquet).
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

EPOCH_DATE = date(1970, 1, 1)


def debezium_date_to_date(value: Any) -> date | None:
    if value is None:
        return None
    try:
        return EPOCH_DATE + timedelta(days=int(value))
    except (TypeError, ValueError):
        return None


def debezium_micros_to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(value))
    except (TypeError, ValueError, OverflowError):
        return None


def debezium_decimal_string_to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def as_string(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
