"""S16 — tests for src/spark/bronze_to_silver.py, the Spark learning
pipeline's transformation logic.

Uses pytest.importorskip("pyspark") deliberately: this project's main
venv (.venv, Windows) has no PySpark installed - only the separate WSL
venv (.venv-spark) does, per docs/29/30's documented reasoning for keeping
Databricks/Spark tooling isolated from the core pipeline's environment.
Running these tests from the main venv SKIPS them cleanly (not a failure);
running them from .venv-spark (WSL) actually executes and validates the
real logic. This keeps the existing 735-test baseline's pass/fail status
completely unaffected either way.

Small, deterministic in-memory test data throughout - no dependency on the
1,010-row generated Bronze CSV.
"""

import pytest

pytest.importorskip("pyspark")

from decimal import Decimal

from pyspark.sql import SparkSession

from src.spark.bronze_to_silver import (
    BRONZE_SCHEMA,
    add_validity_flags,
    build_rejected,
    dedupe_and_route,
    normalize_silver,
    split_valid_and_column_rejected,
)


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test-bronze-to-silver").master("local[1]").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _row(**overrides):
    base = {
        "transaction_id": "TXN-000001",
        "merchant_id": "MER-0001",
        "merchant_name": "Test Merchant",
        "mcc": "5812",
        "currency_code": "GBP",
        "amount": "42.50",
        "transaction_timestamp": "2026-07-01T10:00:00",
        "transaction_status": "APPROVED",
        "card_network": "VISA",
        "country_code": "GB",
    }
    base.update(overrides)
    return tuple(base[f.name] for f in BRONZE_SCHEMA.fields)


def _make_df(spark, rows):
    return spark.createDataFrame(list(rows), schema=BRONZE_SCHEMA)


def _add_row_seq(df):
    from pyspark.sql.functions import monotonically_increasing_id

    return df.withColumn("_row_seq", monotonically_increasing_id())


# ---- per-column validity rules ----


def test_valid_row_passes_all_checks(spark):
    df = _add_row_seq(_make_df(spark, [_row()]))
    flagged = add_validity_flags(df)
    result = flagged.collect()[0]
    assert result["passes_all_checks"] is True


def test_missing_transaction_id_is_rejected(spark):
    df = _add_row_seq(_make_df(spark, [_row(transaction_id=None)]))
    flagged = add_validity_flags(df)
    column_valid, column_rejected = split_valid_and_column_rejected(flagged)
    assert column_valid.count() == 0
    reasons = column_rejected.collect()[0]["rejection_reason"]
    assert "missing_transaction_id" in reasons


def test_missing_merchant_id_is_rejected(spark):
    df = _add_row_seq(_make_df(spark, [_row(merchant_id=None)]))
    flagged = add_validity_flags(df)
    _, column_rejected = split_valid_and_column_rejected(flagged)
    assert "missing_merchant_id" in column_rejected.collect()[0]["rejection_reason"]


@pytest.mark.parametrize("bad_amount", ["-15.00", "0.00", "N/A", None])
def test_invalid_amount_is_rejected(spark, bad_amount):
    df = _add_row_seq(_make_df(spark, [_row(amount=bad_amount)]))
    flagged = add_validity_flags(df)
    _, column_rejected = split_valid_and_column_rejected(flagged)
    assert column_rejected.count() == 1
    assert "invalid_amount" in column_rejected.collect()[0]["rejection_reason"]


@pytest.mark.parametrize("bad_currency", [None, "XXX", "12"])
def test_invalid_currency_code_is_rejected(spark, bad_currency):
    df = _add_row_seq(_make_df(spark, [_row(currency_code=bad_currency)]))
    flagged = add_validity_flags(df)
    _, column_rejected = split_valid_and_column_rejected(flagged)
    assert column_rejected.count() == 1
    assert "invalid_currency_code" in column_rejected.collect()[0]["rejection_reason"]


@pytest.mark.parametrize("bad_ts", [None, "not-a-date", "2026-13-40T99:99:99"])
def test_malformed_timestamp_is_rejected(spark, bad_ts):
    df = _add_row_seq(_make_df(spark, [_row(transaction_timestamp=bad_ts)]))
    flagged = add_validity_flags(df)
    _, column_rejected = split_valid_and_column_rejected(flagged)
    assert column_rejected.count() == 1
    assert "malformed_transaction_timestamp" in column_rejected.collect()[0]["rejection_reason"]


@pytest.mark.parametrize("bad_status", [None, "UNKNOWN_STATE"])
def test_invalid_status_is_rejected(spark, bad_status):
    df = _add_row_seq(_make_df(spark, [_row(transaction_status=bad_status)]))
    flagged = add_validity_flags(df)
    _, column_rejected = split_valid_and_column_rejected(flagged)
    assert column_rejected.count() == 1
    assert "invalid_transaction_status" in column_rejected.collect()[0]["rejection_reason"]


def test_row_can_fail_multiple_checks_at_once(spark):
    df = _add_row_seq(_make_df(spark, [_row(transaction_id=None, amount="N/A")]))
    flagged = add_validity_flags(df)
    _, column_rejected = split_valid_and_column_rejected(flagged)
    reasons = column_rejected.collect()[0]["rejection_reason"]
    assert "missing_transaction_id" in reasons
    assert "invalid_amount" in reasons


# ---- duplicate handling ----


def test_duplicate_transaction_id_keeps_first_and_rejects_rest(spark):
    df = _add_row_seq(_make_df(spark, [_row(amount="10.00"), _row(amount="20.00"), _row(amount="30.00")]))
    flagged = add_validity_flags(df)
    column_valid, _ = split_valid_and_column_rejected(flagged)
    deduped, duplicates = dedupe_and_route(column_valid)

    assert deduped.count() == 1
    assert duplicates.count() == 2
    # first occurrence (by original row order) is the one kept
    assert deduped.collect()[0]["amount"] == "10.00"
    for row in duplicates.collect():
        assert row["rejection_reason"] == "duplicate_transaction_id"


def test_distinct_transaction_ids_are_not_treated_as_duplicates(spark):
    df = _add_row_seq(_make_df(spark, [_row(transaction_id="TXN-A"), _row(transaction_id="TXN-B")]))
    flagged = add_validity_flags(df)
    column_valid, _ = split_valid_and_column_rejected(flagged)
    deduped, duplicates = dedupe_and_route(column_valid)
    assert deduped.count() == 2
    assert duplicates.count() == 0


# ---- normalization + final schema ----


def test_normalize_silver_casts_and_normalizes_casing(spark):
    df = _add_row_seq(_make_df(spark, [_row(currency_code="gbp", transaction_status="approved", card_network="visa", country_code="gb")]))
    flagged = add_validity_flags(df)
    column_valid, _ = split_valid_and_column_rejected(flagged)
    deduped, _ = dedupe_and_route(column_valid)
    silver = normalize_silver(deduped)
    row = silver.collect()[0]

    assert row["currency_code"] == "GBP"
    assert row["transaction_status"] == "APPROVED"
    assert row["card_network"] == "VISA"
    assert row["country_code"] == "GB"
    assert row["amount"] == Decimal("42.50")
    assert row["transaction_timestamp"] is not None
    assert row["ingestion_date"] is not None
    assert row["processed_at"] is not None


def test_silver_schema_has_expected_columns(spark):
    df = _add_row_seq(_make_df(spark, [_row()]))
    flagged = add_validity_flags(df)
    column_valid, _ = split_valid_and_column_rejected(flagged)
    deduped, _ = dedupe_and_route(column_valid)
    silver = normalize_silver(deduped)
    assert silver.columns == [
        "transaction_id", "merchant_id", "merchant_name", "mcc", "currency_code", "amount",
        "transaction_timestamp", "transaction_status", "card_network", "country_code",
        "ingestion_date", "processed_at",
    ]


def test_rejected_output_preserves_raw_values_not_normalized(spark):
    df = _add_row_seq(_make_df(spark, [_row(transaction_id=None, currency_code="gbp")]))
    flagged = add_validity_flags(df)
    column_valid, column_rejected = split_valid_and_column_rejected(flagged)
    _, duplicates = dedupe_and_route(column_valid)
    rejected = build_rejected(column_rejected, duplicates)
    row = rejected.collect()[0]
    # raw value preserved exactly (not uppercased) - rejected output is for
    # diagnosing what came in wrong, not for consumption like Silver is
    assert row["currency_code"] == "gbp"


# ---- end-to-end reconciliation ----


def test_every_row_lands_in_exactly_one_output(spark):
    rows = [
        _row(transaction_id="TXN-A"),
        _row(transaction_id="TXN-B", amount="N/A"),
        _row(transaction_id="TXN-C", currency_code=None),
        _row(transaction_id="TXN-D"),
        _row(transaction_id="TXN-D"),  # exact duplicate of the row above
    ]
    df = _add_row_seq(_make_df(spark, rows))
    flagged = add_validity_flags(df)
    column_valid, column_rejected = split_valid_and_column_rejected(flagged)
    deduped, duplicates = dedupe_and_route(column_valid)
    silver = normalize_silver(deduped)
    rejected = build_rejected(column_rejected, duplicates)

    assert df.count() == silver.count() + rejected.count()
    assert silver.count() == 2  # TXN-A and one of the two TXN-D rows
    assert rejected.count() == 3  # TXN-B (invalid_amount), TXN-C (invalid_currency_code), one TXN-D (duplicate)
