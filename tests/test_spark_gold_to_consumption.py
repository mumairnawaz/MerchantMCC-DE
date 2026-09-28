"""S16 Step 5 -- tests for src/spark/gold_to_consumption.py.

Same pattern as the Step 3/4 Spark test modules: pytest.importorskip so
this skips cleanly on the main Windows venv and actually runs under WSL's
.venv-spark. Small, deterministic in-memory Gold-shaped DataFrames.
"""

import pytest

pytest.importorskip("pyspark")

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.spark.gold_to_consumption import (
    build_card_network_summary,
    build_country_summary,
    build_currency_summary,
    build_daily_trend,
    build_mcc_summary,
    build_merchant_summary,
    build_status_summary,
    reconcile_mart,
)

FACT_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("merchant_key", IntegerType(), True),
        StructField("date_key", IntegerType(), True),
        StructField("mcc", StringType(), True),
        StructField("currency_code", StringType(), True),
        StructField("card_network", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("transaction_status", StringType(), True),
        StructField("amount", DecimalType(18, 2), True),
        StructField("transaction_timestamp", TimestampType(), True),
        StructField("ingestion_date", DateType(), True),
        StructField("processed_at", TimestampType(), True),
    ]
)

DIM_MERCHANT_SCHEMA = StructType(
    [
        StructField("merchant_key", IntegerType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
    ]
)

DIM_DATE_SCHEMA = StructType(
    [
        StructField("date_key", IntegerType(), True),
        StructField("full_date", DateType(), True),
        StructField("year", IntegerType(), True),
        StructField("quarter", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("month_name", StringType(), True),
        StructField("day", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("day_name", StringType(), True),
        StructField("week_of_year", IntegerType(), True),
        StructField("is_weekend", StringType(), True),
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test-gold-to-consumption").master("local[1]").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _fact_row(**overrides):
    base = {
        "transaction_id": "TXN-000001",
        "merchant_key": 1,
        "date_key": 20260701,
        "mcc": "5812",
        "currency_code": "GBP",
        "card_network": "VISA",
        "country_code": "GB",
        "transaction_status": "APPROVED",
        "amount": Decimal("42.50"),
        "transaction_timestamp": datetime(2026, 7, 1, 10, 0, 0),
        "ingestion_date": date(2026, 9, 23),
        "processed_at": datetime(2026, 9, 23, 12, 0, 0),
    }
    base.update(overrides)
    return tuple(base[f.name] for f in FACT_SCHEMA.fields)


def _make_fact(spark, rows):
    return spark.createDataFrame(list(rows), schema=FACT_SCHEMA)


def _make_dim_merchant(spark, rows):
    return spark.createDataFrame(list(rows), schema=DIM_MERCHANT_SCHEMA)


def _make_dim_date(spark, rows):
    return spark.createDataFrame(list(rows), schema=DIM_DATE_SCHEMA)


# ---- individual mart correctness ----


def test_status_summary_aggregates_correctly(spark):
    fact = _make_fact(
        spark,
        [
            _fact_row(transaction_id="TXN-1", transaction_status="APPROVED", amount=Decimal("10.00")),
            _fact_row(transaction_id="TXN-2", transaction_status="APPROVED", amount=Decimal("20.00")),
            _fact_row(transaction_id="TXN-3", transaction_status="DECLINED", amount=Decimal("5.00")),
        ],
    )
    mart = build_status_summary(fact)
    rows = {r["transaction_status"]: (r["transaction_count"], r["total_amount"]) for r in mart.collect()}
    assert rows["APPROVED"] == (2, Decimal("30.00"))
    assert rows["DECLINED"] == (1, Decimal("5.00"))


def test_merchant_summary_joins_and_aggregates_correctly(spark):
    fact = _make_fact(
        spark,
        [
            _fact_row(transaction_id="TXN-1", merchant_key=1, amount=Decimal("10.00")),
            _fact_row(transaction_id="TXN-2", merchant_key=1, amount=Decimal("15.00")),
            _fact_row(transaction_id="TXN-3", merchant_key=2, amount=Decimal("7.00")),
        ],
    )
    dim_merchant = _make_dim_merchant(spark, [(1, "MER-0001", "Ada's Coffee House"), (2, "MER-0002", "Riverside Grocers")])
    mart = build_merchant_summary(fact, dim_merchant)
    rows = {r["merchant_id"]: (r["transaction_count"], r["total_amount"]) for r in mart.collect()}
    assert rows["MER-0001"] == (2, Decimal("25.00"))
    assert rows["MER-0002"] == (1, Decimal("7.00"))


def test_mcc_currency_card_network_country_summaries_partition_correctly(spark):
    fact = _make_fact(
        spark,
        [
            _fact_row(transaction_id="TXN-1", mcc="5812", currency_code="GBP", card_network="VISA", country_code="GB", amount=Decimal("10.00")),
            _fact_row(transaction_id="TXN-2", mcc="5411", currency_code="USD", card_network="AMEX", country_code="US", amount=Decimal("20.00")),
        ],
    )
    assert {r["mcc"] for r in build_mcc_summary(fact).collect()} == {"5812", "5411"}
    assert {r["currency_code"] for r in build_currency_summary(fact).collect()} == {"GBP", "USD"}
    assert {r["card_network"] for r in build_card_network_summary(fact).collect()} == {"VISA", "AMEX"}
    assert {r["country_code"] for r in build_country_summary(fact).collect()} == {"GB", "US"}


def test_daily_trend_joins_dim_date_and_aggregates_correctly(spark):
    fact = _make_fact(
        spark,
        [
            _fact_row(transaction_id="TXN-1", date_key=20260701, amount=Decimal("10.00")),
            _fact_row(transaction_id="TXN-2", date_key=20260701, amount=Decimal("5.00")),
            _fact_row(transaction_id="TXN-3", date_key=20260702, amount=Decimal("3.00")),
        ],
    )
    dim_date = _make_dim_date(
        spark,
        [
            (20260701, date(2026, 7, 1), 2026, 3, 7, "July", 1, 4, "Wednesday", 27, "false"),
            (20260702, date(2026, 7, 2), 2026, 3, 7, "July", 2, 5, "Thursday", 27, "false"),
        ],
    )
    mart = build_daily_trend(fact, dim_date)
    rows = {r["full_date"]: (r["transaction_count"], r["total_amount"]) for r in mart.collect()}
    assert rows[date(2026, 7, 1)] == (2, Decimal("15.00"))
    assert rows[date(2026, 7, 2)] == (1, Decimal("3.00"))


# ---- reconciliation ----


def test_reconcile_mart_passes_when_totals_match(spark):
    fact = _make_fact(spark, [_fact_row(transaction_id="TXN-1", amount=Decimal("10.00")), _fact_row(transaction_id="TXN-2", amount=Decimal("20.00"))])
    mart = build_status_summary(fact)
    reconcile_mart("status_summary", mart, fact_count=2, fact_total_amount=Decimal("30.00"))  # no raise


def test_reconcile_mart_fails_on_count_mismatch(spark):
    fact = _make_fact(spark, [_fact_row(transaction_id="TXN-1", amount=Decimal("10.00"))])
    mart = build_status_summary(fact)
    with pytest.raises(AssertionError, match="transaction_count"):
        reconcile_mart("status_summary", mart, fact_count=999, fact_total_amount=Decimal("10.00"))


def test_reconcile_mart_fails_on_amount_mismatch(spark):
    fact = _make_fact(spark, [_fact_row(transaction_id="TXN-1", amount=Decimal("10.00"))])
    mart = build_status_summary(fact)
    with pytest.raises(AssertionError, match="total_amount"):
        reconcile_mart("status_summary", mart, fact_count=1, fact_total_amount=Decimal("999.00"))


def test_every_mart_reconciles_against_the_same_fact_table(spark):
    fact = _make_fact(
        spark,
        [
            _fact_row(transaction_id="TXN-1", merchant_key=1, mcc="5812", currency_code="GBP", card_network="VISA", country_code="GB", amount=Decimal("10.00")),
            _fact_row(transaction_id="TXN-2", merchant_key=2, mcc="5411", currency_code="USD", card_network="AMEX", country_code="US", amount=Decimal("20.00")),
            _fact_row(transaction_id="TXN-3", merchant_key=1, mcc="5812", currency_code="GBP", card_network="VISA", country_code="GB", amount=Decimal("5.00")),
        ],
    )
    dim_merchant = _make_dim_merchant(spark, [(1, "MER-0001", "A"), (2, "MER-0002", "B")])
    fact_count = fact.count()
    fact_total = Decimal("35.00")

    reconcile_mart("status_summary", build_status_summary(fact), fact_count, fact_total)
    reconcile_mart("merchant_summary", build_merchant_summary(fact, dim_merchant), fact_count, fact_total)
    reconcile_mart("mcc_summary", build_mcc_summary(fact), fact_count, fact_total)
    reconcile_mart("currency_summary", build_currency_summary(fact), fact_count, fact_total)
    reconcile_mart("card_network_summary", build_card_network_summary(fact), fact_count, fact_total)
    reconcile_mart("country_summary", build_country_summary(fact), fact_count, fact_total)
    # no raise for any of them
