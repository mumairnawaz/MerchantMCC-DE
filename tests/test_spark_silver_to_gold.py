"""S16 Step 4 -- tests for src/spark/silver_to_gold.py.

Same pattern as tests/test_spark_bronze_to_silver.py: pytest.importorskip
so this skips cleanly on the main Windows venv (no PySpark there) and
actually runs under WSL's .venv-spark. Small, deterministic in-memory
Silver-shaped DataFrames throughout.
"""

import pytest

pytest.importorskip("pyspark")

from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.spark.silver_to_gold import (
    build_dim_date,
    build_dim_merchant,
    build_fact_transactions,
    run_data_quality_checks,
)

SILVER_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("mcc", StringType(), True),
        StructField("currency_code", StringType(), True),
        StructField("amount", DecimalType(18, 2), True),
        StructField("transaction_timestamp", TimestampType(), True),
        StructField("transaction_status", StringType(), True),
        StructField("card_network", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("ingestion_date", DateType(), True),
        StructField("processed_at", TimestampType(), True),
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test-silver-to-gold").master("local[1]").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _row(**overrides):
    base = {
        "transaction_id": "TXN-000001",
        "merchant_id": "MER-0001",
        "merchant_name": "Ada's Coffee House",
        "mcc": "5812",
        "currency_code": "GBP",
        "amount": Decimal("42.50"),
        "transaction_timestamp": datetime(2026, 7, 1, 10, 0, 0),
        "transaction_status": "APPROVED",
        "card_network": "VISA",
        "country_code": "GB",
        "ingestion_date": datetime(2026, 9, 23).date(),
        "processed_at": datetime(2026, 9, 23, 12, 0, 0),
    }
    base.update(overrides)
    return tuple(base[f.name] for f in SILVER_SCHEMA.fields)


def _make_silver(spark, rows):
    return spark.createDataFrame(list(rows), schema=SILVER_SCHEMA)


# ---- dim_date ----


def test_dim_date_business_key_is_unique_per_distinct_date(spark):
    silver = _make_silver(
        spark,
        [
            _row(transaction_id="TXN-1", transaction_timestamp=datetime(2026, 7, 1, 9, 0)),
            _row(transaction_id="TXN-2", transaction_timestamp=datetime(2026, 7, 1, 18, 0)),  # same date, diff time
            _row(transaction_id="TXN-3", transaction_timestamp=datetime(2026, 7, 2, 9, 0)),
        ],
    )
    dim_date = build_dim_date(silver)
    assert dim_date.count() == 2  # two distinct calendar dates, not three


def test_dim_date_key_is_deterministic_yyyymmdd(spark):
    silver = _make_silver(spark, [_row(transaction_timestamp=datetime(2026, 7, 4, 9, 0))])
    dim_date = build_dim_date(silver)
    row = dim_date.collect()[0]
    assert row["date_key"] == 20260704
    assert row["year"] == 2026
    assert row["month"] == 7
    assert row["day"] == 4


def test_dim_date_weekend_flag_is_correct(spark):
    # 2026-07-04 is a Saturday, 2026-07-06 is a Monday (real calendar facts)
    silver = _make_silver(
        spark,
        [
            _row(transaction_id="TXN-1", transaction_timestamp=datetime(2026, 7, 4, 9, 0)),
            _row(transaction_id="TXN-2", transaction_timestamp=datetime(2026, 7, 6, 9, 0)),
        ],
    )
    dim_date = build_dim_date(silver)
    by_day = {row["day"]: row["is_weekend"] for row in dim_date.collect()}
    assert by_day[4] is True
    assert by_day[6] is False


# ---- dim_merchant ----


def test_dim_merchant_business_key_is_unique_per_merchant(spark):
    silver = _make_silver(
        spark,
        [
            _row(transaction_id="TXN-1", merchant_id="MER-0001", merchant_name="Ada's Coffee House"),
            _row(transaction_id="TXN-2", merchant_id="MER-0001", merchant_name="Ada's Coffee House"),
            _row(transaction_id="TXN-3", merchant_id="MER-0002", merchant_name="Riverside Grocers"),
        ],
    )
    dim_merchant = build_dim_merchant(silver)
    assert dim_merchant.count() == 2


def test_dim_merchant_surrogate_key_is_deterministic_across_rebuilds(spark):
    silver = _make_silver(
        spark,
        [
            _row(transaction_id="TXN-1", merchant_id="MER-0002", merchant_name="Riverside Grocers"),
            _row(transaction_id="TXN-2", merchant_id="MER-0001", merchant_name="Ada's Coffee House"),
        ],
    )
    first = {r["merchant_id"]: r["merchant_key"] for r in build_dim_merchant(silver).collect()}
    second = {r["merchant_id"]: r["merchant_key"] for r in build_dim_merchant(silver).collect()}
    assert first == second
    # alphabetical ordering of the business key -> deterministic assignment
    assert first["MER-0001"] < first["MER-0002"]


def test_dim_merchant_surrogate_keys_have_no_duplicates(spark):
    silver = _make_silver(
        spark,
        [
            _row(transaction_id="TXN-1", merchant_id="MER-0001", merchant_name="A"),
            _row(transaction_id="TXN-2", merchant_id="MER-0002", merchant_name="B"),
            _row(transaction_id="TXN-3", merchant_id="MER-0003", merchant_name="C"),
        ],
    )
    keys = [r["merchant_key"] for r in build_dim_merchant(silver).collect()]
    assert len(keys) == len(set(keys))


# ---- fact_transactions ----


def test_fact_grain_is_one_row_per_transaction(spark):
    silver = _make_silver(spark, [_row(transaction_id="TXN-1"), _row(transaction_id="TXN-2")])
    dim_date = build_dim_date(silver)
    dim_merchant = build_dim_merchant(silver)
    fact = build_fact_transactions(silver, dim_merchant, dim_date)
    assert fact.count() == 2
    assert fact.select("transaction_id").distinct().count() == 2


def test_fact_resolves_merchant_key_and_date_key_correctly(spark):
    silver = _make_silver(
        spark,
        [_row(transaction_id="TXN-1", merchant_id="MER-0001", transaction_timestamp=datetime(2026, 7, 1, 9, 0))],
    )
    dim_date = build_dim_date(silver)
    dim_merchant = build_dim_merchant(silver)
    fact = build_fact_transactions(silver, dim_merchant, dim_date)
    row = fact.collect()[0]

    expected_merchant_key = dim_merchant.filter("merchant_id = 'MER-0001'").collect()[0]["merchant_key"]
    expected_date_key = dim_date.filter("full_date = '2026-07-01'").collect()[0]["date_key"]
    assert row["merchant_key"] == expected_merchant_key
    assert row["date_key"] == expected_date_key


def test_fact_preserves_decimal_amount(spark):
    silver = _make_silver(spark, [_row(amount=Decimal("123.45"))])
    dim_date = build_dim_date(silver)
    dim_merchant = build_dim_merchant(silver)
    fact = build_fact_transactions(silver, dim_merchant, dim_date)
    assert fact.collect()[0]["amount"] == Decimal("123.45")


# ---- aggregation correctness ----


def test_amount_by_currency_aggregation_is_correct(spark):
    silver = _make_silver(
        spark,
        [
            _row(transaction_id="TXN-1", currency_code="GBP", amount=Decimal("10.00")),
            _row(transaction_id="TXN-2", currency_code="GBP", amount=Decimal("20.00")),
            _row(transaction_id="TXN-3", currency_code="USD", amount=Decimal("5.00")),
        ],
    )
    dim_date = build_dim_date(silver)
    dim_merchant = build_dim_merchant(silver)
    fact = build_fact_transactions(silver, dim_merchant, dim_date)

    from pyspark.sql.functions import sum as fsum

    totals = {r["currency_code"]: r["total"] for r in fact.groupBy("currency_code").agg(fsum("amount").alias("total")).collect()}
    assert totals["GBP"] == Decimal("30.00")
    assert totals["USD"] == Decimal("5.00")


# ---- data-quality / integrity checks ----


def test_data_quality_checks_pass_on_clean_star_schema(spark):
    silver = _make_silver(spark, [_row(transaction_id="TXN-1"), _row(transaction_id="TXN-2", merchant_id="MER-0002", merchant_name="B")])
    dim_date = build_dim_date(silver)
    dim_merchant = build_dim_merchant(silver)
    fact = build_fact_transactions(silver, dim_merchant, dim_date)
    run_data_quality_checks(fact, dim_merchant, dim_date, silver.count())  # no raise


def test_data_quality_checks_fail_on_fact_row_count_mismatch(spark):
    silver = _make_silver(spark, [_row()])
    dim_date = build_dim_date(silver)
    dim_merchant = build_dim_merchant(silver)
    fact = build_fact_transactions(silver, dim_merchant, dim_date)
    with pytest.raises(AssertionError, match="fact row count"):
        run_data_quality_checks(fact, dim_merchant, dim_date, silver_count=999)


def test_data_quality_checks_fail_on_duplicate_transaction_id_in_fact(spark):
    silver = _make_silver(spark, [_row()])
    dim_date = build_dim_date(silver)
    dim_merchant = build_dim_merchant(silver)
    fact = build_fact_transactions(silver, dim_merchant, dim_date)
    duplicated_fact = fact.union(fact)  # simulate a corrupted fact table with a repeated transaction_id
    # silver_count matches duplicated_fact's row count on purpose, so the
    # row-count check (checked first) passes and the duplicate-id check
    # specifically is what's being isolated and tested here.
    with pytest.raises(AssertionError, match="duplicate transaction_id"):
        run_data_quality_checks(duplicated_fact, dim_merchant, dim_date, silver_count=duplicated_fact.count())


def test_data_quality_checks_fail_on_orphan_merchant_key(spark):
    silver = _make_silver(spark, [_row()])
    dim_date = build_dim_date(silver)
    dim_merchant = build_dim_merchant(silver)
    fact = build_fact_transactions(silver, dim_merchant, dim_date)
    empty_dim_merchant = dim_merchant.filter("1 = 0")  # simulate a dim_merchant that's missing this fact's merchant
    with pytest.raises(AssertionError, match="dim_merchant"):
        run_data_quality_checks(fact, empty_dim_merchant, dim_date, silver_count=1)


def test_data_quality_checks_fail_on_non_positive_amount(spark):
    silver = _make_silver(spark, [_row(amount=Decimal("-5.00"))])
    dim_date = build_dim_date(silver)
    dim_merchant = build_dim_merchant(silver)
    fact = build_fact_transactions(silver, dim_merchant, dim_date)
    with pytest.raises(AssertionError, match="non-positive or null amount"):
        run_data_quality_checks(fact, dim_merchant, dim_date, silver_count=1)


# ---- full Silver -> Gold reconciliation ----


def test_full_reconciliation_fact_count_matches_silver_count(spark):
    rows = [_row(transaction_id=f"TXN-{i}", merchant_id="MER-0001" if i % 2 == 0 else "MER-0002", merchant_name="A" if i % 2 == 0 else "B") for i in range(10)]
    silver = _make_silver(spark, rows)
    dim_date = build_dim_date(silver)
    dim_merchant = build_dim_merchant(silver)
    fact = build_fact_transactions(silver, dim_merchant, dim_date)

    assert fact.count() == silver.count() == 10
    run_data_quality_checks(fact, dim_merchant, dim_date, silver.count())  # no raise
