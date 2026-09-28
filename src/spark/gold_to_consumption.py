"""S16 Step 5 -- Gold -> Consumption analytical layer (learning pipeline).

Reads the verified Gold star schema (data/gold_spark/{fact_transactions,
dim_merchant,dim_date}, built by src/spark/silver_to_gold.py) and produces
7 small, real summary marts - one per business question already
established (and proven derivable) during Step 4's verification. Every
mart's columns map directly to a question that was explicitly asked for;
nothing here is an invented metric.

    status_summary        - transaction count/amount by transaction_status
                             (answers "transaction count, transaction
                             amount, approved/declined/pending amount")
    merchant_summary       - transaction count/amount by merchant
    mcc_summary             - transaction amount by MCC
    currency_summary        - transaction amount by currency
    card_network_summary    - transaction amount by card network
    country_summary          - transaction amount by country
    daily_trend               - transaction count/amount by calendar date

Each mart is a complete, non-overlapping partition of fact_transactions by
one column (or one dimension join) - so the reconciliation check for this
layer is exactly: SUM(mart.total_amount) == SUM(fact.amount) and
SUM(mart.transaction_count) == COUNT(fact.*), for every single mart, with
no exceptions. This is the same control-total discipline used throughout
this project (S14 Gold, S15-A dbt) - re-derived at this layer rather than
assumed to still hold just because Step 4 verified it once upstream.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import count as fcount
from pyspark.sql.functions import sum as fsum

from src.spark.config import (
    CONSUMPTION_CARD_NETWORK_SUMMARY_PATH,
    CONSUMPTION_COUNTRY_SUMMARY_PATH,
    CONSUMPTION_CURRENCY_SUMMARY_PATH,
    CONSUMPTION_DAILY_TREND_PATH,
    CONSUMPTION_MCC_SUMMARY_PATH,
    CONSUMPTION_MERCHANT_SUMMARY_PATH,
    CONSUMPTION_STATUS_SUMMARY_PATH,
    GOLD_DIM_DATE_PATH,
    GOLD_DIM_MERCHANT_PATH,
    GOLD_FACT_TRANSACTIONS_PATH,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("gold_to_consumption")


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("MerchantMCC-GoldToConsumption").master("local[*]").getOrCreate()


def read_gold(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    logger.info("Reading Gold star schema (fact_transactions, dim_merchant, dim_date)")
    fact = spark.read.parquet(str(GOLD_FACT_TRANSACTIONS_PATH))
    dim_merchant = spark.read.parquet(str(GOLD_DIM_MERCHANT_PATH))
    dim_date = spark.read.parquet(str(GOLD_DIM_DATE_PATH))
    logger.info("fact_transactions rows: %d | dim_merchant rows: %d | dim_date rows: %d", fact.count(), dim_merchant.count(), dim_date.count())
    return fact, dim_merchant, dim_date


def _summarize(df: DataFrame, group_cols: list[str]) -> DataFrame:
    """Shared aggregation shape every mart in this module uses: one row
    per distinct value of group_cols, with a transaction_count and a
    Decimal total_amount - the two measures every requested business
    question in this step actually needs."""
    return df.groupBy(*group_cols).agg(
        fcount("*").alias("transaction_count"),
        fsum("amount").alias("total_amount"),
    )


def build_status_summary(fact: DataFrame) -> DataFrame:
    return _summarize(fact, ["transaction_status"]).select("transaction_status", "transaction_count", "total_amount")


def build_merchant_summary(fact: DataFrame, dim_merchant: DataFrame) -> DataFrame:
    joined = fact.join(dim_merchant, on="merchant_key", how="left")
    return _summarize(joined, ["merchant_key", "merchant_id", "merchant_name"]).select(
        "merchant_key", "merchant_id", "merchant_name", "transaction_count", "total_amount"
    )


def build_mcc_summary(fact: DataFrame) -> DataFrame:
    return _summarize(fact, ["mcc"]).select("mcc", "transaction_count", "total_amount")


def build_currency_summary(fact: DataFrame) -> DataFrame:
    return _summarize(fact, ["currency_code"]).select("currency_code", "transaction_count", "total_amount")


def build_card_network_summary(fact: DataFrame) -> DataFrame:
    return _summarize(fact, ["card_network"]).select("card_network", "transaction_count", "total_amount")


def build_country_summary(fact: DataFrame) -> DataFrame:
    return _summarize(fact, ["country_code"]).select("country_code", "transaction_count", "total_amount")


def build_daily_trend(fact: DataFrame, dim_date: DataFrame) -> DataFrame:
    joined = fact.join(dim_date, on="date_key", how="left")
    return _summarize(joined, ["date_key", "full_date", "year", "month", "month_name", "day_name"]).select(
        "date_key", "full_date", "year", "month", "month_name", "day_name", "transaction_count", "total_amount"
    )


def reconcile_mart(name: str, mart: DataFrame, fact_count: int, fact_total_amount) -> None:
    """FAILS LOUDLY (AssertionError) if a mart's totals don't exactly
    match fact_transactions - every mart here is a complete, non-
    overlapping partition of the same fact table, so there is no
    legitimate reason these could ever differ."""
    mart_count = mart.agg(fsum("transaction_count")).collect()[0][0]
    mart_total = mart.agg(fsum("total_amount")).collect()[0][0]

    assert mart_count == fact_count, f"{name}: SUM(transaction_count)={mart_count} != fact_transactions count={fact_count}"
    assert mart_total == fact_total_amount, f"{name}: SUM(total_amount)={mart_total} != fact_transactions SUM(amount)={fact_total_amount}"


def _write_and_reconcile(name: str, mart: DataFrame, path, fact_count: int, fact_total_amount) -> int:
    reconcile_mart(name, mart, fact_count, fact_total_amount)
    row_count = mart.count()
    logger.info("Writing %s (%d rows) to %s", name, row_count, path)
    mart.write.mode("overwrite").parquet(str(path))
    return row_count


def run() -> dict[str, int]:
    spark = build_spark_session()
    try:
        fact, dim_merchant, dim_date = read_gold(spark)
        fact_count = fact.count()
        fact_total_amount = fact.agg(fsum("amount")).collect()[0][0]
        logger.info("fact_transactions reconciliation baseline: count=%d, SUM(amount)=%s", fact_count, fact_total_amount)

        summary = {
            "status_summary": _write_and_reconcile(
                "status_summary", build_status_summary(fact), CONSUMPTION_STATUS_SUMMARY_PATH, fact_count, fact_total_amount
            ),
            "merchant_summary": _write_and_reconcile(
                "merchant_summary",
                build_merchant_summary(fact, dim_merchant),
                CONSUMPTION_MERCHANT_SUMMARY_PATH,
                fact_count,
                fact_total_amount,
            ),
            "mcc_summary": _write_and_reconcile(
                "mcc_summary", build_mcc_summary(fact), CONSUMPTION_MCC_SUMMARY_PATH, fact_count, fact_total_amount
            ),
            "currency_summary": _write_and_reconcile(
                "currency_summary", build_currency_summary(fact), CONSUMPTION_CURRENCY_SUMMARY_PATH, fact_count, fact_total_amount
            ),
            "card_network_summary": _write_and_reconcile(
                "card_network_summary",
                build_card_network_summary(fact),
                CONSUMPTION_CARD_NETWORK_SUMMARY_PATH,
                fact_count,
                fact_total_amount,
            ),
            "country_summary": _write_and_reconcile(
                "country_summary", build_country_summary(fact), CONSUMPTION_COUNTRY_SUMMARY_PATH, fact_count, fact_total_amount
            ),
            "daily_trend": _write_and_reconcile(
                "daily_trend", build_daily_trend(fact, dim_date), CONSUMPTION_DAILY_TREND_PATH, fact_count, fact_total_amount
            ),
        }

        logger.info("Gold -> Consumption job complete. %s", summary)
        return summary
    finally:
        spark.stop()
        logger.info("SparkSession stopped cleanly.")


if __name__ == "__main__":
    run()
