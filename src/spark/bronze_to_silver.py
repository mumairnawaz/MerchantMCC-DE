"""S16 — Bronze -> Silver PySpark transformation (learning pipeline).

Reads the deliberately-messy data/bronze_spark/transactions/transactions.csv
(src/spark/generate_bronze_transactions.py) and produces two Parquet
outputs, per src/spark/config.py's paths:

    data/silver_spark/transactions/    - clean, valid, deduplicated rows
    data/rejected_spark/transactions/  - every rejected row, WITH the exact
                                          reason(s) it was rejected (never a
                                          silent drop - see build_rejected())

Pipeline shape (all DataFrame API, no RDDs):

    read (explicit string schema)
        -> add per-column validity flags
        -> split: passes-all-checks  vs  fails-at-least-one-check
        -> among passes-all-checks: keep first occurrence per
           transaction_id (Window + row_number), route the rest to
           rejected as "duplicate_transaction_id"
        -> normalize casing + cast types on the surviving rows -> Silver
        -> union both rejected groups, with their reasons -> rejected

Design choices worth naming explicitly:
  - Bronze is read as ALL STRING columns (see BRONZE_SCHEMA). Real raw data
    routinely contains values that don't match their "intended" type (our
    own generator injects "N/A" into `amount` and "not-a-date" into
    `transaction_timestamp`) - reading as string first and casting
    explicitly during validation puts US in control of what counts as
    invalid, rather than letting Spark's CSV parser silently coerce bad
    values to null before we ever get a chance to flag them.
  - Currency/status validity is checked against a small, explicit set
    (VALID_CURRENCIES / VALID_STATUSES) below - a deliberately simple
    business rule for this standalone exercise, not a re-implementation of
    the real project's full ISO-4217/Silver validation (src/silver/).
  - `coalesce(1)` is used before writing: at ~1,000 rows this avoids
    scattering the output across one tiny part-file per core, which would
    just be clutter at this scale. A real job on real data volume would
    NOT do this - it would remove parallelism - and that tradeoff is the
    point of calling it out here rather than doing it silently.
"""

import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    current_date,
    current_timestamp,
    expr,
    lit,
    monotonically_increasing_id,
    row_number,
    trim,
    upper,
    when,
)
from pyspark.sql.types import StringType, StructField, StructType

from src.spark.config import BRONZE_TRANSACTIONS_PATH, REJECTED_TRANSACTIONS_PATH, SILVER_TRANSACTIONS_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bronze_to_silver")

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"
VALID_CURRENCIES = ("GBP", "EUR", "USD")
VALID_STATUSES = ("APPROVED", "DECLINED", "PENDING", "REFUNDED")

BRONZE_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("mcc", StringType(), True),
        StructField("currency_code", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("transaction_timestamp", StringType(), True),
        StructField("transaction_status", StringType(), True),
        StructField("card_network", StringType(), True),
        StructField("country_code", StringType(), True),
    ]
)


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("MerchantMCC-BronzeToSilver").master("local[*]").getOrCreate()


def read_bronze(spark: SparkSession, path: str = str(BRONZE_TRANSACTIONS_PATH)) -> DataFrame:
    logger.info("Reading Bronze CSV from %s (explicit all-string schema)", path)
    df = spark.read.csv(path, header=True, schema=BRONZE_SCHEMA)
    logger.info("Bronze rows read: %d", df.count())
    return df


def add_validity_flags(df: DataFrame) -> DataFrame:
    """One boolean column per Step 4 data-quality rule, plus the row-level
    AND of all of them. Kept as separate named flags (not just one opaque
    boolean) so build_rejected() can report exactly which rule(s) a row
    failed - never a silent, unexplained drop.

    Uses try_cast (via expr()), not plain cast(): Spark 4.x enables ANSI
    SQL mode by default, where cast() on a non-numeric/non-date string
    (our own "N/A" / "not-a-date" injected defects) THROWS instead of
    returning null - confirmed by actually running this job (see docs/30).
    try_cast is Spark's own documented, ANSI-safe way to get the old
    lenient "return null on bad input" behavior back, deliberately, for
    exactly this validation use case - not a workaround, the correct tool.

    Every flag is wrapped so it is always a clean True/False, NEVER SQL
    NULL - confirmed necessary by actually running this job: Spark's CSV
    reader treats an empty field as NULL (its default nullValue), and
    NULL.isin(...) evaluates to NULL (not False) under SQL's three-valued
    logic. Left unguarded, a row with an empty currency_code/status but
    every OTHER field valid ended up with passes_all_checks == NULL -
    which is excluded by BOTH `filter(col == True)` and
    `filter(col == False)`, silently vanishing from both Silver and
    rejected. Caught by this job's own reconciliation assertion in run()
    (source_count == silver_count + rejected_count), not by inspection.
    """
    amount_cast = expr("try_cast(amount as decimal(18,2))")
    timestamp_cast = expr("try_cast(transaction_timestamp as timestamp)")
    return (
        df.withColumn("is_valid_transaction_id", col("transaction_id").isNotNull() & (trim(col("transaction_id")) != ""))
        .withColumn("is_valid_merchant_id", col("merchant_id").isNotNull() & (trim(col("merchant_id")) != ""))
        .withColumn("is_valid_amount", amount_cast.isNotNull() & (amount_cast > 0))
        .withColumn(
            "is_valid_currency_code",
            col("currency_code").isNotNull() & upper(trim(col("currency_code"))).isin(*VALID_CURRENCIES),
        )
        .withColumn("is_valid_timestamp", timestamp_cast.isNotNull())
        .withColumn(
            "is_valid_status",
            col("transaction_status").isNotNull() & upper(trim(col("transaction_status"))).isin(*VALID_STATUSES),
        )
        .withColumn(
            "passes_all_checks",
            coalesce(
                col("is_valid_transaction_id")
                & col("is_valid_merchant_id")
                & col("is_valid_amount")
                & col("is_valid_currency_code")
                & col("is_valid_timestamp")
                & col("is_valid_status"),
                lit(False),
            ),
        )
    )


FLAG_TO_REASON = {
    "is_valid_transaction_id": "missing_transaction_id",
    "is_valid_merchant_id": "missing_merchant_id",
    "is_valid_amount": "invalid_amount",
    "is_valid_currency_code": "invalid_currency_code",
    "is_valid_timestamp": "malformed_transaction_timestamp",
    "is_valid_status": "invalid_transaction_status",
}


def _rejection_reason_column():
    reasons = [when(~col(flag), lit(reason)) for flag, reason in FLAG_TO_REASON.items()]
    return concat_ws(";", *reasons)


def split_valid_and_column_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """First split: rows that pass every per-column check vs rows that
    fail at least one. Duplicate-transaction_id handling happens
    separately, afterward - see dedupe_and_route()."""
    column_valid = df.filter(col("passes_all_checks") == True)  # noqa: E712
    column_rejected = df.filter(col("passes_all_checks") == False).withColumn(  # noqa: E712
        "rejection_reason", _rejection_reason_column()
    )
    return column_valid, column_rejected


def dedupe_and_route(column_valid: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Among rows that already passed every per-column check, keep the
    FIRST occurrence of each transaction_id (by original Bronze row order)
    as Silver; every later occurrence of the same transaction_id is routed
    to rejected with reason "duplicate_transaction_id" - never dropped."""
    window = Window.partitionBy("transaction_id").orderBy(col("_row_seq"))
    ranked = column_valid.withColumn("_dup_rank", row_number().over(window))

    deduped = ranked.filter(col("_dup_rank") == 1).drop("_dup_rank")
    duplicates = (
        ranked.filter(col("_dup_rank") > 1)
        .drop("_dup_rank")
        .withColumn("rejection_reason", lit("duplicate_transaction_id"))
    )
    return deduped, duplicates


def normalize_silver(df: DataFrame) -> DataFrame:
    """Casing normalization (Step 5) + type casting, applied only to rows
    that are already known-valid. mcc/merchant_id/merchant_name/
    transaction_id are left as-is - they were generated consistently and
    normalizing them isn't one of the requested rules."""
    return (
        df.withColumn("currency_code", upper(trim(col("currency_code"))))
        .withColumn("transaction_status", upper(trim(col("transaction_status"))))
        .withColumn("card_network", upper(trim(col("card_network"))))
        .withColumn("country_code", upper(trim(col("country_code"))))
        .withColumn("amount", expr("try_cast(amount as decimal(18,2))"))
        .withColumn("transaction_timestamp", expr("try_cast(transaction_timestamp as timestamp)"))
        .withColumn("ingestion_date", current_date())
        .withColumn("processed_at", current_timestamp())
        .select(
            "transaction_id", "merchant_id", "merchant_name", "mcc", "currency_code", "amount",
            "transaction_timestamp", "transaction_status", "card_network", "country_code",
            "ingestion_date", "processed_at",
        )
    )


def build_rejected(column_rejected: DataFrame, duplicates: DataFrame) -> DataFrame:
    """Unions both rejection sources into one schema-aligned DataFrame -
    original raw values are preserved (not normalized/cast) so the
    rejected file is useful for actually diagnosing what came in wrong."""
    common_columns = [
        "transaction_id", "merchant_id", "merchant_name", "mcc", "currency_code", "amount",
        "transaction_timestamp", "transaction_status", "card_network", "country_code", "rejection_reason",
    ]
    rejected = column_rejected.select(*common_columns).unionByName(duplicates.select(*common_columns))
    return rejected.withColumn("ingestion_date", current_date()).withColumn("processed_at", current_timestamp())


def write_outputs(silver: DataFrame, rejected: DataFrame) -> None:
    logger.info("Writing Silver output to %s", SILVER_TRANSACTIONS_PATH)
    silver.coalesce(1).write.mode("overwrite").parquet(str(SILVER_TRANSACTIONS_PATH))
    logger.info("Writing rejected output to %s", REJECTED_TRANSACTIONS_PATH)
    rejected.coalesce(1).write.mode("overwrite").parquet(str(REJECTED_TRANSACTIONS_PATH))


def run() -> dict[str, int]:
    spark = build_spark_session()
    try:
        bronze = read_bronze(spark)
        # a stable row-sequence column, assigned immediately after reading
        # and before any filtering/repartitioning, so dedupe_and_route()'s
        # "keep the first occurrence" rule is deterministic rather than
        # dependent on Spark's internal (unordered) partition layout.
        bronze = bronze.withColumn("_row_seq", monotonically_increasing_id())

        flagged = add_validity_flags(bronze)
        column_valid, column_rejected = split_valid_and_column_rejected(flagged)

        logger.info("Applying duplicate transaction_id handling")
        deduped, duplicates = dedupe_and_route(column_valid)

        silver = normalize_silver(deduped)
        rejected = build_rejected(column_rejected, duplicates)

        source_count = bronze.count()
        silver_count = silver.count()
        rejected_count = rejected.count()
        duplicate_count = duplicates.count()

        logger.info("Source rows: %d | Valid (Silver): %d | Rejected: %d (of which duplicates: %d)",
                    source_count, silver_count, rejected_count, duplicate_count)

        write_outputs(silver, rejected)

        summary = {
            "source_count": source_count,
            "silver_count": silver_count,
            "rejected_count": rejected_count,
            "duplicate_count": duplicate_count,
        }
        assert source_count == silver_count + rejected_count, "every Bronze row must land in exactly one output"
        logger.info("Bronze -> Silver job complete. %s", summary)
        return summary
    finally:
        spark.stop()
        logger.info("SparkSession stopped cleanly.")


if __name__ == "__main__":
    run()
