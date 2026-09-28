"""S16 Step 4 -- Silver -> Gold analytical layer (learning pipeline).

Reads the verified Silver output (data/silver_spark/transactions/, 875
rows) and builds a small, real dimensional model:

    fact_transactions  - grain: one row per valid Silver transaction
    dim_merchant       - business key merchant_id, attribute merchant_name
    dim_date           - business key full_date, derived calendar attributes

Model decision (explained, not assumed - see the module docstring in
docs/30 once written, and the Step 4 chat report for the full reasoning):
mcc, currency_code, card_network, country_code, and transaction_status are
kept as DEGENERATE dimensions directly on fact_transactions, NOT built as
separate dim_currency/dim_card_network/dim_country/dim_transaction_status
tables. Each of those candidates is, in the available Silver data, nothing
more than a bare code with zero other descriptive attributes - a
"dimension" table holding only {key, code} would add a join for no
analytical value. This mirrors a decision already made and documented in
this project's real Gold layer (src/gold/facts.py, docs/27 SS6): low-
cardinality codes with no extra attributes stay degenerate, not because
building the table is hard, but because it would be pure ceremony.
dim_merchant and dim_date are different: dim_merchant has a real
descriptive attribute (merchant_name) beyond its key, and dim_date's
calendar attributes (year/quarter/day_name/is_weekend/...) are standard,
well-established derived attributes every real date dimension carries -
not invented business entities.

Surrogate keys are deterministic WITHIN one full rebuild (row_number()
over a fixed alphabetical ordering of the natural key) - the same natural
key always gets the same integer as long as the underlying set of distinct
merchants doesn't change shape between runs. This is a smaller guarantee
than the real Gold layer's persisted-registry approach (src/gold/keys.py),
which additionally never renumbers existing keys as NEW ones are added
over time across separate incremental runs. That extra guarantee isn't
needed here: this job, like Bronze->Silver before it, is a full rebuild
every run, not an incremental one - documented, not an oversight.
"""

import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    dayofweek,
    month,
    quarter,
    row_number,
    to_date,
    weekofyear,
    year,
)

from src.spark.config import (
    GOLD_DIM_DATE_PATH,
    GOLD_DIM_MERCHANT_PATH,
    GOLD_FACT_TRANSACTIONS_PATH,
    SILVER_TRANSACTIONS_PATH,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("silver_to_gold")

FACT_COLUMNS = [
    "transaction_id", "merchant_key", "date_key", "mcc", "currency_code",
    "card_network", "country_code", "transaction_status", "amount",
    "transaction_timestamp", "ingestion_date", "processed_at",
]

DIM_DATE_COLUMNS = [
    "date_key", "full_date", "year", "quarter", "month", "month_name",
    "day", "day_of_week", "day_name", "week_of_year", "is_weekend",
]


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("MerchantMCC-SilverToGold").master("local[*]").getOrCreate()


def read_silver(spark: SparkSession, path: str = str(SILVER_TRANSACTIONS_PATH)) -> DataFrame:
    logger.info("Reading Silver Parquet from %s", path)
    df = spark.read.parquet(path)
    logger.info("Silver rows read: %d", df.count())
    return df


def build_dim_date(silver: DataFrame) -> DataFrame:
    """Business key: full_date. Built from the REAL dates observed in
    Silver's transaction_timestamp (never a fabricated calendar range).
    date_key is an integer (yyyyMMdd) - deterministic by construction, no
    row_number() needed. day_of_week/is_weekend use Spark's native
    dayofweek() convention: 1=Sunday .. 7=Saturday (documented here since
    it differs from ISO weekday numbering)."""
    dates = silver.select(to_date(col("transaction_timestamp")).alias("full_date")).distinct()
    return (
        dates.withColumn("date_key", (year("full_date") * 10000 + month("full_date") * 100 + dayofmonth("full_date")).cast("int"))
        .withColumn("year", year("full_date"))
        .withColumn("quarter", quarter("full_date"))
        .withColumn("month", month("full_date"))
        .withColumn("month_name", date_format("full_date", "MMMM"))
        .withColumn("day", dayofmonth("full_date"))
        .withColumn("day_of_week", dayofweek("full_date"))
        .withColumn("day_name", date_format("full_date", "EEEE"))
        .withColumn("week_of_year", weekofyear("full_date"))
        .withColumn("is_weekend", dayofweek("full_date").isin(1, 7))
        .select(*DIM_DATE_COLUMNS)
    )


def build_dim_merchant(silver: DataFrame) -> DataFrame:
    """Business key: merchant_id. merchant_key assigned via row_number()
    over a fixed alphabetical ordering of merchant_id - deterministic
    across reruns of this same full-rebuild job (see module docstring for
    the tradeoff vs the real Gold layer's persisted-registry approach)."""
    merchants = silver.select("merchant_id", "merchant_name").distinct()
    window = Window.orderBy("merchant_id")
    return merchants.withColumn("merchant_key", row_number().over(window).cast("int")).select(
        "merchant_key", "merchant_id", "merchant_name"
    )


def build_fact_transactions(silver: DataFrame, dim_merchant: DataFrame, dim_date: DataFrame) -> DataFrame:
    """Grain: one row per valid Silver transaction (transaction_id).
    LEFT joins on purpose, not inner: by construction every merchant_id/
    date in Silver has a matching dim row (both dims are built from this
    SAME Silver read), so a left join should never actually drop or null
    anything - but an inner join could silently swallow a fact row if that
    assumption were ever violated, whereas a left join surfaces it as a
    null merchant_key/date_key that run_data_quality_checks() catches and
    fails loudly on instead."""
    with_date = silver.withColumn("full_date", to_date(col("transaction_timestamp")))
    joined = with_date.join(dim_merchant.select("merchant_key", "merchant_id"), on="merchant_id", how="left").join(
        dim_date.select("date_key", "full_date"), on="full_date", how="left"
    )
    return joined.select(*FACT_COLUMNS)


def run_data_quality_checks(fact: DataFrame, dim_merchant: DataFrame, dim_date: DataFrame, silver_count: int) -> None:
    """Every check FAILS LOUDLY (raises AssertionError) rather than
    logging and continuing - consistent with this project's reconciliation
    discipline throughout (Bronze->Silver's own assert, and every prior
    phase's reconcile_counts()-style hard failures)."""
    fact_count = fact.count()
    assert fact_count == silver_count, f"fact row count {fact_count} != Silver valid count {silver_count}"

    dup_tx = fact.groupBy("transaction_id").count().filter("count > 1")
    assert dup_tx.count() == 0, f"duplicate transaction_id(s) in fact_transactions: {[r['transaction_id'] for r in dup_tx.collect()]}"

    null_keys = fact.filter(col("transaction_id").isNull() | col("merchant_key").isNull() | col("date_key").isNull())
    null_key_count = null_keys.count()
    assert null_key_count == 0, f"{null_key_count} fact rows have a null business/surrogate key"

    orphan_merchant = fact.join(dim_merchant, on="merchant_key", how="left_anti")
    assert orphan_merchant.count() == 0, "fact rows reference a merchant_key missing from dim_merchant"

    orphan_date = fact.join(dim_date, on="date_key", how="left_anti")
    assert orphan_date.count() == 0, "fact rows reference a date_key missing from dim_date"

    bad_amount = fact.filter(~(col("amount") > 0))
    assert bad_amount.count() == 0, "fact_transactions contains a non-positive or null amount"

    dup_merchant_key = dim_merchant.groupBy("merchant_id").count().filter("count > 1")
    assert dup_merchant_key.count() == 0, "duplicate merchant_id in dim_merchant"

    dup_date_key = dim_date.groupBy("full_date").count().filter("count > 1")
    assert dup_date_key.count() == 0, "duplicate full_date in dim_date"

    logger.info("All Gold data-quality/integrity checks passed.")


def write_outputs(dim_merchant: DataFrame, dim_date: DataFrame, fact: DataFrame) -> None:
    # Deliberately NOT coalesce(1) here (unlike Step 3's Bronze->Silver
    # output): that was an explicit, documented, demonstration-only choice
    # for a single-file CSV source. Gold is the layer this design should
    # demonstrate scaling past 1,010 rows, so it's left to Spark's natural
    # partitioning.
    logger.info("Writing dim_merchant to %s", GOLD_DIM_MERCHANT_PATH)
    dim_merchant.write.mode("overwrite").parquet(str(GOLD_DIM_MERCHANT_PATH))
    logger.info("Writing dim_date to %s", GOLD_DIM_DATE_PATH)
    dim_date.write.mode("overwrite").parquet(str(GOLD_DIM_DATE_PATH))
    logger.info("Writing fact_transactions to %s", GOLD_FACT_TRANSACTIONS_PATH)
    fact.write.mode("overwrite").parquet(str(GOLD_FACT_TRANSACTIONS_PATH))


def run() -> dict[str, int]:
    spark = build_spark_session()
    try:
        silver = read_silver(spark)
        silver_count = silver.count()

        dim_date = build_dim_date(silver)
        dim_merchant = build_dim_merchant(silver)
        fact = build_fact_transactions(silver, dim_merchant, dim_date)

        run_data_quality_checks(fact, dim_merchant, dim_date, silver_count)
        write_outputs(dim_merchant, dim_date, fact)

        summary = {
            "silver_count": silver_count,
            "fact_count": fact.count(),
            "dim_merchant_count": dim_merchant.count(),
            "dim_date_count": dim_date.count(),
        }
        logger.info("Silver -> Gold job complete. %s", summary)
        return summary
    finally:
        spark.stop()
        logger.info("SparkSession stopped cleanly.")


if __name__ == "__main__":
    run()
