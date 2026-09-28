"""S16 — first local PySpark application (learning exercise only).

This script does NOT touch any real MerchantMCC-DE data (no Gold Parquet,
no Silver, no Postgres, no Kafka) — it only creates a tiny in-memory
DataFrame to demonstrate the core PySpark building blocks:

    SparkSession -> DataFrame -> transformation -> aggregation -> action

Run it with spark-submit (see docs/30 once written, or the chat reply that
introduced this file for the exact WSL command). Every print() below is
there so you can see, step by step, what each concept produces — not
because a real pipeline script would normally print this much.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# A SparkSession is the single entry point to Spark. Creating it starts the
# Spark DRIVER process (this Python process itself, once py4j hands control
# to the JVM it launches) and, in local mode, prepares local "executor"
# threads on this same machine — there is no separate cluster.
spark = (
    SparkSession.builder
    .appName("MerchantMCC-HelloSpark")
    .master("local[*]")  # use all available CPU cores on this machine as executors
    .getOrCreate()
)

print(f"Spark version: {spark.version}")
print(f"Application name: {spark.sparkContext.appName}")

# A small, fully in-memory DataFrame — thematically like a tiny slice of
# MerchantMCC's transaction data, but NOT read from any real project file.
# Creating this DataFrame is a TRANSFORMATION-free step (no computation
# happens yet) — Spark just records the data and an inferred schema.
sample_transactions = [
    ("Ada Proctor", "GBP", 42.50),
    ("Ada Proctor", "GBP", 18.25),
    ("Seraphine", "EUR", 91.00),
    ("Treat", "USD", 12.75),
    ("Treat", "USD", 60.00),
]
columns = ["merchant_name", "currency_code", "amount"]
df = spark.createDataFrame(sample_transactions, columns)

print("\n--- DataFrame contents (this is an ACTION: it triggers a Spark job) ---")
df.show()

print("--- DataFrame schema ---")
df.printSchema()

# One TRANSFORMATION: filter to GBP/USD rows only, and add a derived column.
# Transformations are LAZY — nothing runs yet, Spark only builds a plan.
transformed = df.filter(col("currency_code") != "EUR").withColumn(
    "amount_doubled", col("amount") * 2
)

print("--- After transformation (filter + withColumn), still lazy until shown ---")
transformed.show()

# One AGGREGATION: total amount per merchant. groupBy + agg is itself a
# transformation; calling .show() below is the ACTION that actually
# triggers the job (and, behind the scenes, a "stage boundary" — grouping
# requires Spark to shuffle rows so matching merchant_name values land
# together before summing).
print("--- Aggregation: total amount per merchant ---")
totals = df.groupBy("merchant_name").agg(spark_sum("amount").alias("total_amount"))
totals.show()

# Partitions are the unit of parallelism: each partition of a DataFrame
# becomes one task when an action runs. With only 5 tiny rows created
# locally, Spark will typically show a small number of partitions (often
# matching local[*]'s core count for a freshly created DataFrame) — there
# is nothing to genuinely parallelize at this size, but the mechanism is
# the same one a multi-terabyte DataFrame would use.
print(f"--- Number of partitions in df: {df.rdd.getNumPartitions()} ---")

# Always stop the SparkSession explicitly: this shuts down the driver's
# JVM and releases the local executor threads/resources cleanly.
spark.stop()
print("\nSparkSession stopped cleanly.")
