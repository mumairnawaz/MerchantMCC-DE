"""Path constants for the S16 Spark Bronze->Silver->Gold->Consumption
learning pipeline.

Deliberately separate from data/bronze|silver (the real API Silver layer,
src/silver/), data/bronze_cdc|silver_cdc (the real CDC layer, src/cdc/),
and data/gold (the real S14 DuckDB-based Gold layer, src/gold/) - this is
a standalone, self-contained exercise with its own small synthetic
dataset, following the same "_suffix" pattern this project already uses to
keep parallel pipelines from colliding (bronze_cdc sits next to bronze the
same way bronze_spark/gold_spark/consumption_spark sit next to both).

No environment variables needed (same reasoning as src/gold/config.py):
everything here is a local file path, no external service to connect to.
"""

from pathlib import Path

BRONZE_ROOT = Path("data/bronze_spark")
SILVER_ROOT = Path("data/silver_spark")
REJECTED_ROOT = Path("data/rejected_spark")
GOLD_ROOT = Path("data/gold_spark")
CONSUMPTION_ROOT = Path("data/consumption_spark")

BRONZE_TRANSACTIONS_PATH = BRONZE_ROOT / "transactions" / "transactions.csv"
SILVER_TRANSACTIONS_PATH = SILVER_ROOT / "transactions"
REJECTED_TRANSACTIONS_PATH = REJECTED_ROOT / "transactions"

GOLD_DIM_MERCHANT_PATH = GOLD_ROOT / "dim_merchant"
GOLD_DIM_DATE_PATH = GOLD_ROOT / "dim_date"
GOLD_FACT_TRANSACTIONS_PATH = GOLD_ROOT / "fact_transactions"

CONSUMPTION_STATUS_SUMMARY_PATH = CONSUMPTION_ROOT / "status_summary"
CONSUMPTION_MERCHANT_SUMMARY_PATH = CONSUMPTION_ROOT / "merchant_summary"
CONSUMPTION_MCC_SUMMARY_PATH = CONSUMPTION_ROOT / "mcc_summary"
CONSUMPTION_CURRENCY_SUMMARY_PATH = CONSUMPTION_ROOT / "currency_summary"
CONSUMPTION_CARD_NETWORK_SUMMARY_PATH = CONSUMPTION_ROOT / "card_network_summary"
CONSUMPTION_COUNTRY_SUMMARY_PATH = CONSUMPTION_ROOT / "country_summary"
CONSUMPTION_DAILY_TREND_PATH = CONSUMPTION_ROOT / "daily_trend"
