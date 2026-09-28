"""Generates the raw, deliberately messy Bronze CSV for the Spark
Bronze->Silver learning pipeline (S16).

Written in plain Python (csv + random + datetime) on purpose, NOT PySpark:
in real life, Bronze data arrives however the source system produced it
(an SFTP drop, a processor's daily export, an API dump) - it is never
manufactured by the same Spark job that will later read it. Spark's job
starts at "read the raw file", not "invent the raw file".

Reproducible: a fixed SEED means re-running this script produces the exact
same file every time (same convention as src/synthetic/common.py's
SEED=20260921), so the pipeline downstream of it is testable and
deterministic.

Deliberately injected raw-data problems (so the Bronze->Silver data-quality
step has real, non-decorative work to do - see docs/30 once written):
  - missing transaction_id
  - missing merchant_id
  - invalid amount (negative, zero, or non-numeric)
  - missing/invalid currency_code
  - malformed transaction_timestamp
  - invalid/missing transaction_status
  - exact duplicate transaction_id (the same row repeated, as a raw
    processor occasionally re-sends a record)
  - inconsistent casing on currency_code/transaction_status/card_network/
    country_code (raw source systems rarely normalize casing themselves)
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from src.spark.config import BRONZE_TRANSACTIONS_PATH

SEED = 20260923
TOTAL_ROWS = 1000

MERCHANTS = [
    (f"MER-{i:04d}", name)
    for i, name in enumerate(
        [
            "Ada's Coffee House", "Riverside Grocers", "Northgate Electronics", "Blue Fox Diner",
            "Harbor Books", "Summit Sporting Goods", "Cedar Lane Pharmacy", "Fenwick Hardware",
            "Maple & Co Bakery", "Union Street Garage", "Willow Market", "Ironbridge Tools",
            "Golden Wok", "Silver Birch Florist", "Anchor Point Marina Supplies", "Bramblewood Toys",
            "Clearwater Optics", "Foxglove Tea Room", "Granite Hill Furniture", "Hollow Creek Cinema",
        ],
        start=1,
    )
]

MCC_CODES = ["5812", "5411", "5999", "5732", "4111", "5941", "5813", "5651"]
CURRENCIES = ["GBP", "EUR", "USD"]
STATUSES = ["APPROVED", "DECLINED", "PENDING", "REFUNDED"]
CARD_NETWORKS = ["VISA", "MASTERCARD", "AMEX"]
COUNTRIES = ["GB", "US", "DE", "FR", "IE", "ES"]

COLUMNS = [
    "transaction_id", "merchant_id", "merchant_name", "mcc", "currency_code",
    "amount", "transaction_timestamp", "transaction_status", "card_network", "country_code",
]


def _random_casing(value: str, rng: random.Random) -> str:
    """Raw source systems rarely normalize casing - simulate that."""
    choice = rng.random()
    if choice < 0.15:
        return value.lower()
    if choice < 0.25:
        return value.capitalize()
    return value


def _random_timestamp(rng: random.Random) -> str:
    base = datetime(2026, 6, 1)
    offset_days = rng.randint(0, 110)
    offset_seconds = rng.randint(0, 86399)
    return (base + timedelta(days=offset_days, seconds=offset_seconds)).strftime("%Y-%m-%dT%H:%M:%S")


def generate_rows(seed: int = SEED, total_rows: int = TOTAL_ROWS) -> list[dict[str, str]]:
    rng = random.Random(seed)
    rows: list[dict[str, str]] = []

    for i in range(1, total_rows + 1):
        merchant_id, merchant_name = rng.choice(MERCHANTS)
        row = {
            "transaction_id": f"TXN-{i:06d}",
            "merchant_id": merchant_id,
            "merchant_name": merchant_name,
            "mcc": rng.choice(MCC_CODES),
            "currency_code": _random_casing(rng.choice(CURRENCIES), rng),
            "amount": f"{rng.uniform(2.0, 450.0):.2f}",
            "transaction_timestamp": _random_timestamp(rng),
            "transaction_status": _random_casing(rng.choice(STATUSES), rng),
            "card_network": _random_casing(rng.choice(CARD_NETWORKS), rng),
            "country_code": _random_casing(rng.choice(COUNTRIES), rng),
        }
        rows.append(row)

    # --- Deliberately inject raw-data problems into a small, fixed subset ---
    defect_rng = random.Random(seed + 1)
    n = len(rows)

    for idx in defect_rng.sample(range(n), max(1, n * 2 // 100)):
        rows[idx]["transaction_id"] = ""  # missing transaction_id

    for idx in defect_rng.sample(range(n), max(1, n * 2 // 100)):
        rows[idx]["merchant_id"] = ""  # missing merchant_id

    for idx in defect_rng.sample(range(n), max(1, n * 3 // 100)):
        rows[idx]["amount"] = defect_rng.choice(["-15.00", "0.00", "N/A", ""])

    for idx in defect_rng.sample(range(n), max(1, n * 2 // 100)):
        rows[idx]["currency_code"] = defect_rng.choice(["", "XXX", "12"])

    for idx in defect_rng.sample(range(n), max(1, n * 2 // 100)):
        rows[idx]["transaction_timestamp"] = defect_rng.choice(["", "not-a-date", "2026-13-40T99:99:99"])

    for idx in defect_rng.sample(range(n), max(1, n * 2 // 100)):
        rows[idx]["transaction_status"] = defect_rng.choice(["", "UNKNOWN_STATE"])

    # exact duplicate transaction_id: repeat a handful of already-generated
    # rows (with their original transaction_id intact) as extra rows
    duplicate_source_indices = defect_rng.sample(
        [idx for idx in range(n) if rows[idx]["transaction_id"]], max(1, n * 1 // 100)
    )
    for idx in duplicate_source_indices:
        rows.append(dict(rows[idx]))

    defect_rng.shuffle(rows)
    return rows


def write_bronze_csv(rows: list[dict[str, str]], path: Path = BRONZE_TRANSACTIONS_PATH) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return path


def run() -> dict[str, int]:
    rows = generate_rows()
    path = write_bronze_csv(rows)

    ids = [r["transaction_id"] for r in rows if r["transaction_id"]]
    summary = {
        "total_rows": len(rows),
        "missing_transaction_id": sum(1 for r in rows if not r["transaction_id"]),
        "missing_merchant_id": sum(1 for r in rows if not r["merchant_id"]),
        "invalid_amount": sum(1 for r in rows if r["amount"] in ("-15.00", "0.00", "N/A", "")),
        "invalid_currency_code": sum(1 for r in rows if r["currency_code"] in ("", "XXX", "12")),
        "invalid_timestamp": sum(1 for r in rows if r["transaction_timestamp"] in ("", "not-a-date", "2026-13-40T99:99:99")),
        "invalid_status": sum(1 for r in rows if r["transaction_status"] in ("", "UNKNOWN_STATE")),
        "duplicate_transaction_ids": len(ids) - len(set(ids)),
    }
    print(f"Bronze CSV written: {path}")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    return summary


if __name__ == "__main__":
    run()
