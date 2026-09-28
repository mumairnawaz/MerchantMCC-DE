"""Synthetic OLTP business-data foundation (Phase S9).

Represents the data that would live in the future PostgreSQL synthetic OLTP
source system (docs/22-synthetic-business-data-design.md) — the operational
system Bronze ingestion will eventually extract from, NOT Bronze or Silver
themselves. This package never imports from src.ingestion and never writes to
data/bronze/ or data/silver/; it only *reads* already-published Silver Parquet
output as reference dimensions (src.synthetic.reference) and *writes* to
data/synthetic_oltp/.
"""
