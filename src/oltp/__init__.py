"""FinPay OLTP layer (Phase S10) — a real, runnable PostgreSQL database
representing the FinPay operational system, seeded from the S9 synthetic
Parquet data.

This package never imports src.ingestion or src.silver's transformation
modules, and never writes to data/bronze/, data/watermarks/, or data/silver/.
It only reads data/synthetic_oltp/*.parquet (S9's output) and writes to the
PostgreSQL database configured via environment variables (src.oltp.config).

Architectural position (docs/23-oltp-postgresql-design.md):
    Synthetic FinPay Application (S9) -> PostgreSQL OLTP (S10, this package)
    -> [future S11+] Debezium CDC -> Kafka -> Bronze -> Silver -> ...
No CDC/Kafka/Debezium code exists here — S10 stops at the OLTP database itself.
"""
