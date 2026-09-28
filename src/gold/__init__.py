"""FinPay Gold/OLAP layer (Phase S14): integrates API Silver (S3-S8, external
reference data) and CDC Silver (S13, internal operational data) into a
dimensional model.

OLAP technology: DuckDB (embedded, free, open-source) — see
docs/27-gold-olap-design.md §5 for the full decision record. Gold tables are
materialized as Parquet under data/gold/{dimensions,facts}/ (the durable,
portable artifact, consistent with every other layer in this project) and
loaded into a DuckDB database file (data/gold/gold.duckdb) for SQL access and
the data-mart views.

This package reads API Silver (data/silver/) and CDC Silver (data/silver_cdc/)
as READ-ONLY inputs. It never writes to either, never imports src.ingestion,
and never touches data/bronze/, data/bronze_cdc/, or data/watermarks/.
"""
