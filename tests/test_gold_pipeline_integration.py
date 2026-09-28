"""S14 — end-to-end Gold pipeline integration, against the real local
Silver/CDC Silver data already on disk from S3-S13. No Postgres/Kafka
required (Gold only reads already-materialized Parquet) — unlike the CDC
integration suites, this runs unconditionally.
"""

from pathlib import Path

import duckdb
import pytest

from src.gold.config import DUCKDB_PATH, FACTS_ROOT
from src.gold.pipeline import run
from src.gold.reconciliation import compute_control_totals


@pytest.fixture(scope="module")
def pipeline_result():
    return run()


def test_pipeline_reconciliation_all_pass(pipeline_result):
    for name, result in pipeline_result["reconciliation"].items():
        assert result["passed"] is True, f"{name} reconciliation failed: {result}"


def test_pipeline_control_total_identity_holds(pipeline_result):
    totals = pipeline_result["control_totals"]
    assert totals["approved_transaction_total"] == totals["settlement_total"] == totals["reconciliation_expected_total"]


def test_pipeline_control_total_is_computed_not_hardcoded(pipeline_result):
    # the figure must come out of real fact data — this test would still pass
    # even if the real underlying amount changed, unlike a hardcoded literal
    import pyarrow.parquet as pq

    facts = pq.read_table(FACTS_ROOT / "fact_transactions.parquet").to_pylist()
    settlements = pq.read_table(FACTS_ROOT / "fact_settlements.parquet").to_pylist()
    reconciliation = pq.read_table(FACTS_ROOT / "fact_reconciliation.parquet").to_pylist()
    recomputed = compute_control_totals(facts, settlements, reconciliation)
    assert str(recomputed["approved_transaction_total"]) == pipeline_result["control_totals"]["approved_transaction_total"]


def test_pipeline_writes_all_13_dimensions_and_5_facts(pipeline_result):
    assert len(pipeline_result["dimension_row_counts"]) == 13
    assert len(pipeline_result["fact_row_counts"]) == 5


def test_pipeline_fact_transactions_row_count_matches_cdc_silver():
    from src.silver.common import read_parquet

    result = run()
    source_count = len(read_parquet("data/silver_cdc/silver_cdc_transaction/data.parquet"))
    assert result["fact_row_counts"]["fact_transactions"] == source_count


def test_pipeline_is_idempotent_on_rerun():
    r1 = run()
    r2 = run()
    assert r1["dimension_row_counts"] == r2["dimension_row_counts"]
    assert r1["fact_row_counts"] == r2["fact_row_counts"]
    assert r1["control_totals"] == r2["control_totals"]


def test_pipeline_duckdb_file_created_and_queryable(pipeline_result):
    # As of S15, this module only owns the "main" schema (13 dims + 5
    # facts) — mart views (merchant_mart/transaction_mart/...) are now
    # built by the dbt project at dbt/, into their own "marts" schema, over
    # these same tables as dbt sources. See tests/test_dbt_gold_transformation.py
    # and docs/28-dbt-transformation-layer-design.md.
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        for expected in ["dim_date", "dim_merchant", "fact_transactions", "fact_settlements"]:
            assert expected in tables
    finally:
        con.close()


def test_pipeline_never_modifies_bronze_or_api_silver_or_watermarks():
    watched_paths = [
        Path("configs/sources.json"),
        Path("data/silver/merchant/data.parquet"),
        Path("data/silver/mcc/data.parquet"),
        Path("data/silver_cdc/silver_cdc_transaction/data.parquet"),
    ]
    watched_paths = [p for p in watched_paths if p.exists()]
    before = {p: p.stat().st_mtime_ns for p in watched_paths}
    run()
    after = {p: p.stat().st_mtime_ns for p in watched_paths}
    assert before == after


def test_pipeline_never_touches_bronze_or_watermark_directories():
    bronze_dir = Path("data/bronze")
    watermark_dir = Path("data/watermarks")
    before_bronze = sorted(p.stat().st_mtime_ns for p in bronze_dir.rglob("*") if p.is_file()) if bronze_dir.exists() else []
    before_watermarks = sorted(p.stat().st_mtime_ns for p in watermark_dir.rglob("*") if p.is_file()) if watermark_dir.exists() else []
    run()
    after_bronze = sorted(p.stat().st_mtime_ns for p in bronze_dir.rglob("*") if p.is_file()) if bronze_dir.exists() else []
    after_watermarks = sorted(p.stat().st_mtime_ns for p in watermark_dir.rglob("*") if p.is_file()) if watermark_dir.exists() else []
    assert before_bronze == after_bronze
    assert before_watermarks == after_watermarks
