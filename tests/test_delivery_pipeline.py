"""Client Data Delivery pipeline tests — controlled fixture DuckDB only, never
data/gold/gold.duckdb. The dataset registry and client entitlements are the
REAL, static, version-controlled configs/*.json files (immutable, safe to
depend on); only the mutable data source (the DuckDB file itself) is fully
isolated per test via tmp_path.
"""

import json
from pathlib import Path

import duckdb
import pyarrow.csv as pa_csv
import pyarrow.parquet as pa_parquet
import pytest

from src.delivery import pipeline
from src.delivery.errors import DeliveryValidationError, UnauthorizedDatasetError, UnknownClientError


def _build_fixture_duckdb(path: Path) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE SCHEMA marts")

        con.execute(
            """CREATE TABLE marts.transaction_mart (
                full_date DATE, year INTEGER, month INTEGER, month_name VARCHAR,
                currency_code VARCHAR, transaction_status VARCHAR,
                transaction_count BIGINT, amount_total DECIMAL(18,2)
            )"""
        )
        con.execute(
            """INSERT INTO marts.transaction_mart VALUES
                ('2026-01-01', 2026, 1, 'January', 'GBP', 'APPROVED', 10, 1000.00),
                ('2026-01-02', 2026, 1, 'January', 'GBP', 'APPROVED', 5, 500.00),
                ('2026-01-02', 2026, 1, 'January', 'GBP', 'DECLINED', 2, 200.00)"""
        )

        con.execute(
            """CREATE TABLE marts.settlement_mart (
                settlement_batch_id VARCHAR, currency_code VARCHAR,
                batch_start_date DATE, batch_end_date DATE, settlement_count BIGINT,
                settlement_amount_total DECIMAL(18,2), fee_amount_total DECIMAL(18,2),
                net_amount_total DECIMAL(18,2)
            )"""
        )
        con.execute(
            """INSERT INTO marts.settlement_mart VALUES
                ('BATCH-1', 'GBP', '2026-01-01', '2026-01-02', 15, 1500.00, 15.00, 1485.00)"""
        )

        con.execute(
            """CREATE TABLE marts.reconciliation_mart (
                reconciled_date DATE, year INTEGER, month INTEGER, month_name VARCHAR,
                match_status VARCHAR, reconciliation_count BIGINT,
                expected_amount_total DECIMAL(18,2), actual_amount_total DECIMAL(18,2),
                variance_total DECIMAL(18,2)
            )"""
        )
        con.execute(
            """INSERT INTO marts.reconciliation_mart VALUES
                ('2026-01-01', 2026, 1, 'January', 'MATCHED', 15, 1500.00, 1500.00, 0.00)"""
        )

        con.execute(
            """CREATE TABLE marts.merchant_mart (
                merchant_key BIGINT, merchant_id VARCHAR, merchant_name VARCHAR,
                mcc_code VARCHAR, mcc_description VARCHAR, transaction_count BIGINT,
                approved_amount_total DECIMAL(18,2), declined_count BIGINT
            )"""
        )
        con.execute(
            """INSERT INTO marts.merchant_mart VALUES
                (1, 'node:1', 'Test Bakery', '5812', 'Eating places', 10, 900.00, 1),
                (2, 'node:2', 'Test Cafe', '5814', 'Fast food', 5, 400.00, 0)"""
        )

        con.execute(
            """CREATE TABLE marts.client_program_mart (
                client_key BIGINT, client_id VARCHAR, client_legal_name VARCHAR,
                program_key BIGINT, program_id VARCHAR, program_name VARCHAR,
                program_type VARCHAR, transaction_count BIGINT,
                approved_amount_total DECIMAL(18,2), declined_count BIGINT
            )"""
        )
        con.execute(
            """INSERT INTO marts.client_program_mart VALUES
                (7, 'CLI-0007', 'VIRGIN HOLIDAYS LIMITED', 1, 'PRG-0001', 'Program 1', 'CASHBACK', 100, 5000.00, 5),
                (7, 'CLI-0007', 'VIRGIN HOLIDAYS LIMITED', 7, 'PRG-0007', 'Program 7', 'CASHBACK', 120, 6000.00, 3),
                (8, 'CLI-0008', 'THE JONATHAN PHILIP SWEET PERSONAL INJURY SETTLEMENT', 2, 'PRG-0002', 'Program 2', 'LOYALTY', 90, 4500.00, 2)"""
        )
    finally:
        con.close()


@pytest.fixture()
def gold_db(tmp_path):
    db_path = tmp_path / "gold.duckdb"
    _build_fixture_duckdb(db_path)
    return db_path


@pytest.fixture()
def outbox(tmp_path):
    return tmp_path / "outbox"


# ---- CSV / Parquet generation, schema, row count, control total, manifest ----


def test_csv_generation_for_transaction_mart(gold_db, outbox):
    result = pipeline.run(client_id="CLI-0001", dataset_name="transaction_mart", formats=["csv"], gold_duckdb_path=gold_db, outbox_root=outbox)
    assert result["status"] == "delivered"
    csv_path = Path(result["output_dir"]) / "transaction_mart.csv"
    assert csv_path.exists()
    table = pa_csv.read_csv(csv_path)
    assert table.num_rows == 3
    assert table.column_names == ["full_date", "year", "month", "month_name", "currency_code", "transaction_status", "transaction_count", "amount_total"]


def test_parquet_generation_for_merchant_mart(gold_db, outbox):
    result = pipeline.run(client_id="CLI-0007", dataset_name="merchant_mart", gold_duckdb_path=gold_db, outbox_root=outbox)
    parquet_path = Path(result["output_dir"]) / "merchant_mart.parquet"
    assert parquet_path.exists()
    table = pa_parquet.read_table(parquet_path)
    assert table.num_rows == 2


def test_correct_row_count_matches_source(gold_db, outbox):
    result = pipeline.run(client_id="CLI-0001", dataset_name="reconciliation_mart", gold_duckdb_path=gold_db, outbox_root=outbox)
    assert result["record_count"] == 1
    assert result["manifest"]["record_count"] == 1


def test_correct_business_grain_no_duplicates(gold_db, outbox):
    result = pipeline.run(client_id="CLI-0001", dataset_name="transaction_mart", gold_duckdb_path=gold_db, outbox_root=outbox)
    table = pa_csv.read_csv(Path(result["output_dir"]) / "transaction_mart.csv")
    grain = list(zip(table.column("full_date").to_pylist(), table.column("currency_code").to_pylist(), table.column("transaction_status").to_pylist()))
    assert len(set(grain)) == len(grain) == 3


def test_correct_control_total_transaction_mart(gold_db, outbox):
    # only the two APPROVED rows (1000.00 + 500.00) count toward the control total
    result = pipeline.run(client_id="CLI-0001", dataset_name="transaction_mart", gold_duckdb_path=gold_db, outbox_root=outbox)
    assert result["manifest"]["control_total"] == "1500.00"


def test_control_total_not_applicable_is_explicit_when_no_column(gold_db, outbox, monkeypatch):
    from src.delivery import config as delivery_config

    registry = delivery_config.load_dataset_registry()
    registry["merchant_mart"] = {**registry["merchant_mart"], "control_total_column": None}
    # pipeline.py imported get_dataset_definition by value (`from ... import
    # get_dataset_definition`), so the reference actually used inside
    # pipeline.run() lives in pipeline's own namespace, not config's.
    monkeypatch.setattr(pipeline, "get_dataset_definition", lambda name, r=None: registry[name])
    result = pipeline.run(client_id="CLI-0007", dataset_name="merchant_mart", gold_duckdb_path=gold_db, outbox_root=outbox)
    assert result["manifest"]["control_total"] == "NOT_APPLICABLE"


def test_manifest_generated_with_required_fields(gold_db, outbox):
    result = pipeline.run(client_id="CLI-0001", dataset_name="settlement_mart", gold_duckdb_path=gold_db, outbox_root=outbox)
    manifest = result["manifest"]
    for field in [
        "run_id", "client_id", "dataset", "schema_version", "generated_at_utc",
        "reporting_period", "output_formats", "record_count", "control_total",
        "dq_status", "validation_status",
    ]:
        assert field in manifest, f"manifest missing '{field}'"
    assert manifest["validation_status"] == "PASSED"
    assert manifest["reporting_period"] == {"start": "2026-01-01", "end": "2026-01-02"}


def test_manifest_matches_generated_file_on_disk(gold_db, outbox):
    result = pipeline.run(client_id="CLI-0001", dataset_name="transaction_mart", formats=["csv"], gold_duckdb_path=gold_db, outbox_root=outbox)
    manifest_path = Path(result["output_dir"]) / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    table = pa_csv.read_csv(Path(result["output_dir"]) / "transaction_mart.csv")
    assert manifest["record_count"] == table.num_rows


def test_csv_parquet_logical_consistency_where_both_exist(gold_db, outbox):
    from decimal import Decimal

    result = pipeline.run(client_id="CLI-0001", dataset_name="transaction_mart", formats=["csv", "parquet"], gold_duckdb_path=gold_db, outbox_root=outbox)
    csv_table = pa_csv.read_csv(Path(result["output_dir"]) / "transaction_mart.csv")
    parquet_table = pa_parquet.read_table(Path(result["output_dir"]) / "transaction_mart.parquet")
    assert csv_table.num_rows == parquet_table.num_rows
    # CSV's own writer drops decimal128's fixed-scale zero-padding ("1000" not
    # "1000.00") — a real, harmless formatting difference, not a value
    # difference, so compare numerically (Decimal) rather than by raw string.
    csv_values = sorted(Decimal(v) for v in csv_table.column("amount_total").cast("string").to_pylist())
    parquet_values = sorted(Decimal(str(v)) for v in parquet_table.column("amount_total").to_pylist())
    assert csv_values == parquet_values


# ---- reporting period without a date column ----


def test_reporting_period_documents_no_date_semantics_for_merchant_mart(gold_db, outbox):
    result = pipeline.run(client_id="CLI-0007", dataset_name="merchant_mart", gold_duckdb_path=gold_db, outbox_root=outbox)
    assert result["manifest"]["reporting_period"] == {"semantics": "full_snapshot_no_date_dimension_in_mart"}


# ---- client entitlement filtering ----


def test_client_program_mart_filtered_to_only_that_clients_rows(gold_db, outbox):
    result = pipeline.run(client_id="CLI-0007", dataset_name="client_program_mart", gold_duckdb_path=gold_db, outbox_root=outbox)
    table = pa_parquet.read_table(Path(result["output_dir"]) / "client_program_mart.parquet")
    assert set(table.column("client_id").to_pylist()) == {"CLI-0007"}
    assert table.num_rows == 2  # CLI-0007 has 2 programs in the fixture


def test_different_clients_receive_only_their_authorized_rows(gold_db, outbox):
    result_a = pipeline.run(client_id="CLI-0007", dataset_name="client_program_mart", gold_duckdb_path=gold_db, outbox_root=outbox)
    result_b = pipeline.run(client_id="CLI-0008", dataset_name="client_program_mart", gold_duckdb_path=gold_db, outbox_root=outbox)

    table_a = pa_parquet.read_table(Path(result_a["output_dir"]) / "client_program_mart.parquet")
    table_b = pa_parquet.read_table(Path(result_b["output_dir"]) / "client_program_mart.parquet")

    assert set(table_a.column("client_id").to_pylist()) == {"CLI-0007"}
    assert set(table_b.column("client_id").to_pylist()) == {"CLI-0008"}
    assert table_a.num_rows == 2
    assert table_b.num_rows == 1


def test_unauthorized_dataset_delivery_is_rejected(gold_db, outbox):
    # CLI-0007 is a PROGRAM_OWNER, entitled to client_program_mart/merchant_mart
    # only — not settlement_mart (see configs/client_entitlements.json).
    with pytest.raises(UnauthorizedDatasetError):
        pipeline.run(client_id="CLI-0007", dataset_name="settlement_mart", gold_duckdb_path=gold_db, outbox_root=outbox)
    assert not (outbox / "CLI-0007" / "settlement_mart").exists()


def test_unknown_client_is_rejected(gold_db, outbox):
    with pytest.raises(UnknownClientError):
        pipeline.run(client_id="CLI-DOES-NOT-EXIST", dataset_name="transaction_mart", gold_duckdb_path=gold_db, outbox_root=outbox)


# ---- validation failure prevents delivery ----


def test_validation_failure_leaves_no_deliverable_in_outbox(gold_db, outbox):
    # Inject a duplicate business-grain row directly into the fixture DB.
    con = duckdb.connect(str(gold_db))
    try:
        con.execute(
            "INSERT INTO marts.transaction_mart VALUES ('2026-01-01', 2026, 1, 'January', 'GBP', 'APPROVED', 99, 999.00)"
        )
    finally:
        con.close()

    with pytest.raises(DeliveryValidationError):
        pipeline.run(client_id="CLI-0001", dataset_name="transaction_mart", gold_duckdb_path=gold_db, outbox_root=outbox)

    assert not (outbox / "CLI-0001" / "transaction_mart").exists()


# ---- idempotency ----


def test_rerun_same_delivery_is_recognized_and_skipped(gold_db, outbox):
    first = pipeline.run(client_id="CLI-0001", dataset_name="transaction_mart", gold_duckdb_path=gold_db, outbox_root=outbox)
    assert first["status"] == "delivered"

    second = pipeline.run(client_id="CLI-0001", dataset_name="transaction_mart", gold_duckdb_path=gold_db, outbox_root=outbox)
    assert second["status"] == "skipped_existing"
    assert second["existing_run_id"] == first["run_id"]

    # exactly one run directory exists — no duplicate/conflicting delivery
    run_dirs = list((outbox / "CLI-0001" / "transaction_mart").iterdir())
    assert len(run_dirs) == 1


def test_force_rerun_creates_a_new_run_id_without_deleting_the_prior_one(gold_db, outbox):
    first = pipeline.run(client_id="CLI-0001", dataset_name="transaction_mart", gold_duckdb_path=gold_db, outbox_root=outbox)
    second = pipeline.run(client_id="CLI-0001", dataset_name="transaction_mart", gold_duckdb_path=gold_db, outbox_root=outbox, force=True)

    assert second["status"] == "delivered"
    assert second["run_id"] != first["run_id"]
    run_dirs = {p.name for p in (outbox / "CLI-0001" / "transaction_mart").iterdir()}
    assert first["run_id"] in run_dirs
    assert second["run_id"] in run_dirs


# ---- determinism ----


def test_csv_output_is_deterministic_across_regenerations(gold_db, tmp_path):
    outbox_a = tmp_path / "outbox_a"
    outbox_b = tmp_path / "outbox_b"
    result_a = pipeline.run(client_id="CLI-0001", dataset_name="transaction_mart", formats=["csv"], gold_duckdb_path=gold_db, outbox_root=outbox_a)
    result_b = pipeline.run(client_id="CLI-0001", dataset_name="transaction_mart", formats=["csv"], gold_duckdb_path=gold_db, outbox_root=outbox_b)

    content_a = (Path(result_a["output_dir"]) / "transaction_mart.csv").read_text(encoding="utf-8")
    content_b = (Path(result_b["output_dir"]) / "transaction_mart.csv").read_text(encoding="utf-8")
    assert content_a == content_b
