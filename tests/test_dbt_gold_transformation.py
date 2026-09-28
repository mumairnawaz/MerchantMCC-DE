"""S15-A — dbt transformation layer over Gold (dbt/). Runs the real dbt CLI
against a real, but ISOLATED, copy of the dbt/finpay_gold project and its own
dedicated gold.duckdb — no mocks, consistent with this project's testing
philosophy, but never the production data/gold/gold.duckdb file. Requires no
live infrastructure (Postgres/Kafka), only Python + dbt-duckdb + the Gold
Parquet already produced by src/gold/pipeline.py.

Isolation rationale (API Automation Test Failures audit): this module
previously ran `src.gold.pipeline.run()` + a real `dbt build` against the
SAME data/gold/gold.duckdb file that the live Airflow environment's
gold_rebuild/dbt_build tasks write to every 15 minutes. Whenever this
module's own run collided with a concurrently-running Airflow task, DuckDB's
single-writer file lock raised an exception during fixture setup — surfacing
as ERROR (not FAILED) on every test sharing that fixture. Running against a
copied dbt/ project + a dedicated gold.duckdb under tmp_path eliminates the
collision entirely without touching the production Gold pipeline or dbt
project themselves.
"""

import contextlib
import shutil
import subprocess
from pathlib import Path

import duckdb
import pytest

from src.gold.pipeline import run as run_gold_pipeline

REPO_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = REPO_ROOT / "dbt"
DBT_EXE = REPO_ROOT / ".venv" / "Scripts" / "dbt.exe"


@contextlib.contextmanager
def _gold_duckdb_path_override(path: Path):
    """Test-only monkeypatch of the module-level DUCKDB_PATH constant both
    src.gold.config and src.gold.pipeline hold a reference to — never writes
    to either file, and always restores the original value in `finally` so
    no other test in the same pytest session (e.g. tests/test_gold_pipeline_
    integration.py) ever sees the patched path."""
    import src.gold.config as gold_config
    import src.gold.pipeline as gold_pipeline

    original_config_path = gold_config.DUCKDB_PATH
    original_pipeline_path = gold_pipeline.DUCKDB_PATH
    gold_config.DUCKDB_PATH = path
    gold_pipeline.DUCKDB_PATH = path
    try:
        yield
    finally:
        gold_config.DUCKDB_PATH = original_config_path
        gold_pipeline.DUCKDB_PATH = original_pipeline_path


def _dbt(dbt_dir: Path, *args: str) -> subprocess.CompletedProcess:
    # cwd must be the dbt project dir: dbt-duckdb resolves profiles.yml's
    # relative `path:` against the process's current working directory, not
    # against --project-dir (verified in S15-A — passing --project-dir alone
    # from a different cwd silently resolves gold.duckdb's path wrong).
    return subprocess.run(
        [str(DBT_EXE), *args, "--project-dir", str(dbt_dir), "--profiles-dir", str(dbt_dir)],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(dbt_dir),
    )


@pytest.fixture(scope="module")
def dbt_sandbox(tmp_path_factory):
    """Isolated copy of dbt/ plus its own gold.duckdb, built once per module.
    The copy is placed at the SAME relative position (`<sandbox>/dbt/` next to
    `<sandbox>/data/gold/gold.duckdb`) that dbt/profiles.yml's own unmodified
    `path: "../data/gold/gold.duckdb"` already expects — no content change to
    the copied profiles.yml is needed for it to resolve correctly."""
    sandbox = tmp_path_factory.mktemp("dbt_gold_isolated")
    dbt_dir = sandbox / "dbt"
    duckdb_path = sandbox / "data" / "gold" / "gold.duckdb"

    shutil.copytree(
        DBT_PROJECT_DIR,
        dbt_dir,
        ignore=shutil.ignore_patterns("target", "dbt_packages", "logs", ".user.yml"),
    )

    with _gold_duckdb_path_override(duckdb_path):
        run_gold_pipeline()

    return dbt_dir, duckdb_path


@pytest.fixture(scope="module")
def dbt_build_result(dbt_sandbox):
    """Gold must exist before dbt can read it as a source — dbt_sandbox
    already rebuilt it (isolated) — then run the full dbt DAG (models +
    tests) in one pass, also against the isolated project/database."""
    dbt_dir, _duckdb_path = dbt_sandbox
    return _dbt(dbt_dir, "build")


def test_dbt_build_succeeds(dbt_build_result):
    assert dbt_build_result.returncode == 0, dbt_build_result.stdout + dbt_build_result.stderr


def test_dbt_build_ran_all_28_models_and_45_tests_with_zero_failures(dbt_build_result):
    output = dbt_build_result.stdout
    assert "PASS=73" in output
    assert "ERROR=0" in output
    assert "TOTAL=73" in output


def test_dbt_creates_staging_intermediate_marts_schemas(dbt_build_result, dbt_sandbox):
    _dbt_dir, duckdb_path = dbt_sandbox
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        schemas = {row[0] for row in con.execute("SELECT DISTINCT schema_name FROM information_schema.schemata").fetchall()}
        for expected in ["staging", "intermediate", "marts", "main"]:
            assert expected in schemas
    finally:
        con.close()


def test_dbt_staging_has_18_thin_models_over_gold_sources(dbt_build_result, dbt_sandbox):
    _dbt_dir, duckdb_path = dbt_sandbox
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        count = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='staging'").fetchone()[0]
        assert count == 18
    finally:
        con.close()


def test_dbt_marts_schema_has_all_6_implemented_marts(dbt_build_result, dbt_sandbox):
    _dbt_dir, duckdb_path = dbt_sandbox
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        tables = {row[0] for row in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='marts'").fetchall()}
        assert tables == {
            "merchant_mart",
            "transaction_mart",
            "client_program_mart",
            "settlement_mart",
            "reconciliation_mart",
            "rewards_mart",
        }
    finally:
        con.close()


def test_dbt_control_total_identity_holds_across_marts(dbt_build_result, dbt_sandbox):
    _dbt_dir, duckdb_path = dbt_sandbox
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        approved = con.execute("SELECT SUM(amount_total) FROM marts.transaction_mart WHERE transaction_status = 'APPROVED'").fetchone()[0]
        settlement = con.execute("SELECT SUM(settlement_amount_total) FROM marts.settlement_mart").fetchone()[0]
        reconciliation = con.execute("SELECT SUM(expected_amount_total) FROM marts.reconciliation_mart").fetchone()[0]
        assert approved == settlement == reconciliation
    finally:
        con.close()


def test_dbt_control_total_matches_python_gold_pipeline_result(dbt_build_result, dbt_sandbox):
    _dbt_dir, duckdb_path = dbt_sandbox
    with _gold_duckdb_path_override(duckdb_path):
        gold_result = run_gold_pipeline()
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        approved = con.execute("SELECT SUM(amount_total) FROM marts.transaction_mart WHERE transaction_status = 'APPROVED'").fetchone()[0]
        assert str(approved) == gold_result["control_totals"]["approved_transaction_total"]
    finally:
        con.close()


def test_dbt_merchant_mart_row_count_matches_real_merchant_overlap(dbt_build_result, dbt_sandbox):
    # verified in S14 (docs/27 §3): 520/520 distinct CDC transaction merchant_ids
    # resolve against real silver_merchant — merchant_mart should have exactly
    # that many rows (one per merchant that actually has a transaction).
    _dbt_dir, duckdb_path = dbt_sandbox
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        count = con.execute("SELECT COUNT(*) FROM marts.merchant_mart").fetchone()[0]
        assert count == 520
    finally:
        con.close()


def test_dbt_rebuild_is_idempotent(dbt_sandbox):
    dbt_dir, duckdb_path = dbt_sandbox
    first = _dbt(dbt_dir, "build")
    assert first.returncode == 0
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        before = con.execute("SELECT SUM(approved_amount_total) FROM marts.merchant_mart").fetchone()[0]
    finally:
        con.close()

    second = _dbt(dbt_dir, "build")
    assert second.returncode == 0
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        after = con.execute("SELECT SUM(approved_amount_total) FROM marts.merchant_mart").fetchone()[0]
    finally:
        con.close()
    assert before == after


def test_dbt_never_modifies_bronze_api_silver_or_cdc_silver(dbt_sandbox):
    dbt_dir, _duckdb_path = dbt_sandbox
    watched_paths = [
        Path("configs/sources.json"),
        Path("data/silver/merchant/data.parquet"),
        Path("data/silver_cdc/silver_cdc_transaction/data.parquet"),
    ]
    watched_paths = [p for p in watched_paths if p.exists()]
    before = {p: p.stat().st_mtime_ns for p in watched_paths}
    result = _dbt(dbt_dir, "build")
    assert result.returncode == 0
    after = {p: p.stat().st_mtime_ns for p in watched_paths}
    assert before == after
