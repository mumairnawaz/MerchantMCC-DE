"""MerchantMCC-DE — External API ingestion/transformation orchestration DAG.

Companion to dags/merchantmcc_cdc_pipeline.py, deliberately a SEPARATE DAG with
its own daily schedule (API Ingestion + Airflow Orchestration Audit, §11):
the 3 live API sources (OpenStreetMap, Frankfurter, GLEIF) update at most
daily — running them on the CDC pipeline's 15-minute cadence would be
wasteful against free, shared, community APIs for zero business benefit.

Airflow is the ORCHESTRATOR only. Every task below calls an existing,
already-implemented, already-tested entry point (src/ingestion/*.run(),
src/silver/*.run()) — no business/data-processing logic is duplicated here.

DESIGN B (per the audit, §4/§7): one Bronze/Silver task pair per source, not
one monolithic api_bronze/api_silver task pair — better failure isolation and
observability (a GLEIF pagination failure, say, is visible as exactly one red
task, not a generic "api_bronze" task covering all three unrelated sources).
merchant_osm_bronze >> merchant_silver, currency_bronze >> fx_rate_silver,
gleif_bronze >> legal_entity_silver — three independent Bronze->Silver chains,
matching §10's requirement that each source's Silver task only run after its
own corresponding Bronze task, not after every source's Bronze task.

Reference sources (MCC, country, ISO 4217, BIN/IIN issuer) are deliberately
NOT included — the audit found these "slow-changing... low-frequency manual
refresh" sources have no incremental/update-frequency rationale for being on
any recurring schedule (§11) and remain manual/ad-hoc, unchanged by this DAG.

DELIBERATELY DOES NOT include gold_rebuild/dbt_build tasks. Reasoning (this is
the "shared downstream processing" decision the API automation task asked to
be resolved before implementing, not silently assumed):

  - src/gold/dimensions.py::_api_silver() already reads whatever is currently
    on disk in data/silver/<dataset>/ UNCONDITIONALLY on every invocation,
    tolerating a missing/stale file (returns [] rather than raising) — this
    was verified directly in the audit and re-confirmed by this task's own
    baseline read (dim_merchant/dim_legal_entity are populated right now from
    data last ingested over a week ago). Gold's gold_rebuild task, wherever it
    runs, always incorporates whatever is CURRENTLY in API Silver — it does
    not need a literal dependency edge from api_silver to "wait for" it,
    because it never blocks on freshness in the first place.
  - The existing merchantmcc_cdc_pipeline DAG already runs gold_rebuild (and
    dbt_build immediately after it) unconditionally every 15 minutes, on a
    schedule already proven to work in production. Adding a second,
    independent gold_rebuild/dbt_build task pair here — on a different
    schedule — would mean two DAGs each capable of invoking
    src.gold.pipeline.run() (a full DuckDB rebuild) at unpredictable,
    potentially overlapping times: a real risk of two processes writing to
    the same gold.duckdb file concurrently. That risk buys nothing, because
    of the point above — Gold already picks up fresh API Silver data on its
    very next 15-minute tick regardless of which DAG produced it.
  - Net effect: this DAG's own Silver tasks finishing is enough. The next
    scheduled run of merchantmcc_cdc_pipeline (at most ~15 minutes later)
    rebuilds Gold/dbt using the now-fresher API Silver data alongside
    whatever CDC Silver currently has — exactly the "Gold waits for both
    branches" outcome the task asked for, achieved by Gold's existing
    unconditional-read behavior rather than a new cross-DAG trigger. A
    TriggerDagRunOperator-based alternative (this DAG explicitly kicking off
    merchantmcc_cdc_pipeline's gold_rebuild immediately on completion) was
    considered and rejected: it adds real complexity (a second entry point
    into the same DAG, a race against that DAG's own natural 15-minute
    trigger, given both share max_active_runs=1) to buy, at most, a ~15-minute
    reduction in staleness for data that only changes once a day — not
    justified.
"""

import logging
import os

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

logger = logging.getLogger(__name__)

PROJECT_ROOT = "/opt/airflow/project"


def _chdir_to_project_root() -> None:
    """Same real defect/fix documented in dags/merchantmcc_cdc_pipeline.py:
    src/ingestion/* and src/silver/* modules use paths relative to the
    project root (data/bronze/..., data/silver/..., data/watermarks/...) — the
    base Airflow image's own WORKDIR is /opt/airflow, not this project's
    mounted root, so a PythonOperator task inherits the wrong cwd unless
    told otherwise."""
    os.chdir(PROJECT_ROOT)


def _run_merchant_osm_bronze(**context) -> None:
    """Calls the existing src.ingestion.merchant_osm.run() unchanged."""
    _chdir_to_project_root()
    from src.ingestion.merchant_osm import run as merchant_osm_run

    run_dir = merchant_osm_run()
    logger.info("merchant_osm_bronze: completed -> %s", run_dir)


def _run_currency_bronze(**context) -> None:
    """Calls the existing src.ingestion.currency.run() unchanged. May return
    None (Frankfurter's own "watermark already current" short-circuit,
    src/ingestion/currency.py) — not an error, nothing to do."""
    _chdir_to_project_root()
    from src.ingestion.currency import run as currency_run

    run_dir = currency_run()
    logger.info("currency_bronze: completed -> %s", run_dir)


def _run_gleif_bronze(**context) -> None:
    """Calls the existing src.ingestion.gleif.run() unchanged."""
    _chdir_to_project_root()
    from src.ingestion.gleif import run as gleif_run

    run_dir = gleif_run()
    logger.info("gleif_bronze: completed -> %s", run_dir)


def _run_merchant_silver(**context) -> None:
    """Calls the existing src.silver.merchant.run() unchanged. Reads the
    LATEST merchant_osm Bronze run only (src/silver/common.py::latest_bronze_run)
    — this task's own upstream dependency on merchant_osm_bronze (below)
    guarantees that "latest" is the run this same scheduled DAG run just
    produced, per the audit's §10 requirement."""
    _chdir_to_project_root()
    from src.silver.merchant import run as merchant_silver_run

    result = merchant_silver_run()
    logger.info("merchant_silver: completed -> %s", result)


def _run_fx_rate_silver(**context) -> None:
    """Calls the existing src.silver.fx_rate.run() unchanged."""
    _chdir_to_project_root()
    from src.silver.fx_rate import run as fx_rate_silver_run

    result = fx_rate_silver_run()
    logger.info("fx_rate_silver: completed -> %s", result)


def _run_legal_entity_silver(**context) -> None:
    """Calls the existing src.silver.legal_entity.run() unchanged."""
    _chdir_to_project_root()
    from src.silver.legal_entity import run as legal_entity_silver_run

    result = legal_entity_silver_run()
    logger.info("legal_entity_silver: completed -> %s", result)


default_args = {
    "owner": "merchantmcc-de",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="merchantmcc_api_pipeline",
    description=(
        "MerchantMCC-DE: External API ingestion (OpenStreetMap/Frankfurter/GLEIF) "
        "-> API Bronze -> API Silver. Gold/dbt are deliberately NOT included here "
        "— see module docstring; the existing merchantmcc_cdc_pipeline DAG's "
        "15-minute gold_rebuild/dbt_build already picks up this DAG's output."
    ),
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 9, 27, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["merchantmcc", "api", "bronze", "silver"],
) as dag:
    merchant_osm_bronze = PythonOperator(
        task_id="merchant_osm_bronze",
        python_callable=_run_merchant_osm_bronze,
    )
    currency_bronze = PythonOperator(
        task_id="currency_bronze",
        python_callable=_run_currency_bronze,
    )
    gleif_bronze = PythonOperator(
        task_id="gleif_bronze",
        python_callable=_run_gleif_bronze,
    )

    merchant_silver = PythonOperator(
        task_id="merchant_silver",
        python_callable=_run_merchant_silver,
    )
    fx_rate_silver = PythonOperator(
        task_id="fx_rate_silver",
        python_callable=_run_fx_rate_silver,
    )
    legal_entity_silver = PythonOperator(
        task_id="legal_entity_silver",
        python_callable=_run_legal_entity_silver,
    )

    merchant_osm_bronze >> merchant_silver
    currency_bronze >> fx_rate_silver
    gleif_bronze >> legal_entity_silver
