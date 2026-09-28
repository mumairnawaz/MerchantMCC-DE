"""MerchantMCC-DE — CDC -> Gold -> dbt orchestration DAG.

Airflow is the ORCHESTRATOR only. Every task below calls an existing,
already-implemented, already-tested entry point (src/cdc/consumer.py,
src/cdc/silver.py, src/gold/pipeline.py, `dbt build`) — no business/data-
processing logic is duplicated here.

Dependency chain (matches the CDC/Gold/dbt Airflow Orchestration Audit's
recommended architecture exactly, docs/ Airflow audit report):

    cdc_bronze >> cdc_silver >> gold_rebuild >> dbt_build

Scheduled batch, not streaming/sensor-based: cdc_bronze already drains
whatever Kafka currently has and exits (a finite batch job, not a daemon)
- Airflow just needs to call it periodically, not wait for a message to
arrive. Zero new Kafka messages / zero new Bronze records is a normal,
successful outcome at every stage, not a failure condition.

Import safety: all `src.*` imports happen INSIDE each task callable, not
at module level. Importing these modules has no real side effect (no
Kafka/Postgres connection is opened merely by importing - only by calling
run()), but keeping them inside the callables removes any doubt and keeps
DAG-processor parsing fast, per standard Airflow authoring practice.
"""

import logging
import os

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

logger = logging.getLogger(__name__)

PROJECT_ROOT = "/opt/airflow/project"
DBT_PROJECT_DIR = f"{PROJECT_ROOT}/dbt"


def _chdir_to_project_root() -> None:
    """Real defect found during first-execution testing, fixed here (a DAG/
    orchestration-layer concern, not an src/ application-code change):
    every pipeline module (src/cdc/consumer.py, silver.py, src/gold/config.py)
    uses paths relative to the project root ("data/bronze_cdc", etc.) - a
    correct, established convention everywhere else this code runs (pytest
    with pythonpath=["."], spark-submit invoked from the repo root). The
    base Airflow image's own WORKDIR is /opt/airflow (AIRFLOW_HOME), not
    this project's mounted root, so a PythonOperator task inherits the
    WRONG cwd unless told otherwise - confirmed by a real run that silently
    wrote a full, isolated, empty-looking data tree under
    /opt/airflow/data/ instead of /opt/airflow/project/data/, while the
    real project data was untouched. BashOperator's dbt_build task never
    had this problem - its bash_command already `cd`s explicitly."""
    os.chdir(PROJECT_ROOT)


def _run_cdc_bronze(**context) -> dict:
    """Calls the existing src.cdc.consumer.run() unchanged. Persist-before-
    commit ordering, checkpointing, and the existing consumer group are
    entirely owned by that module - nothing here alters any of it."""
    _chdir_to_project_root()
    from src.cdc.consumer import run as bronze_run
    from src.cdc.consumer_config import settings as default_settings

    report = bronze_run(settings=default_settings)

    if not report.topics:
        logger.info("cdc_bronze: no new Kafka messages on any topic - nothing to do (success).")
        return {}

    summary = {}
    for topic, r in report.topics.items():
        logger.info(
            "cdc_bronze | topic=%s read=%d persisted=%d tombstones=%d dup_skip=%d quarantined=%d checkpoint_committed=%s",
            topic, r.events_read, r.events_persisted, r.tombstones_handled, r.duplicates_skipped, r.quarantined, r.checkpoint_committed,
        )
        summary[topic] = {
            "events_read": r.events_read,
            "events_persisted": r.events_persisted,
            "tombstones_handled": r.tombstones_handled,
            "duplicates_skipped": r.duplicates_skipped,
            "quarantined": r.quarantined,
            "checkpoint_committed": r.checkpoint_committed,
        }
    logger.info("cdc_bronze: completed - %d topic(s) had activity.", len(summary))
    return summary


def _run_cdc_silver(**context) -> dict:
    """Calls the existing src.cdc.silver.run() unchanged. LSN-based
    ordering, the deleted-keys memory, and quarantine routing are entirely
    owned by that module."""
    _chdir_to_project_root()
    from src.cdc.silver import run as silver_run

    results = silver_run()

    if not results:
        logger.info("cdc_silver: no tables processed - nothing to do (success).")
        return {}

    summary = {}
    for table, r in results.items():
        logger.info(
            "cdc_silver | table=%s read=%d inserted=%d updated=%d unchanged=%d deleted=%d stale_skipped=%d dup_skip=%d quarantined=%d",
            table, r.events_read, r.inserted, r.updated, r.unchanged, r.deleted, r.stale_skipped, r.duplicates_skipped, r.quarantined,
        )
        summary[table] = {
            "events_read": r.events_read,
            "inserted": r.inserted,
            "updated": r.updated,
            "unchanged": r.unchanged,
            "deleted": r.deleted,
            "stale_skipped": r.stale_skipped,
            "duplicates_skipped": r.duplicates_skipped,
            "quarantined": r.quarantined,
        }
    logger.info("cdc_silver: completed - %d table(s) processed.", len(summary))
    return summary


def _run_gold_rebuild(**context) -> dict:
    """Calls the existing src.gold.pipeline.run() unchanged - a
    deterministic full rebuild every run, exactly as designed in S14.
    Deliberately NOT wrapped in try/except: pipeline.run() already raises
    (AssertionError) on any reconciliation/integrity failure, and that
    exception must propagate to fail this Airflow task naturally, not be
    caught and suppressed."""
    _chdir_to_project_root()
    from src.gold.pipeline import run as gold_run

    summary = gold_run()

    logger.info("gold_rebuild | dimension_row_counts=%s", summary.get("dimension_row_counts"))
    logger.info("gold_rebuild | fact_row_counts=%s", summary.get("fact_row_counts"))
    logger.info("gold_rebuild | control_totals=%s", summary.get("control_totals"))
    reconciliation = summary.get("reconciliation", {})
    logger.info("gold_rebuild | all reconciliations passed=%s", all(r.get("passed") for r in reconciliation.values()))
    logger.info("gold_rebuild: completed.")
    return summary


default_args = {
    "owner": "merchantmcc-de",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="merchantmcc_cdc_pipeline",
    description="MerchantMCC-DE: PostgreSQL/Debezium/Kafka CDC -> CDC Bronze -> CDC Silver -> Gold -> dbt",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 9, 25, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["merchantmcc", "cdc", "gold", "dbt"],
) as dag:
    cdc_bronze = PythonOperator(
        task_id="cdc_bronze",
        python_callable=_run_cdc_bronze,
    )

    cdc_silver = PythonOperator(
        task_id="cdc_silver",
        python_callable=_run_cdc_silver,
    )

    gold_rebuild = PythonOperator(
        task_id="gold_rebuild",
        python_callable=_run_gold_rebuild,
    )

    # BashOperator, not a Python API wrapper: dbt is invoked as a CLI in
    # every other part of this project too, and dbt-duckdb resolves
    # profiles.yml's relative `path:` against the process's cwd (docs/28) -
    # `cd ... && dbt build --profiles-dir .` makes that cwd explicit and
    # correct, exactly matching how this command is run everywhere else in
    # this project.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt build --profiles-dir .",
    )

    cdc_bronze >> cdc_silver >> gold_rebuild >> dbt_build
