"""MerchantMCC-DE — Client Data Delivery orchestration DAG.

A third, separate DAG (alongside merchantmcc_cdc_pipeline and
merchantmcc_api_pipeline) — deliberately not a branch appended to either.
Reasoning (Client Data Delivery Audit §6, re-confirmed here before choosing
this DAG's trigger mechanism):

  - dbt marts (this DAG's only data source — see src/delivery/pipeline.py)
    are already rebuilt unconditionally every 15 minutes by
    merchantmcc_cdc_pipeline's own gold_rebuild/dbt_build tasks. By the time
    this DAG's own daily schedule fires, the marts are already at least as
    fresh as the last CDC tick and the last API-pipeline daily run — exactly
    the same "independent schedule, already-fresh downstream state" pattern
    already proven correct for merchantmcc_api_pipeline's relationship to
    Gold. No TriggerDagRunOperator or sensor is needed: the marts don't need
    to be "waited for," only read as they currently stand.
  - A client delivery is a business artifact, not a technical refresh — an
    issuer/network/program owner does not want a new file every 15 minutes.
    Schedule chosen: 0 6 * * * UTC — after both merchantmcc_api_pipeline's
    0 2 * * * daily run and many hours of merchantmcc_cdc_pipeline ticks
    have already settled, so every daily delivery reflects a full day's
    upstream activity.

Airflow is the ORCHESTRATOR only. All three tasks below call existing,
already-implemented, already-tested entry points in src/delivery/pipeline.py
and src/delivery/config.py — no extract-generation or validation logic is
duplicated here.
"""

import logging
import os

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

logger = logging.getLogger(__name__)

PROJECT_ROOT = "/opt/airflow/project"


def _chdir_to_project_root() -> None:
    """Same real defect/fix documented in dags/merchantmcc_cdc_pipeline.py and
    dags/merchantmcc_api_pipeline.py: src/delivery/* uses paths relative to the
    project root (data/gold/gold.duckdb, outbox/, configs/) — the base Airflow
    image's own WORKDIR is /opt/airflow, not this project's mounted root."""
    os.chdir(PROJECT_ROOT)


def _run_validate_marts(**context) -> dict:
    _chdir_to_project_root()
    from src.delivery.config import load_dataset_registry
    from src.delivery.pipeline import validate_marts_available

    result = validate_marts_available(dataset_names=list(load_dataset_registry().keys()))
    logger.info("validate_marts: %s", result)
    return result


def _run_generate_client_extracts(**context) -> dict:
    _chdir_to_project_root()
    from src.delivery.config import load_client_entitlements
    from src.delivery.pipeline import run as run_delivery

    entitlements = load_client_entitlements()
    delivered: list[dict] = []
    skipped: list[dict] = []
    failures: list[str] = []

    for client_id, entitlement in entitlements.items():
        for dataset_name in entitlement["allowed_datasets"]:
            try:
                result = run_delivery(client_id=client_id, dataset_name=dataset_name)
            except Exception as exc:  # noqa: BLE001 - collected, not swallowed; re-raised below
                logger.error("generate_client_extracts: %s/%s failed: %s", client_id, dataset_name, exc)
                failures.append(f"{client_id}/{dataset_name}: {exc}")
                continue

            if result["status"] == "delivered":
                logger.info(
                    "generate_client_extracts: %s/%s -> %s (%d rows)",
                    client_id, dataset_name, result["run_id"], result["record_count"],
                )
                delivered.append(result)
            else:
                logger.info(
                    "generate_client_extracts: %s/%s already delivered for this period (%s) - skipped",
                    client_id, dataset_name, result["existing_run_id"],
                )
                skipped.append(result)

    if failures:
        raise RuntimeError(f"generate_client_extracts: {len(failures)} delivery(ies) failed: {'; '.join(failures)}")

    return {"delivered": delivered, "skipped": skipped}


def _run_validate_deliveries(**context) -> dict:
    """Re-reads every manifest this run's generate_client_extracts task just
    wrote (via XCom) and confirms it is present on disk and marked PASSED —
    a final, independent confirmation distinct from generate_client_extracts'
    own internal validation gates."""
    _chdir_to_project_root()
    import json
    from pathlib import Path

    ti = context["ti"]
    generated = ti.xcom_pull(task_ids="generate_client_extracts")
    checked = 0
    problems: list[str] = []

    for result in generated.get("delivered", []):
        manifest_path = Path(result["output_dir"]) / "manifest.json"
        if not manifest_path.exists():
            problems.append(f"{result['output_dir']}: manifest.json missing")
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("validation_status") != "PASSED":
            problems.append(f"{result['output_dir']}: validation_status={manifest.get('validation_status')!r}")
        checked += 1

    if problems:
        raise RuntimeError(f"validate_deliveries: {len(problems)} problem(s): {'; '.join(problems)}")

    logger.info("validate_deliveries: confirmed %d deliveries.", checked)
    return {"confirmed": checked}


default_args = {
    "owner": "merchantmcc-de",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="merchantmcc_client_delivery",
    description=(
        "MerchantMCC-DE: dbt marts -> client entitlement filtering -> validated "
        "CSV/Parquet extracts + manifest -> outbox/<client_id>/<dataset>/<run_id>/"
    ),
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 9, 27, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["merchantmcc", "delivery", "outbox"],
) as dag:
    validate_marts = PythonOperator(
        task_id="validate_marts",
        python_callable=_run_validate_marts,
    )

    generate_client_extracts = PythonOperator(
        task_id="generate_client_extracts",
        python_callable=_run_generate_client_extracts,
    )

    validate_deliveries = PythonOperator(
        task_id="validate_deliveries",
        python_callable=_run_validate_deliveries,
    )

    validate_marts >> generate_client_extracts >> validate_deliveries
