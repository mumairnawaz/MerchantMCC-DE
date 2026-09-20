# 14 — Orchestration

Status: **PLANNED**. No Airflow installation or DAGs exist in this repository yet.

## Target design

One DAG per live source, plus a low-frequency reference-data refresh DAG:

- `osm_ingestion_dag` — weekly
- `frankfurter_ingestion_dag` — daily
- `gleif_ingestion_dag` — daily
- `reference_data_refresh_dag` — manual/low-frequency (MCC, country, ISO 4217, BIN/IIN)

Each DAG: `read watermark → fetch → validate → write Bronze → update watermark`. Watermarks
are planned to be stored as Airflow Variables or a dedicated `pipeline_watermark` Postgres
table — never hardcoded into DAG source.

## Why Airflow, and why not yet

Airflow is justified once there are enough interdependent, schedule-driven jobs to need
retries and dependency management — which is exactly the point this project has now
reached in design, even though the DAGs themselves aren't built. Building orchestration
before the ingestion logic it orchestrates is stable would be solving the wrong problem
first.
