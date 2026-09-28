# Airflow Evidence

No GUI-automation tooling is available in this environment to capture real browser
screenshots of the Airflow UI safely (i.e. without risking capturing unrelated desktop
content). The command-line evidence below is real, captured directly from the live
Airflow environment, and proves the same facts a screenshot would.

## CLI evidence (real, captured 2026-09-28)

**All 3 DAGs registered, unpaused:**
```
dag_id                      | is_paused
merchantmcc_api_pipeline    | False
merchantmcc_cdc_pipeline    | False
merchantmcc_client_delivery | False
```

**Most recent `merchantmcc_cdc_pipeline` run — all 4 tasks succeeded:**
```
task_id       state
cdc_bronze    success
cdc_silver    success
gold_rebuild  success
dbt_build     success
```
Run `scheduled__2026-09-28T04:15:00+00:00`, completed in under a minute.

## Manual screenshot checklist (to be captured by opening http://localhost:8081)

- [ ] DAG list page — showing all 3 DAGs (`merchantmcc_cdc_pipeline`,
      `merchantmcc_api_pipeline`, `merchantmcc_client_delivery`), unpaused
- [ ] `merchantmcc_cdc_pipeline` — Graph view showing `cdc_bronze → cdc_silver →
      gold_rebuild → dbt_build`
- [ ] `merchantmcc_cdc_pipeline` — Grid/run-history view showing multiple successful runs
- [ ] `merchantmcc_api_pipeline` — Graph view showing the per-source Bronze/Silver task
      pairs
- [ ] `merchantmcc_client_delivery` — Graph view showing `validate_marts →
      generate_client_extracts → validate_deliveries`
- [ ] Any one task's log view showing a successful completion

**Caption suggestion**: *"Airflow — CDC DAG showing the Bronze → Silver → Gold → dbt
dependency chain, all tasks green."*

No credentials, cookies, or tokens are visible in the Airflow UI's DAG/graph/grid views —
standard screenshots of these pages are safe to include as-is.
