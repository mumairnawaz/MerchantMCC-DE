# Airflow Evidence

Real screenshots from the live Airflow 3 UI at `http://localhost:8081`, captured
2026-09-28, plus the CLI evidence that was captured alongside them.

### DAG overview — all 3 DAGs registered and unpaused

![Airflow DAG overview](01-dag-overview.png)

`merchantmcc_api_pipeline` (`0 2 * * *`), `merchantmcc_cdc_pipeline` (`*/15 * * * *`), and
`merchantmcc_client_delivery` (`0 6 * * *`) — each on its own independently justified
schedule, all currently unpaused with a green latest-run indicator.

### CDC pipeline — task grid

![CDC DAG graph](02-cdc-dag-graph.png)

`cdc_bronze → cdc_silver → gold_rebuild → dbt_build`, the last 24 hours of runs. One
historical failed task/run is visible in the "Last 24 Hours" panel — shown as-is rather
than cropped out, consistent with this project's evidence-based documentation standard.

### CDC pipeline — run history

![CDC DAG run history](03-cdc-dag-grid.png)

Consecutive scheduled runs on the `*/15 * * * *` cadence, each completing in roughly
55–70 seconds, all `Success`.

### API pipeline — task grid

![API DAG graph](04-api-dag-graph.png)

The six per-source Bronze/Silver ingestion tasks (`currency_bronze`, `fx_rate_silver`,
`gleif_bronze`, `legal_entity_silver`, `merchant_osm_bronze`, `merchant_silver`), zero
failed tasks in the last 24 hours.

### Client delivery pipeline — task grid

![Client delivery DAG graph](05-client-delivery-dag-graph.png)

`validate_marts → generate_client_extracts → validate_deliveries`, zero failed tasks.

## CLI evidence (real, captured 2026-09-28)

Cross-checked against the screenshots above to confirm the CLI and UI agree:

```
dag_id                      | is_paused
merchantmcc_api_pipeline    | False
merchantmcc_cdc_pipeline    | False
merchantmcc_client_delivery | False
```

```
task_id       state
cdc_bronze    success
cdc_silver    success
gold_rebuild  success
dbt_build     success
```

No credentials, cookies, or tokens are visible in any of the screenshots above.
