# Project Evidence Gallery

Screenshots and captured command-line evidence proving the platform actually runs — not
decoration. Each subdirectory's own `README.md` explains exactly what it proves and lists
a manual-capture checklist for anything that requires opening a GUI (Airflow's web UI,
Power BI Desktop) that could not be safely automated in this environment.

| Category | What it proves |
|---|---|
| [`airflow/`](airflow/) | 3 DAGs registered and unpaused; a real CDC pipeline run with all 4 tasks succeeding |
| [`kafka/`](kafka/) | Real Debezium-generated CDC topics, connector `RUNNING`, zero consumer lag |
| [`api/`](api/) | Real Bronze metadata/watermarks proving live external data actually enters the platform |
| [`postgres/`](postgres/) | The 11-table synthetic fintech OLTP schema feeding CDC |
| [`dbt/`](dbt/) | A real `dbt build` run — 73/73 models and tests passing, including the control-total identity |
| [`powerbi/`](powerbi/) | Honest current status — schema-validated; Desktop rendering not yet confirmed |
| [`client-delivery/`](client-delivery/) | Real `outbox/` structure, a real manifest, entitlement filtering, idempotency |
| [`data-quality/`](data-quality/) | Spark's 1,010 → 875 valid + 135 rejected reconciliation; the £435,106.16 identity |
| [`testing/`](testing/) | Real test counts and the isolated Client Delivery suite's 25/25 result |

Where a GUI screenshot could not be safely captured automatically (no browser/desktop
automation tooling exists in this environment), the directory instead contains real,
reproducible command-line evidence proving the same fact, plus a manual checklist for
capturing the visual screenshot separately.
