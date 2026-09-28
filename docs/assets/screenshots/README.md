# Project Evidence Gallery

Real evidence proving the platform actually runs — not decoration. Each category answers
"what does this prove?"

## Policy: real screenshots where safely capturable, CLI evidence otherwise

Four categories now contain real, manually-captured GUI screenshots (Airflow, Kafka,
PostgreSQL/pgAdmin, dbt docs), captured directly by opening each tool and saving the
result. Power BI contains two illustrative mockup images (clearly labeled — see
[`powerbi/README.md`](powerbi/README.md)), not real Power BI Desktop screenshots. The
remaining categories (DuckDB, Docker\*, API, CDC, Spark, Client Delivery, Data Quality,
Testing) use real, reproducible **command-line evidence** instead, because this
environment has no safe, general-purpose way to drive a browser or target a specific
desktop window for automated capture — so rather than risk capturing unrelated desktop
content, or fabricate an image, the same underlying facts are demonstrated via real CLI
output.

\* Docker is the exception: one real Docker Desktop screenshot was manually captured and
is included.

## Categories

| Category | What it proves |
|---|---|
| [`airflow/`](airflow/) | Real screenshots: 3 DAGs unpaused, CDC/API/delivery task grids all green |
| [`kafka/`](kafka/) | Real screenshots: Debezium-generated CDC topics, live transaction messages |
| [`postgresql/`](postgresql/) | Real screenshots: the 11-table synthetic OLTP schema, a live analytical query |
| [`dbt/`](dbt/) | Real screenshots: the full model lineage graph, a model detail page — plus a real `dbt build`, 73/73 passing |
| [`docker/`](docker/) | Real screenshot: 7 healthy MerchantMCC containers |
| [`duckdb/`](duckdb/) | CLI evidence: the real 25-table/view Gold schema and a live control-total query |
| [`api/`](api/) | CLI evidence: real Bronze metadata/watermarks proving live external data enters the platform |
| [`cdc/`](cdc/) | CLI evidence: a real checkpoint file and zero consumer lag, proving idempotency |
| [`spark/`](spark/) | CLI evidence: the independent Spark track's real 1,010 → 875 valid + 135 rejected reconciliation |
| [`client-delivery/`](client-delivery/) | CLI evidence: real `outbox/` structure, a real manifest, entitlement filtering, idempotency |
| [`data-quality/`](data-quality/) | CLI evidence: the £435,106.16 identity independently re-proven at 3 layers |
| [`testing/`](testing/) | CLI evidence: real test counts and the isolated Client Delivery suite's 25/25 result |
| [`powerbi/`](powerbi/) | Two **illustrative mockups** (clearly labeled, not real screenshots) plus honest current rendering status |

Full capability → evidence → code → test mapping: [`../../EVIDENCE.md`](../../EVIDENCE.md).
