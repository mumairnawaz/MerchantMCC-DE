# Architecture

This folder holds every MerchantMCC-DE architecture diagram. The overall picture is here;
each stage has its own focused diagram in [`diagrams/`](diagrams/). All are Mermaid,
rendered directly by GitHub — no static images, so they stay accurate as the
implementation evolves.

## High-level platform architecture

```mermaid
flowchart TB
    subgraph API_PATH["EXTERNAL API PATH"]
        EXT["External APIs<br/>OSM · Frankfurter · GLEIF · reference"] --> ABronze[("API Bronze")] --> ASilver[("API Silver")]
    end
    subgraph CDC_PATH["CDC PATH"]
        PG[("PostgreSQL / OLTP")] --> DBZ["Debezium"] --> KAFKA["Kafka"] --> CBronze[("CDC Bronze")] --> CSilver[("CDC Silver")]
    end
    ASilver --> GOLD[("Gold — DuckDB<br/>facts + dimensions")]
    CSilver --> GOLD
    GOLD --> DBT["dbt<br/>staging → intermediate → marts"]
    DBT --> BI["Power BI<br/>internal analytics"]
    DBT --> DELIVERY["Client Delivery<br/>entitlement → validation → manifest → CSV/Parquet → outbox/"]
```

A separate, independent PySpark track (Bronze → Silver → Gold → Consumption) and Apache
Airflow orchestration (3 DAGs, one per operational pipeline) sit alongside this — see
[`diagrams/01-overall-architecture.md`](diagrams/01-overall-architecture.md) for the
complete picture including both.

## All diagrams

**Primary sequence** — one diagram per pipeline stage, in data-flow order:

| # | Diagram | Location |
|---|---|---|
| 1 | Overall architecture (both source paths, Airflow, Spark) | [`diagrams/01-overall-architecture.md`](diagrams/01-overall-architecture.md) |
| 2 | API ingestion (OSM, Frankfurter, GLEIF, reference data) | [`diagrams/02-api-ingestion.md`](diagrams/02-api-ingestion.md) |
| 3 | CDC pipeline (PostgreSQL → Debezium → Kafka → CDC Bronze/Silver) | [`diagrams/03-cdc-pipeline.md`](diagrams/03-cdc-pipeline.md) |
| 4 | Gold warehouse (facts, dimensions, live control values) | [`diagrams/04-gold-warehouse.md`](diagrams/04-gold-warehouse.md) |
| 5 | dbt marts (staging → intermediate → marts) | [`diagrams/05-dbt-marts.md`](diagrams/05-dbt-marts.md) |
| 6 | Airflow orchestration (all 3 DAGs, real schedules) | [`diagrams/06-airflow-orchestration.md`](diagrams/06-airflow-orchestration.md) |
| 7 | Client Data Delivery (entitlements, validation, manifest) | [`diagrams/07-client-data-delivery.md`](diagrams/07-client-data-delivery.md) |
| 8 | Power BI consumption (PBIP/TMDL/PBIR) | [`diagrams/08-power-bi-consumption.md`](diagrams/08-power-bi-consumption.md) |
| 9 | Spark pipeline (independent engineering track) | [`diagrams/09-spark-pipeline.md`](diagrams/09-spark-pipeline.md) |
| 10 | End-to-end data flow (everything combined) | [`diagrams/10-end-to-end-data-flow.md`](diagrams/10-end-to-end-data-flow.md) |

**Supplementary diagrams** — domain/design detail referenced from the docs above, not
duplicating them:

| # | Diagram | Location |
|---|---|---|
| 11 | Real vs reference vs synthetic data boundary | [`diagrams/11-real-reference-synthetic.md`](diagrams/11-real-reference-synthetic.md) |
| 12 | Incremental (watermark) ingestion sequence | [`diagrams/12-incremental-ingestion.md`](diagrams/12-incremental-ingestion.md) |
| 13 | Fintech transaction lifecycle (synthetic) | [`diagrams/13-transaction-lifecycle.md`](diagrams/13-transaction-lifecycle.md) |
| 14 | Data lineage (metadata foundation; full lineage tooling remains a documented future extension) | [`diagrams/14-data-lineage.md`](diagrams/14-data-lineage.md) |
