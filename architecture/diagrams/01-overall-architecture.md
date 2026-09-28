# 1. Overall Architecture

The two source paths (external API, and CDC from the synthetic OLTP system) converge at
Gold, from which dbt produces marts consumed by two independent downstream outputs.

```mermaid
flowchart TB
    subgraph SOURCES["SOURCE SYSTEMS"]
        OSM["OpenStreetMap<br/>Overpass"]
        FX["Frankfurter<br/>FX rates"]
        GLEIF["GLEIF<br/>Legal Entities"]
        REF["Reference data<br/>MCC · country · ISO 4217 · BIN/IIN"]
        PG[("PostgreSQL OLTP<br/>11-table synthetic fintech schema")]
    end

    subgraph APIPATH["API INGESTION"]
        ABronze[("API Bronze")] --> ASilver[("API Silver")]
    end

    subgraph CDCPATH["CDC STREAMING"]
        DBZ["Debezium"] --> KAFKA["Kafka"] --> CBronze[("CDC Bronze")] --> CSilver[("CDC Silver")]
    end

    OSM --> ABronze
    FX --> ABronze
    GLEIF --> ABronze
    REF --> ABronze
    PG --> DBZ

    ASilver --> GOLD[("GOLD<br/>facts + dimensions — DuckDB")]
    CSilver --> GOLD

    GOLD --> DBT["dbt<br/>staging → intermediate → marts"]

    subgraph CONSUMPTION["CONSUMPTION"]
        BI["Power BI<br/>internal analytics"]
        DELIVERY["Client Data Delivery<br/>entitlement filter → validation → manifest"]
    end

    DBT --> BI
    DBT --> DELIVERY
    DELIVERY --> FILES[("CSV / Parquet<br/>outbox/")]

    ORCH{{"Airflow — orchestration<br/>3 independent DAGs"}}
    ORCH -.-> APIPATH
    ORCH -.-> CDCPATH
    ORCH -.-> DELIVERY

    QUALITY{{"Data Quality & Reconciliation<br/>validation · control totals · idempotency · checkpoints"}}
    QUALITY -.-> APIPATH
    QUALITY -.-> CDCPATH
    QUALITY -.-> GOLD
    QUALITY -.-> DELIVERY
```

Airflow orchestrates the API, CDC, and Client Delivery pipelines on three independent
schedules (`0 2 * * *`, `*/15 * * * *`, `0 6 * * *` respectively — see
[`06-airflow-orchestration.md`](06-airflow-orchestration.md)); it is drawn here as an
orchestration layer around the pipelines it schedules, not as another pipeline stage.
Data quality and reconciliation controls apply at every stage, not just at the end — see
[`18-data-quality-flow.md`](18-data-quality-flow.md) for the consolidated view with real
numbers.

A separate, independent PySpark engineering track exists alongside this architecture — it
shares no code or data with the pipeline above. See
[`09-spark-pipeline.md`](09-spark-pipeline.md).

```mermaid
flowchart LR
    SB[("Spark Bronze")] --> SS[("Spark Silver")] --> SG[("Spark Gold")] --> SC[("Consumption")]
```

Each stage of the primary architecture has its own focused diagram in this folder — see
the index in [`architecture/README.md`](../README.md).
