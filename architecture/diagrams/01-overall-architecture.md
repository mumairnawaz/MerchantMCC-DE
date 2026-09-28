# 1. Overall Architecture

The two source paths (external API, and CDC from the synthetic OLTP system) converge at
Gold, from which dbt produces marts consumed by two independent downstream outputs.

```mermaid
flowchart TB
    subgraph SRC1["EXTERNAL API PATH"]
        EXT["External APIs<br/>OSM · Frankfurter · GLEIF · reference data"] --> ABronze[("API Bronze")]
        ABronze --> ASilver[("API Silver")]
    end

    subgraph SRC2["CDC PATH"]
        PG[("PostgreSQL / OLTP<br/>synthetic fintech DB")] --> DBZ["Debezium"]
        DBZ --> KAFKA["Kafka"]
        KAFKA --> CBronze[("CDC Bronze")]
        CBronze --> CSilver[("CDC Silver")]
    end

    ASilver --> GOLD[("GOLD<br/>Facts + Dimensions<br/>DuckDB")]
    CSilver --> GOLD

    GOLD --> DBT["dbt<br/>staging → intermediate → marts"]
    DBT --> MARTS[("Marts")]

    MARTS --> BI["Power BI<br/>Internal Analytics"]
    MARTS --> ENT["Client Delivery<br/>entitlement filter"]
    ENT --> VAL["Validation"]
    VAL --> MAN["Manifest"]
    MAN --> FILES["CSV / Parquet"]
    FILES --> OUTBOX[("outbox/")]
```

```mermaid
flowchart LR
    AF["Airflow"] --> D1["merchantmcc_cdc_pipeline<br/>*/15 * * * *"]
    AF --> D2["merchantmcc_api_pipeline<br/>0 2 * * *"]
    AF --> D3["merchantmcc_client_delivery<br/>0 6 * * *"]
```

A separate, independent PySpark engineering track exists alongside this architecture — it
shares no code or data with the pipeline above. See
[`09-spark-pipeline.md`](09-spark-pipeline.md).

```mermaid
flowchart LR
    SB[("Spark Bronze")] --> SS[("Spark Silver")] --> SG[("Spark Gold")] --> SC[("Consumption")]
```

Each stage of the primary architecture has its own focused diagram in this folder — see
the index in [`architecture/README.md`](../README.md).
