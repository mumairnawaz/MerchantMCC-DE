# 10. End-to-End Data Flow

The complete path from source to consumption, combining every stage documented
individually in this folder.

```mermaid
flowchart TB
    EXT["External APIs<br/>OSM · Frankfurter · GLEIF · reference"] --> ABronze[("API Bronze")] --> ASilver[("API Silver")]
    PG[("PostgreSQL / OLTP")] --> DBZ["Debezium"] --> KAFKA["Kafka"] --> CBronze[("CDC Bronze")] --> CSilver[("CDC Silver")]

    ASilver --> GOLD[("Gold — DuckDB<br/>facts + dimensions")]
    CSilver --> GOLD

    GOLD --> DBT["dbt<br/>staging → intermediate"]
    DBT --> MARTS[("6 marts")]

    MARTS --> BI["Power BI<br/>internal analytics"]
    MARTS --> DELIV["Client Delivery<br/>entitlement → validation → manifest"]
    DELIV --> OUTBOX[("outbox/<br/>CSV / Parquet")]

    AF{{"Airflow"}} -.orchestrates.-> ABronze
    AF -.orchestrates.-> CBronze
    AF -.orchestrates.-> GOLD
    AF -.orchestrates.-> DBT
    AF -.orchestrates.-> DELIV
```

**Every stage in this diagram is real and independently verified** — not a plan. The
reconciled identity that proves the pipeline is internally consistent end to end:

```
Approved transaction amount  =  Settlement amount  =  Reconciliation expected amount  =  £435,106.16
```

recomputed independently at Gold, at the dbt mart layer, and again inside Client
Delivery's own validation gate — all three agree. The gross total across all transaction
statuses (£501,672.07) is a different, non-control figure — see
[`04-gold-warehouse.md`](04-gold-warehouse.md).

A separate, independent PySpark track (§9) and the Power BI semantic model (§8) both sit
alongside this flow without altering it — Spark shares no code or data with it, and Power
BI is one of two read-only consumers of the marts, not a stage in producing them.
