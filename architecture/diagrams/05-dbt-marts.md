# 5. dbt Marts

```mermaid
flowchart LR
    GOLD[("Gold<br/>facts + dimensions")] --> STG["staging<br/>18 thin models, 1:1 over Gold sources"]
    STG --> INT["intermediate<br/>enriched joins"]
    INT --> MARTS["marts"]

    subgraph MARTS_LIST["6 consumption marts"]
        M1["transaction_mart"]
        M2["merchant_mart"]
        M3["settlement_mart"]
        M4["reconciliation_mart"]
        M5["rewards_mart"]
        M6["client_program_mart"]
    end

    MARTS --> M1
    MARTS --> M2
    MARTS --> M3
    MARTS --> M4
    MARTS --> M5
    MARTS --> M6

    M1 --> TEST["dbt singular test:<br/>assert_control_total_identity"]
    M3 --> TEST
    M4 --> TEST

    M1 --> BI["Power BI"]
    M2 --> BI
    M3 --> BI
    M4 --> BI
    M6 --> BI
    M1 --> DELIVERY["Client Delivery"]
    M2 --> DELIVERY
    M3 --> DELIVERY
    M4 --> DELIVERY
    M6 --> DELIVERY
```

dbt (dbt-core + dbt-duckdb, operating on the same `data/gold/gold.duckdb` file Gold
writes) turns the star schema into consumption-oriented marts used **identically by both
downstream consumers** — Power BI and Client Delivery both read the same marts, so no
aggregation logic is duplicated between them. A dbt singular test independently
re-verifies the £435,106.16 control-total identity from the marts themselves on every
`dbt build`. dbt documentation has been generated (`dbt docs generate`) and is present
under `dbt/target/`.

Orchestrated as the final task (`dbt_build`) of Airflow's `merchantmcc_cdc_pipeline` DAG.
Full detail: [`docs/28-dbt-transformation-layer-design.md`](../../docs/28-dbt-transformation-layer-design.md).
