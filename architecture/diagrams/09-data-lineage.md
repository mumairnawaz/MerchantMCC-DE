# 9. Data Lineage

Status: PLANNED — OpenLineage/Marquez integration, deferred until Silver/Gold
transformations exist. This diagram shows the lineage metadata foundation already designed
into every Bronze write.

```mermaid
flowchart LR
    API["Source API / File"] -->|"source_url<br/>ingestion_timestamp_utc<br/>run_id"| BRONZE[("Bronze")]
    BRONZE -->|"lineage: bronze run_id"| SILVER[("Silver")]
    SILVER -->|"lineage: silver transform_id"| GOLD[("Gold")]
    GOLD -->|"lineage: gold mart_id"| OUT["Power BI / Files"]
```

Full narrative: [`docs/15-data-lineage.md`](../../docs/15-data-lineage.md).
