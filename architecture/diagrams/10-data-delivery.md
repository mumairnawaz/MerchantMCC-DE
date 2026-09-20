# 10. Final Data Delivery Architecture

Status: PLANNED.

```mermaid
flowchart TB
    GOLD[("Gold Marts")] --> BI["Power BI<br/>internal, interactive"]
    GOLD --> MANIFEST["Manifest generator<br/>period, timestamp, schema version,<br/>record count, control totals, DQ status"]
    MANIFEST --> CSV["CSV export"]
    MANIFEST --> PARQUET["Parquet export"]
    CSV --> ISSUER["Issuer"]
    CSV --> NETWORK["Network"]
    CSV --> PROGRAM["Program Owner"]
    PARQUET --> ISSUER
    PARQUET --> NETWORK
    PARQUET --> PROGRAM
```

A dashboard is for a human exploring data interactively; a governed file is what a
downstream system actually loads and validates against a control total. Full narrative:
[`docs/16-data-delivery.md`](../../docs/16-data-delivery.md).
