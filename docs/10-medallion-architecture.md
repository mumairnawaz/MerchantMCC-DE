# 10 — Medallion Architecture

| Layer | Purpose | Status |
|---|---|---|
| Bronze | Raw, source-faithful, timestamped, append-only | PLANNED for this repository (framework previously built and locally validated, not yet published here) |
| Data Quality | Structural validation at ingestion; cross-source referential checks at Silver boundary | PLANNED |
| Silver | Cleaned, deduplicated, conformed keys, reference crosswalks applied (e.g. OSM→MCC) | PLANNED |
| Transformation | Business logic, dbt models | PLANNED |
| Warehouse | Dimensional schema, PostgreSQL | PLANNED |
| Gold Marts | Merchant performance, MCC analysis, reconciliation, rewards/CLO | PLANNED |
| Delivery | Power BI + governed CSV/Parquet | PLANNED |

## Why Bronze is never overwritten

Every ingestion run writes to its own timestamped directory. This isn't just a convention —
it's what allowed a real deprecated-API response (REST Countries v3.1 → paid v5) to be kept
as an honest audit trail rather than silently lost, when that was encountered during earlier
development. See [13-incremental-ingestion.md](13-incremental-ingestion.md) for the
watermark model built on top of this.

## Why data quality is split into two tiers

**Ingestion-level** validation (non-empty, required-fields, duplicate-key, field-pattern
checks) answers "did we receive something structurally usable?" It is deliberately minimal.
**Silver-layer** validation (cross-source referential integrity, business-rule checks) is a
separate, later concern — conflating the two tends to produce either overly strict ingestion
code or an under-powered Silver gate. See [12-data-quality.md](12-data-quality.md).
