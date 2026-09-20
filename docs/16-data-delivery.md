# 16 — Data Delivery

Status: **PLANNED**.

## Two channels, two different purposes

**Power BI** (internal, interactive) — exploration and drill-down for internal analysts.
Connects directly to the Gold schema.

**CSV / Parquet** (external stakeholders — issuer, network, program owner) — a governed,
programmatic handoff. Each delivery is planned to include a manifest: reporting period,
generation timestamp, schema version, record count, control totals, run identifier, and
data-quality status. A downstream system loads and validates against this file directly; it
cannot "load" a dashboard. This is the deliberate point of building file delivery
alongside BI rather than treating BI as the finished product.

## Why Power BI is not the center of this project

Power BI is the last stage of the pipeline, not the architecture's organizing idea. The
engineering substance is everything upstream of it: real API verification, incremental
ingestion, validation, modeling, and governed delivery.
