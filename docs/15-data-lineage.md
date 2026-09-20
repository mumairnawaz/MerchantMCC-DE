# 15 — Data Lineage

Status: **PLANNED**.

## Foundation already designed for it

Every Bronze record's metadata is designed to capture `source_url`,
`ingestion_timestamp_utc`, and a run identifier — the minimum foundation lineage tooling
needs to attach to. This is a design commitment carried through every source contract in
[06-data-source-catalog.md](06-data-source-catalog.md), not an afterthought.

## Planned tooling

OpenLineage, with Marquez as the reference implementation for visualization — deferred
until Silver/Gold transformations exist to have lineage *between*. Tracking lineage across
a single ingestion hop has limited value; it earns its place once there are multiple
transformation stages to connect.
