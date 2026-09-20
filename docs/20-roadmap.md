# 20 — Roadmap

| Phase | Scope | Status |
|---|---|---|
| 0 | GitHub / project foundation (this documentation) | IN PROGRESS |
| 1 | Live API source implementation (OSM incremental, Frankfurter incremental, GLEIF) | PLANNED |
| 2 | Bronze ingestion for all seven sources | PLANNED |
| 3 | Data quality — Silver-layer gate (cross-source referential checks, OSM→MCC crosswalk applied) | PLANNED |
| 4 | Silver — cleaned, conformed, deduplicated tables | PLANNED |
| 5 | Warehouse / Gold marts (dimensional model) | PLANNED |
| 6 | Airflow automation (scheduled, watermark-driven DAGs) | PLANNED |
| 7 | Power BI + governed CSV/Parquet stakeholder delivery | PLANNED |

Each phase will be a distinct, reviewable set of commits — this roadmap exists so the
repository's history tells that story deliberately rather than as one large, undifferentiated
drop.
