# 08 — System Architecture

Full diagrams live in [`architecture/`](../architecture/README.md). This document is the
narrative companion.

## The pipeline, end to end

```
REAL EXTERNAL APIs (OSM, Frankfurter, GLEIF)  ─┐
REFERENCE DATA (MCC, Country, ISO 4217, BIN)  ─┼─→ INGESTION ─→ BRONZE ─→ DATA QUALITY
SYNTHETIC DATA (OLTP, transaction events)     ─┘
                                                                       ↓
                                                                    SILVER
                                                                       ↓
                                                              DATA WAREHOUSE
                                                                       ↓
                                                                  GOLD MARTS
                                                                   ↙       ↘
                                                            POWER BI    CSV / PARQUET
```

## Design principles

1. **Every layer is source-traceable.** Bronze metadata captures source URL, ingestion
   timestamp, and a run identifier for every record set, regardless of tier (live/
   reference/synthetic).
2. **The real/synthetic boundary never blurs.** A `source_type` field (`live_api` /
   `reference` / `synthetic`) is carried from Bronze through to any delivered file.
3. **Bronze is append-only.** Every ingestion run gets its own timestamped directory —
   nothing is overwritten in place.
4. **Incremental where the source genuinely supports it.** Each live source's watermark
   mechanism is independently verified before being relied on — see
   [13-incremental-ingestion.md](13-incremental-ingestion.md).

See also: [09-data-flow.md](09-data-flow.md), [10-medallion-architecture.md](10-medallion-architecture.md).
