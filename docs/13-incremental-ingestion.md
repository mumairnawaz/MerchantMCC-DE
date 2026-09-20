# 13 — Incremental Ingestion

All three live sources have a **verified-live** incremental mechanism. None of these are
real-time — they are scheduled, incremental batch pipelines.

| Source | Watermark | Mechanism | Verified live? |
|---|---|---|---|
| OSM Overpass | Last `osm3s.timestamp_osm_base` | `newer:"<date>"` filter on a named result set | Yes |
| Frankfurter | `MAX(date)` already ingested | `GET /v1/{watermark+1}..{today}` | Yes |
| GLEIF | `MAX(registration.lastUpdateDate)` already ingested | `filter[registration.lastUpdateDate]=>=<date>` | Yes |

## Initial load vs. subsequent run

```
INITIAL LOAD                              SUBSEQUENT RUN
  API                                       Scheduler
   ↓                                          ↓
  Full scoped extraction                    Read previous watermark
   ↓                                          ↓
  Bronze                                    API (request new/changed records only)
   ↓                                          ↓
  Watermark recorded                        Validate
                                               ↓
                                             Bronze (new run)
                                               ↓
                                             Update watermark
```

## Two real failures encountered and resolved during verification

1. **OSM, missing User-Agent**: an identical query without a descriptive `User-Agent`
   header returned HTTP 406. With one, HTTP 200. Now treated as a hard requirement, not an
   optional courtesy header.
2. **OSM, timeout too short**: the `newer` filter scans OSM's attic/history data and is
   more expensive than a plain extract — a `[timeout:25]` budget (fine for a plain query)
   returned HTTP 504 for an incremental query; `[timeout:60]` succeeded.

## Known limitation

OSM's `newer` filter surfaces additions and edits but not deletions within our bounded
extract — a periodic full re-sync remains part of the design regardless of incremental
ingestion.

## Reference data does not get this treatment

MCC, country, ISO 4217, and BIN/IIN are refreshed by full snapshot replace, at low,
manual frequency — they are not live sources and are never described as incrementally
ingested.
