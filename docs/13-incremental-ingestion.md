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

## OSM implementation (Step 4)

`src/ingestion/merchant_osm.py` is watermark-aware: `run()` checks
`data/watermarks/merchant_osm.json` (via `src/ingestion/watermark.py`) and does a **full
load** if no watermark is recorded, or an **incremental load** if one is.

**Full load**: the existing bbox/shop/amenity query, unchanged.
**Incremental load**: the same bbox/shop/amenity filters, restructured into Overpass QL's
required set-then-filter grammar (`(...)->.all; node.all(newer:"<watermark>"); out meta;`)
— a single chained filter is not valid QL, this two-statement form is the one verified
live. Uses `out meta;` to capture `timestamp`/`version`/`changeset` per element.

**Watermark store**: `data/watermarks/<source_name>.json`, a small local JSON file
(gitignored, like Bronze data) behind a narrow `read_watermark()` / `write_watermark()`
interface in `src/ingestion/watermark.py`. This is deliberately the minimum viable
mechanism for the current pre-Airflow, pre-database local stage — the interface is narrow
specifically so it can be swapped for Airflow Variables or a PostgreSQL
`pipeline_watermark` table later without changing `merchant_osm.py` at all.

**Watermark value**: always the *source's own* `osm3s.timestamp_osm_base` from the most
recent successful run — never the local machine clock.

**Advance-only-on-success rule**: the watermark is written only when the response passes
validation (structurally valid shape, and per-record checks when records are present). A
failed HTTP request or a failed validation both leave the watermark untouched, so a bad run
never causes the next run to silently skip a window of real changes.

**Boundary semantics**: the watermark value is passed straight through to Overpass's own
`newer:` filter without any client-side date-math — whether Overpass treats the exact
boundary instant as inclusive or exclusive is Overpass's own documented behavior, not
something this project reinterprets. If that boundary is ever inclusive (returning the same
edge element again on the next run), Bronze's existing duplicate-key validation operates
*within* a single run, not *across* runs — a possible one-record overlap at the boundary is
a Silver-layer deduplication concern (Silver already needs id-based deduplication across
merged runs by design), not a Bronze defect. Bronze's job is to honestly record what the
source returned on each run, not to pre-deduplicate across runs.

## Known limitation

OSM's `newer` filter surfaces additions and edits but not deletions within our bounded
extract — a periodic full re-sync remains part of the design regardless of incremental
ingestion. Concretely, at this local stage, a full re-sync means deleting
`data/watermarks/merchant_osm.json` before the next run, which is a manual, deliberate
action, not something triggered automatically.

## Reference data does not get this treatment

MCC, country, ISO 4217, and BIN/IIN are refreshed by full snapshot replace, at low,
manual frequency — they are not live sources and are never described as incrementally
ingested.
