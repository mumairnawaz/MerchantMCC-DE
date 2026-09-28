# 28 — dbt Transformation Layer over Gold (Phase S15-A)

Status: **IMPLEMENTED**. Scope boundary is exact:

```
S1-S8:   Public APIs → Bronze → API Silver                       [done]
S9-S13:  Synthetic OLTP → ... → CDC Bronze → CDC Silver           [done]
S14:     API Silver + CDC Silver → Gold/OLAP (DuckDB, Parquet)    [done, frozen]
S15-A:   dbt over Gold — staging / intermediate / marts (THIS)    dbt/
S15-B+:  Airflow / Power BI / further dbt work                    [future, not started]
```

This phase does **not** touch Bronze, API ingestion, CDC (Postgres,
Debezium, Kafka, CDC Bronze, CDC Silver), or `src/gold/dimensions.py` /
`src/gold/facts.py` / `src/gold/validation.py` / `src/gold/reconciliation.py`
— the S14 dimensional model (13 dimensions, 5 facts, surrogate keys, unknown
member, SCD strategy) is **frozen** and unchanged. The one Gold-layer change
made here (removing `_create_mart_views()` from `src/gold/pipeline.py`) is
a mart-*ownership* migration, not a redesign — see §4.

## 1. Architecture

```
data/gold/gold.duckdb  (built by src/gold/pipeline.py — UNCHANGED, S14)
   schema "main": 13 dim_* tables + 5 fact_* tables
        │
        │  dbt sources (dbt/models/staging/_gold_sources.yml)
        ▼
 ┌──────────────────────────────────────────────────────────────┐
 │  dbt/  (dbt-core 1.12, dbt-duckdb 1.11, project "finpay_gold") │
 │                                                                │
 │  models/staging/     18 thin 1:1 pass-throughs, schema        │
 │                       "staging" (stg_dim_*, stg_fact_*)       │
 │                             │                                 │
 │  models/intermediate/ 4 business-logic joins, schema          │
 │                       "intermediate" (int_*_enriched)         │
 │                             │                                 │
 │  models/marts/        6 consumption-ready tables, schema      │
 │                       "marts" (merchant_mart, transaction_mart,│
 │                       client_program_mart, settlement_mart,   │
 │                       reconciliation_mart, rewards_mart)      │
 │                             │                                 │
 │  tests/                45 schema tests (unique/not_null/      │
 │                       relationships) + 1 singular test        │
 │                       (assert_control_total_identity)         │
 └──────────────────────────────────────────────────────────────┘
        │
        ▼
data/gold/gold.duckdb  (same file — schemas "staging"/"intermediate"/"marts"
                         added alongside the untouched "main" schema)
        │
        ▼
   [S15-B+: Power BI / further marts — not started]
```

**One physical DuckDB file, four schemas, two owners**: `main` is written
exclusively by `src/gold/pipeline.py` (Python, S14); `staging`,
`intermediate`, `marts` are written exclusively by `dbt run`/`dbt build`
(this phase). Neither tool writes into the other's schema. This satisfies
"don't replace DuckDB" literally — dbt is a second, additive *writer* into
the same local engine and the same file, not a new engine.

## 2. Why dbt Reads Gold, Not Silver, Directly

The architecture given for this phase places dbt **after** Gold
(`API Silver + CDC Silver → Gold/OLAP → dbt → staging/intermediate/marts`),
not in parallel with it. Concretely: dbt's `sources` are the 18 already-
dimensional Gold tables (`dim_*`/`fact_*`), not the 6 API Silver or 11 CDC
Silver Parquet datasets. This is a deliberate reading of the given
architecture diagram, not an assumption — the diagram shows exactly one
arrow into "dbt", originating from "Gold / OLAP", with API Silver and CDC
Silver both terminating at Gold beforehand. Reasons this is also the
correct engineering choice, not just the literal one:

- Gold already resolved the hard, real-data-verified problems (surrogate
  keys, unknown-member fallback, the event-log grain widening from §7 of
  docs/27, the multi-hop client/program/cardholder lookup chain). Re-doing
  any of that inside dbt would either duplicate `src/gold/keys.py`'s
  persisted-registry mechanism in SQL (fragile, two sources of truth for
  the same surrogate key) or silently drop the guarantees S14 already
  earned through real-data testing.
- dbt's staging layer conventionally establishes a lineage/contract
  boundary over whatever it's given — it does not require its sources to be
  "raw". Gold is a legitimate, common dbt source shape.

## 3. Real Verification Performed Before Writing Any Model

- `pq.read_schema()`/`DESCRIBE` against all 18 real `gold.duckdb` "main"
  tables — every staging/intermediate/mart column name used below was
  copied from this output, not guessed (docs/27 §5-6 already documents
  these schemas; re-verified here since dbt would fail loudly and
  immediately on a wrong column name, which is a cheap, real check).
- `int_reconciliation_enriched`'s join path: confirmed
  `fact_reconciliation` carries no currency of its own (docs/27 §6) and
  must join through `settlement_id` → `fact_settlements` to get
  `settlement_batch_id`/`currency_code` — the same real relationship
  `fact_reconciliation` already encodes as a degenerate join key.
- **A real, reproducible operational finding**: `dbt-duckdb` resolves
  `profiles.yml`'s relative `path:` (`../data/gold/gold.duckdb`) against the
  **process's current working directory**, not against `--project-dir`.
  Running `dbt debug --project-dir dbt --profiles-dir dbt` from the repo
  root fails with `IO Error: Cannot open file
  "C:\Users\...\Documents\data\gold\gold.duckdb"` (one directory too high),
  while `cd dbt && dbt debug --profiles-dir .` succeeds. Verified by
  reproducing both ways. **Resolution**: every dbt invocation in this
  project (documented in §7, and enforced in
  `tests/test_dbt_gold_transformation.py::_dbt()` via an explicit
  `cwd=dbt/`) must run with the dbt project directory as the working
  directory — the standard `cd dbt && dbt <command>` pattern, not a
  workaround or a code change to dbt itself.

## 4. Mart-Ownership Migration (the one Gold-layer change)

S14 built `merchant_mart`/`transaction_mart` as raw SQL views inside
`src/gold/pipeline.py::_create_mart_views()`, explicitly flagged there as
provisional ("not dbt models — dbt is out of scope for S14"). Now that dbt
is in scope, **dbt owns every mart** — keeping the old Python-created views
around would leave two competing definitions of `merchant_mart` in the same
schema, which is exactly the kind of silent inconsistency this project's
documentation discipline exists to prevent.

**Change made**: `_create_mart_views()` and its call site were deleted from
`src/gold/pipeline.py`. `src/gold/pipeline.py::run()` now only writes the
`main` schema (unchanged 13 dims + 5 facts) — it no longer creates any
view. `tests/test_gold_pipeline_integration.py` was updated to match (the
two assertions that checked for `merchant_mart`/`transaction_mart` in the
default schema were removed/updated; a new, equivalent check now lives in
`tests/test_dbt_gold_transformation.py`, against the `marts` schema).

This is **not** a change to the frozen S14 dimensional model — no
dimension, fact, surrogate key, or reconciliation logic in
`src/gold/dimensions.py`/`facts.py`/`validation.py`/`reconciliation.py` was
touched. It is exclusively the removal of two ad hoc SQL views that this
phase's own architecture makes redundant.

## 5. Staging Layer (18 models)

One model per Gold source table (`stg_dim_date`, `stg_dim_merchant`, ...,
`stg_fact_rewards`), each a pure `select * from {{ source('gold', '<table>') }}`
— deliberately no transformation. Gold data is already typed and validated
(docs/27); staging here exists purely to give dbt (and any future dbt
package/consumer) a stable model name to depend on, independent of the
literal `main` schema table it happens to read today.

Materialized as **views** (`+materialized: view` in `dbt_project.yml`) —
cheap, always-current, no duplicated storage of data Gold already persisted
to Parquet.

## 6. Intermediate Layer (4 models)

| Model | Grain | Joins in |
|---|---|---|
| `int_transactions_enriched` | `transaction_id` | dim_date, dim_merchant, dim_mcc, dim_currency, dim_program, dim_client |
| `int_settlements_enriched` | `settlement_id` | dim_date, dim_currency |
| `int_reconciliation_enriched` | `reconciliation_id` | dim_date, `int_settlements_enriched` (for batch_id/currency — §3) |
| `int_rewards_enriched` | `(reward_id, kafka_offset)` | dim_date, dim_currency, dim_offer, dim_campaign |

Every join is a `LEFT JOIN` against a dimension whose surrogate-key FK on
the fact side already resolves to either a real row or the `UNKNOWN_KEY`
(-1) member (S14's unknown-member convention) — so a `LEFT JOIN` here never
silently drops a fact row; at worst it surfaces the `UNKNOWN` dimension
row's own placeholder attributes, which is by design, not a leak. No
`INNER JOIN` is used anywhere in the intermediate layer for this reason.

## 7. Marts Layer (6 tables, 1 deferred)

| Mart | Grain | KPI domain (docs/27 §19) | Status |
|---|---|---|---|
| `merchant_mart` | `merchant_key` | B — Merchant intelligence | Migrated from S14 Python view |
| `transaction_mart` | `(full_date, currency_code, transaction_status)` | A — Transaction/payment intelligence | Migrated from S14 Python view |
| `client_program_mart` | `(client_key, program_key)` | C — Client/program intelligence | New in S15-A |
| `settlement_mart` | `(settlement_batch_id, currency_code)` | D — Settlement | New in S15-A |
| `reconciliation_mart` | `(reconciled_date, match_status)` | E — Reconciliation | New in S15-A |
| `rewards_mart` | `(offer_key, campaign_key, qualification_status)` | F — Rewards/CLO | New in S15-A |
| `operations_mart` | — | G — Data engineering operations | **Still deferred** — see §11 |

Materialized as **tables** (`+materialized: table`) — these are the layer a
BI tool or analyst actually queries; materializing avoids re-running the
staging→intermediate join chain on every dashboard refresh.

`operations_mart` remains unbuilt, exactly as flagged in docs/27 §20/§24:
there is still no durable source for pipeline-run metadata (row counts,
reconciliation pass/fail) — `src/gold/pipeline.py::run()` only returns an
in-memory Python dict today. Building this mart honestly would require
either a new persisted operations log (a `src/gold/pipeline.py` change
beyond what S15-A's "introduce dbt" scope asked for) or fabricating
metrics dbt has no real source for — this phase does neither, and leaves
the gap open rather than silently closing it with invented data.

## 8. Schema Naming

A custom `generate_schema_name` macro (`dbt/macros/generate_schema_name.sql`)
overrides dbt-duckdb's default schema-naming (which would concatenate the
profile's target schema and the model's custom schema, e.g. `main_marts`).
This project uses the custom schema name directly (`staging`, `intermediate`,
`marts`) — a well-known, common dbt customization, not a hidden or unusual
mechanism.

## 9. Testing

**Schema tests** (`dbt/models/staging/_staging_schema.yml`,
`dbt/models/marts/_marts_schema.yml`): `unique`+`not_null` on every
dimension's surrogate key; `not_null` on every fact's grain-defining
column(s); `relationships` tests on `stg_fact_transactions`
(merchant_key/mcc_key/currency_key → their dimensions) and
`stg_fact_rewards.offer_key` → `stg_dim_offer` — these pass specifically
*because* the unknown-member convention means `-1` always exists on both
sides of the relationship, which is itself a live regression check on that
S14 guarantee.

Event-log composite grain (`(event_id, kafka_offset)`,
`(reward_id, kafka_offset)`, docs/27 §7) is **not** re-tested with a dbt
`unique` test here — dbt's built-in `unique` test is single-column only, and
adding `dbt_utils` (a new package dependency) solely for this would be
disproportionate to what's needed: `src/gold/validation.py` already
hard-enforces this grain in Python before any Parquet is written, so dbt
would only be re-checking a guarantee that can't actually be violated by
the time dbt ever sees the data.

**Singular test** (`dbt/tests/assert_control_total_identity.sql`): the same
control-total identity `src/gold/reconciliation.py` verifies in Python
(approved transaction total == settlement total == reconciliation expected
total) — re-verified here independently, from the dbt marts themselves, as
a genuine second, independent check rather than trusting the Python result
transitively.

**Result**: `dbt build` (all models + all tests) — **PASS=73, ERROR=0,
WARN=0, TOTAL=73** (28 models + 45 data tests).

**Python integration test** (`tests/test_dbt_gold_transformation.py`, 10
tests): shells out to the real `dbt` executable against the real project
(no mocks, consistent with this project's testing philosophy throughout
S1-S14) — verifies `dbt build` succeeds, all 4 schemas exist, staging has
exactly 18 models, marts has exactly the 6 expected tables, the control-
total identity holds when queried directly from `marts.*` and matches
`src/gold/pipeline.py`'s own Python-computed figure exactly, `merchant_mart`
has exactly 520 rows (the real, S14-verified merchant/transaction overlap),
a second `dbt build` is idempotent (identical aggregate values), and dbt
never modifies Bronze/API Silver/CDC Silver files (mtime-diffed).

## 10. Idempotency & Rebuild Strategy

Every dbt model in this project is `CREATE OR REPLACE` (views) or a full
`CREATE OR REPLACE TABLE ... AS SELECT` (dbt's default `table` materialization
behavior) — there is no incremental model in this phase, matching Gold's own
full-rebuild philosophy (docs/27 §18). Verified directly: running
`dbt build` twice in a row produces an identical `SUM(approved_amount_total)`
from `marts.merchant_mart`.

## 11. Known Limitations / Open Decisions

**Carried forward from S14, unresolved (per instruction):** all 15 items
listed in docs/27 §24 remain open and untouched by this phase — none of
them are dbt-affecting (FX/reporting-currency conversion, `dim_time`,
GLEIF→country join, etc.).

**New, S15-A-specific:**

1. **`operations_mart` still not built** (§7) — blocked on a durable
   operations-log source that doesn't exist yet; would need a small,
   separate `src/gold/pipeline.py` addition (writing a run-log
   table/Parquet) to have real data to build it from. Not done here because
   it is outside this phase's stated scope ("introduce dbt"), not because
   it's hard.
2. **`dbt/profiles.yml` is committed to the repo** (not `~/.dbt/profiles.yml`)
   — safe here because it holds a relative file path and no credentials,
   consistent with this project's "local/free/portfolio" philosophy (docs/27
   §2). If a future phase points dbt at a networked warehouse, credentials
   must move to environment variables / a `.env`-backed profile, not stay
   inline.
3. **The `dbt-duckdb` cwd-resolution quirk (§3)** is documented and worked
   around by convention (`cd dbt && dbt <command>`), not patched — no dbt
   or dbt-duckdb code was modified. A future phase automating dbt calls
   (e.g. from Airflow, S15-B+) must preserve this working-directory
   requirement.
4. **No `dbt_utils` package** installed — the one place it would have been
   useful (composite-key uniqueness tests on the two event-log marts'
   sources) was judged disproportionate to add a package dependency for,
   given Python already enforces that grain upstream (§9).
5. **Power BI connectivity** (S15-B+, this architecture's next arrow) — not
   started; `gold.duckdb`'s `marts` schema is the stable interface it would
   connect to (via ODBC or DuckDB's own connectors), unchanged by whichever
   BI tool eventually reads it.

## 12. Next Phase Boundary

S15-B+ = whichever of Airflow orchestration / Power BI / further dbt
modeling (e.g. `operations_mart`, incremental models, dbt snapshots for
real SCD2 history once a second OSM ingestion exists) comes next, per your
priority. **Not started here.**
