# 27 — CDC Silver + API Silver → Gold/OLAP (Phase S14)

Status: **IMPLEMENTED**. Scope boundary is exact:

```
S1-S8:  Public APIs → Bronze → API Silver                 [already done]
S9-S13: Synthetic OLTP → Postgres → Debezium → Kafka →
        CDC Bronze → CDC Silver                            [already done]
S14:    API Silver + CDC Silver → Gold/OLAP (THIS PHASE)   data/gold/
S15+:   Airflow / dbt / Power BI / cloud deployment         [future, not started]
```

This phase does **not** re-validate business columns (API Silver and CDC
Silver already did that — S3-S8, S13). It does not regenerate Bronze. It
does not modify any Silver dataset. It reads two already-trustworthy inputs
and integrates them into a dimensional model, focused on the concerns only a
dimensional model has: grain, surrogate keys, conformed joins, unknown
members, and analytical-model integrity.

## 1. Architecture — the two upstream worlds converging

```
 EXTERNAL REFERENCE DATA (S1-S8)          INTERNAL OPERATIONAL DATA (S9-S13)
 ────────────────────────────────         ──────────────────────────────────
 Public APIs (REST Countries, OSM,          Synthetic FinPay OLTP (S9)
 IBAN BIC, GLEIF, mcc-codes, ISO 4217)              │
        │                                            ▼
        ▼                                   PostgreSQL `finpay` schema (S10)
 data/bronze/<source>/                              │  logical replication
        │                                            ▼
        ▼                                   Debezium connector (S11)
 src/silver/<source>.py                             │
        │                                            ▼
        ▼                                   Kafka topics finpay.finpay.<table>
 data/silver/<dataset>/data.parquet                 │
   merchant, mcc, country,                           ▼
   iso_currency, card_issuer,                 src/cdc/consumer.py (S12)
   legal_entity                                      │
        │                                            ▼
        │                                   data/bronze_cdc/<table>/ (immutable)
        │                                            │
        │                                            ▼
        │                                   src/cdc/silver.py (S13)
        │                                            │
        │                                            ▼
        │                                   data/silver_cdc/silver_cdc_<entity>/
        │                                     9 current_state + 2 event_log
        │                                            │
        └───────────────────┬────────────────────────┘
                             ▼
                  src/gold/pipeline.py :: run()   (THIS PHASE)
                             │
             ┌───────────────┼────────────────┐
             ▼                                 ▼
   src/gold/dimensions.py           src/gold/facts.py
   (13 dimension builders,          (5 fact builders, grain-checked,
    deterministic surrogate keys,    Decimal measures, unknown-member
    Type1/Type2 SCD, unknown         fallback via src/gold/keys.py
    member row per dimension)        registries)
             │                                 │
             └───────────────┬─────────────────┘
                              ▼
           src/gold/validation.py (grain / FK / key-uniqueness checks)
           src/gold/reconciliation.py (row-count identities + control totals)
                              │
                              ▼
          data/gold/dimensions/dim_*.parquet   (durable, portable artifact)
          data/gold/facts/fact_*.parquet
                              │
                              ▼
                  data/gold/gold.duckdb   (SQL access + data-mart views)
```

## 2. OLAP Technology Decision

**Chosen: DuckDB** (embedded, free, open-source, MIT-licensed), `duckdb>=1.0`.

| Criterion (from this phase's requirements) | DuckDB |
|---|---|
| Not a commercial/cloud warehouse | ✅ fully local, no account, no billing |
| Free / portfolio-appropriate | ✅ pip-installable, zero infrastructure |
| Local development friendly | ✅ single file (`data/gold/gold.duckdb`), no Docker container, no new service |
| SQL accessible | ✅ full SQL, standard JDBC/ODBC drivers available |
| Native Parquet interoperability | ✅ `read_parquet()` queries Parquet directly, no import/ETL step needed |
| Future dbt integration path | ✅ `dbt-duckdb` adapter exists and is well-maintained |
| Future Power BI connectivity | ✅ via ODBC driver, or by exporting/reading the Parquet files directly |
| Consistent with project's Parquet-first convention | ✅ every prior layer (Bronze, Silver, CDC Bronze, CDC Silver) already materializes Parquet as the durable artifact — DuckDB is a query layer *on top of* that, not a replacement for it |

Explicitly **not** chosen, per this phase's boundary instruction (no cloud /
commercial warehouse to be silently selected): Snowflake, BigQuery,
Redshift, Databricks SQL. Also explicitly **not started** in this phase
(available future integration points, not implemented): ClickHouse, Trino,
Postgres-as-a-warehouse (a real option, since `finpay` already runs in
Docker — deferred because DuckDB satisfies every stated criterion with zero
additional infrastructure, which a second Postgres schema would not add).

**Materialization strategy**: every dimension and fact is written as Parquet
first (`data/gold/dimensions/`, `data/gold/facts/`) — the same durable,
portable artifact every other layer in this project produces — and then
loaded into `data/gold/gold.duckdb` via `CREATE OR REPLACE TABLE ... AS
SELECT * FROM read_parquet(...)`. Parquet remains the source of truth;
DuckDB is a disposable, rebuildable query cache over it (deleting
`gold.duckdb` and re-running `src.gold.pipeline.run()` fully reconstructs
it — verified in this phase).

## 3. Real Silver Structures Inspected (not assumed)

Verified directly against real Parquet in this phase before any dimension
or fact code was written:

- **API Silver schemas** (`pq.read_schema()` against `data/silver/*/data.parquet`):
  `merchant` (22 columns), `mcc` (10), `country` (14), `iso_currency` (10),
  `card_issuer` (14), `legal_entity` (26) — column names used in
  `src/gold/dimensions.py` are copied verbatim from these, not guessed.
- **`merchant.country_code` and `merchant.mcc_code` are 0/521 non-null** —
  re-confirmed in this phase, unchanged since S5/S6 (docs/21 open decision).
  This is why `fact_transactions.country_key` is always `UNKNOWN_KEY` (§10)
  and why `mcc_key` is resolved directly from the CDC transaction's own
  `mcc_code`, never through merchant.
- **CDC Silver `current_state` tables** are a single `data.parquet` per
  dataset and already exclude deleted rows (`src/cdc/silver.py`'s state
  machine removes a key from current state on delete) — Gold reads them
  as-is, no extra delete-filtering needed.
- **CDC Silver `event_log` tables** (`silver_cdc_transaction_event`,
  `silver_cdc_reward_event`) are **append-only across multiple
  `run_<ts>Z.parquet` files**, not one `data.parquet` — `src/gold/facts.py`
  reads and concatenates all of them.
- **A real, unanticipated data-quality finding** (see §16): `event_id` /
  `reward_id` are **not unique** in the event-log tables. The S10
  idempotency-test fixture (`tests/test_oltp.py`) repeatedly
  inserts+deletes the same hardcoded `EVT-9990001` / `RWD-9990001` against
  the live database every pytest session, producing genuine repeated
  `(event_id, operation='c')` CDC events at different `kafka_offset`s, plus
  `operation='d'` delete markers (some with blank/epoch-dated before-images
  from before S12's `REPLICA IDENTITY FULL` fix). Assuming `event_id`
  uniqueness — as originally planned — would have been a **fabricated
  grain**. See §7 for how this was actually handled.
- Verified clean joins with zero orphans (sampled/counted, not assumed):
  520/520 distinct CDC `transaction.merchant_id` values exist in
  `silver_merchant`; all 22 distinct `mcc_code` values in CDC transactions
  are valid `silver_mcc` codes; all 3 currency codes (GBP/EUR/USD) are valid
  `silver_iso_currency` codes; all 261 `bin_range` values in
  `silver_cdc_card_token` are valid `silver_card_issuer` codes; all 12
  client LEIs in `silver_cdc_client` are valid `silver_legal_entity` LEIs.

## 4. Dimensional Model Overview

**13 dimensions, 5 facts** — every one justified by data actually present
in Silver/CDC Silver; none invented.

```
                           dim_date
                               │
  dim_merchant ─┐              │              ┌─ dim_currency
  dim_mcc ───────┤              │              ├─ dim_card_issuer
  dim_country ───┤(country_key  │              │
                  │ always      │              │
                  │ UNKNOWN)    ▼              │
                  └────────► fact_transactions ◄┘
                               │  ▲  ▲  ▲
              dim_card_token ──┘  │  │  └── dim_client
              dim_cardholder ─────┘  │
              dim_program ───────────┘

  fact_transactions ◄── (transaction_id, degenerate join) ── fact_transaction_events
  fact_transactions ◄── (transaction_id, degenerate join) ── fact_settlements ── fact_reconciliation
  fact_transactions ◄── (transaction_id, degenerate join) ── fact_rewards ──► dim_offer ──► dim_campaign ──► dim_program

  dim_legal_entity ──► dim_client ──► dim_program ──► dim_campaign ──► dim_offer
                                            └────────► dim_cardholder ──► dim_card_token ──► dim_card_issuer
```

## 5. Dimension Catalog

| Dimension | Grain (1 row per) | Source | SCD | Rows (real, incl. UNKNOWN) |
|---|---|---|---|---|
| `dim_date` | calendar date | derived from real fact date columns (§12) | n/a | 268 |
| `dim_merchant` | `merchant_id` | `silver/merchant` | Type 2-ready, 1 real version (§11) | 522 |
| `dim_mcc` | `mcc_code` | `silver/mcc` | Type 1 | 982 |
| `dim_country` | `country_code_alpha3` | `silver/country` | Type 1 | 251 |
| `dim_currency` | `currency_code` | `silver/iso_currency` | Type 1 | 308 |
| `dim_card_issuer` | `bin_range` | `silver/card_issuer` | Type 1 | 374,789 |
| `dim_legal_entity` | `lei` | `silver/legal_entity` | Type 1 | 10,001 |
| `dim_client` | `client_id` | `silver_cdc_client` | Type 1 | 13 |
| `dim_program` | `program_id` | `silver_cdc_program` | Type 1 | 9 |
| `dim_campaign` | `campaign_id` | `silver_cdc_campaign` | Type 1 | 13 |
| `dim_offer` | `offer_id` | `silver_cdc_offer` | Type 1 | 21 |
| `dim_cardholder` | `cardholder_id` | `silver_cdc_cardholder` | Type 1 | 201 |
| `dim_card_token` | `token_id` | `silver_cdc_card_token` | Type 1 | 262 |

Naming resolves docs/21's open decision #1 (`dim_issuer` vs
`dim_card_issuer`) — **`dim_card_issuer`** was chosen (matches the source
Silver dataset name, avoids a synonym for the same concept).

`dim_card_issuer` and `dim_legal_entity` are large because they carry the
**complete external reference datasets** (all BIN ranges / all GLEIF LEI
records ingested in S5/S6), not just the subset referenced by FinPay's
synthetic OLTP data — this is real, verified, and intentional: a reference
dimension should hold the full domain, so it also resolves lookups the
current fact data doesn't yet exercise.

## 6. Fact Catalog

### `fact_transactions`
- **Business process**: a card authorization attempt.
- **Grain**: one row per `transaction_id`.
- **Source**: `silver_cdc_transaction` (current_state), joined to
  `dim_merchant`, `dim_mcc`, `dim_currency`, `dim_card_token`,
  `dim_cardholder`, `dim_program`, `dim_client` via the surrogate-key
  registries built in dependency order (§8).
- **Keys**: `date_key`, `merchant_key`, `mcc_key`, `currency_key`,
  `card_token_key`, `cardholder_key`, `program_key`, `client_key`,
  `country_key` (always `UNKNOWN_KEY`, §3/§10).
- **Measures**: `amount` (additive).
- **Non-additive attribute**: `mcc_confidence` (a quality score, not a
  monetary fact — kept as an attribute, not classified as a measure).
- **Degenerate dimensions**: `transaction_status`, `decline_reason`,
  `auth_code` — kept directly on the fact rather than a separate
  low-cardinality junk dimension (a documented choice, not an oversight).
- **Row count**: 4,000 (== CDC Silver source exactly, no exclusions).

### `fact_transaction_events`
- **Business process**: a lifecycle event on a transaction (authorization /
  capture / reversal) — a **factless fact** (no monetary measure;
  `event_count=1` lets any BI tool `SUM()` for counts).
- **Grain**: one row per **`(event_id, kafka_offset)`** — *not* `event_id`
  alone (§3/§7 — a real, verified finding, not the originally assumed grain).
- **Source**: `silver_cdc_transaction_event` (event_log, all `run_*.parquet`
  concatenated), **excluding `operation='d'` delete markers** (§7).
- **Keys**: `date_key`; `transaction_id` is a **degenerate join key** back
  to `fact_transactions` (fact-to-fact reference, not a conformed-dimension
  FK — the "one" side is itself a fact grain).
- **Row count**: 7,554 (== 7,560 source rows − 6 delete markers).

### `fact_settlements`
- **Business process**: a merchant settlement/payout for a transaction.
- **Grain**: one row per `settlement_id`.
- **Source**: `silver_cdc_settlement` (current_state).
- **Keys**: `date_key` (from `settlement_date`), `currency_key`;
  `transaction_id` is a degenerate join key. `settlement_batch_id` is kept
  as a degenerate attribute (no separate batch dimension — it carries no
  attributes of its own in current source data).
- **Measures**: `settlement_amount`, `fee_amount`, `net_amount` — all
  additive.
- **Row count**: 3,464 (== source exactly).

### `fact_reconciliation`
- **Business process**: matching a settlement against its expected value.
- **Grain**: one row per `reconciliation_id`.
- **Source**: `silver_cdc_reconciliation` (current_state).
- **Keys**: `date_key` (from `reconciled_date`); `settlement_id` is a
  degenerate join key.
- **Measures**: `expected_amount`, `actual_amount` — additive.
  `variance` is stored as-is (it is a real column, `actual - expected`) and
  is technically additive, but summing it answers "net over/under", not a
  magnitude-of-error metric — documented so it isn't misread.
- **Row count**: 3,464 (== source exactly).

### `fact_rewards`
- **Business process**: a cashback/loyalty reward event tied to a
  transaction and an offer.
- **Grain**: one row per **`(reward_id, kafka_offset)`** — same real-data
  justification as `fact_transaction_events` (§7).
- **Source**: `silver_cdc_reward_event` (event_log), excluding
  `operation='d'` delete markers.
- **Keys**: `offer_key` (via `dim_offer`), `date_key`, `currency_key`;
  `transaction_id` is a degenerate join key.
- **Measures**: `reward_amount` — additive; verified `NOT_QUALIFIED` rows
  carry a real `0.0000`, so a plain `SUM()` is always correct.
- **Row count**: 2,682 (== 2,688 source rows − 6 delete markers).

## 7. A Real Finding: Event-Log Grain and the Delete-Marker Exclusion

The original plan (before real-data verification) assumed
`fact_transaction_events`/`fact_rewards` grain = one row per `event_id` /
`reward_id`. Real data proved this false: the S10 OLTP idempotency test
fixture repeatedly inserts and deletes `EVT-9990001`/`RWD-9990001` against
the live Postgres database on every `pytest` run, and CDC Silver's own
design (docs/26 §6) deliberately keeps every occurrence for traceability —
so the natural key genuinely repeats.

**Resolution** (documented, not silently patched around):
1. Grain widened to `(event_id, kafka_offset)` / `(reward_id, kafka_offset)`
   — `kafka_offset` is already the dedup key S13's own consumer relies on,
   so this reuses an existing, meaningful identifier rather than inventing
   a surrogate row number.
2. `operation='d'` rows are excluded from both facts — a delete of an
   append-only business event is not itself a business event, and the
   pre-`REPLICA IDENTITY FULL` delete rows specifically carry blank business
   columns and an epoch (`1970-01-01`) `event_timestamp` that would corrupt
   `dim_date` and every degenerate attribute if kept.
3. The exclusion is **reconciled, not just filtered**:
   `src/gold/reconciliation.py::reconcile_event_log_fact` hard-fails unless
   `silver_row_count == len(fact_rows) + delete_marker_count` exactly — the
   same "every input row lands in exactly one named bucket" discipline used
   at every other layer.
4. A separate real finding surfaced by this: `RWD-9990001`'s `c` rows
   reference `offer_id="OFR-9001"`, which is **not** a real `dim_offer`
   row — the unknown-member convention (§10) catches this correctly
   (`offer_key = UNKNOWN_KEY`), verified in
   `tests/test_gold_facts.py::test_fact_rewards_excludes_delete_markers_and_uses_unknown_member_for_unresolved_offers`.

## 8. API Silver ↔ CDC Silver Convergence

| Gold entity | API Silver side | CDC Silver side | Join key | Missing-ref behavior |
|---|---|---|---|---|
| `fact_transactions.merchant_key` | `silver/merchant.merchant_id` | `silver_cdc_transaction.merchant_id` | exact string match | `UNKNOWN_KEY` (none observed in real data — 520/520 resolve) |
| `fact_transactions.mcc_key` | `silver/mcc.mcc_code` | `silver_cdc_transaction.mcc_code` | exact string match | `UNKNOWN_KEY` (none observed) |
| `fact_transactions.currency_key` | `silver/iso_currency.currency_code` | `silver_cdc_transaction.currency_code` | exact string match | `UNKNOWN_KEY` (none observed) |
| `dim_card_token.card_issuer_key` | `silver/card_issuer.bin_range` | `silver_cdc_card_token.bin_range` | exact string match | `UNKNOWN_KEY` (none observed — 261/261 resolve) |
| `dim_client.legal_entity_key` | `silver/legal_entity.lei` | `silver_cdc_client.lei` | exact string match | `UNKNOWN_KEY` (none observed — 12/12 resolve) |
| `dim_offer.eligible_mcc_key` | `silver/mcc.mcc_code` | `silver_cdc_offer.eligible_mcc_code` (nullable) | exact string match | `NULL` when source is `NULL` (11/20 offers — "any MCC eligible", a real business state, not a join failure) |
| `fact_transactions.country_key` | `silver/country` | *(no CDC-side country signal exists)* | **none — not built** | always `UNKNOWN_KEY` (§3, a real, verified absence, not invented) |

**Source precedence**: there is no case in this model where API Silver and
CDC Silver both claim authority over the same attribute — API Silver is
always the reference/lookup side (merchant, mcc, country, currency, card
issuer, legal entity), CDC Silver is always the transactional/event side
(client, program, campaign, offer, cardholder, card_token, transaction,
settlement, reconciliation, transaction_event, reward_event). No conflict
resolution rule was needed because none is exercised by real data.

**Type normalization**: every join key on both sides is already a plain
string in Silver (Debezium string / Silver `as_string()` on the CDC side,
Silver's own string columns on the API side) — no type coercion needed at
Gold, verified by comparing `pq.read_schema()` output on both sides before
writing any join code.

**Duplicate/null handling**: natural keys are deduplicated via
`set()` before surrogate-key assignment (`src/gold/keys.py::assign_surrogate_keys`
sorts and dedupes); a `NULL`/missing join value resolves to `UNKNOWN_KEY`
via `dict.get(key, UNKNOWN_KEY)` everywhere in `src/gold/dimensions.py` and
`src/gold/facts.py` — never a `KeyError`, never a silently dropped row.

## 9. Surrogate-Key Strategy

`src/gold/keys.py` — a small persisted JSON registry per dimension
(`data/gold/_key_registry/<dimension>.json`, `natural_key -> int`), mirroring
this project's established "small JSON state file" pattern
(`src/ingestion/watermark.py`, `src/cdc/checkpoint.py`). Natural keys are
assigned the next-available integer **in sorted order** on first
appearance; existing assignments are **never renumbered** — verified by
`tests/test_gold_keys.py::test_assign_surrogate_keys_never_renumbers_existing`
and by running the full pipeline twice and diffing the registry
(`tests/test_gold_pipeline_integration.py::test_pipeline_is_idempotent_on_rerun`).
This is what makes the "full rebuild every run" strategy (§13) safe:
rebuilding from scratch still produces the same surrogate key for the same
business entity every time.

`-1` is globally reserved for the unknown member (§10) and is never
assigned to a real natural key (enforced by starting `next_key` at 1 and
never assigning it during normal registry growth).

## 10. Unknown-Member Convention

Every dimension gets exactly one synthetic row with `surrogate_key = -1`
(`UNKNOWN_KEY`, `src/gold/config.py`), `natural_key = "UNKNOWN"`, and
human-readable placeholder attribute values (e.g. `"Unknown Merchant"`).
Every fact-builder function resolves a foreign key via
`registry.get(natural_key, UNKNOWN_KEY)` — a fact row is **never dropped**
and a join **never raises** because of a missing dimension match.
`src/gold/validation.py::validate_fk_integrity` explicitly treats
`UNKNOWN_KEY` as always valid (not an orphan) when checking every other FK.

Real cases where this fires today: `fact_transactions.country_key` (always,
§3/§8) and `fact_rewards.offer_key` for the `RWD-9990001` test-fixture rows
(§7) — both verified with dedicated tests, not just designed-for.

## 11. SCD Strategy Per Dimension

- **`dim_merchant`**: structurally **Type 2** (`effective_from`,
  `effective_to`, `is_current` columns present), because the original
  architecture (docs/11) calls for it. But real Bronze OSM data has only
  ever been ingested **once** (S5) — there is no second snapshot to derive
  real history from. Implemented: every merchant gets exactly one version
  (`effective_from = 2026-01-01` placeholder epoch, `effective_to = NULL`,
  `is_current = True`). Future-ready, not fabricated — a second Bronze OSM
  ingestion run would let this dimension start accumulating real history
  without a schema change.
- **Every other dimension: Type 1** (full overwrite each rebuild) — no
  dimension besides merchant has any documented Type 2 requirement in
  docs/21-26, and none of their sources currently provide more than one
  state per natural key.

## 12. `dim_date` Design

Built by `src/gold/dimensions.py::build_dim_date(min_date, max_date)` —
attributes: `full_date`, `year`, `quarter`, `month`, `month_name`, `day`,
`day_of_week` (ISO, ISO-verified against real calendar dates in
`tests/test_gold_dimensions.py`), `day_name`, `week_of_year`, `is_weekend`.

**The range is never hardcoded.** `src/gold/pipeline.py::run()` collects
every real date value actually present across every fact's date-bearing
source column — `transaction_timestamp`, `settlement_date`,
`reconciled_date`, and `event_timestamp` (from both event-log facts,
excluding delete markers) — and builds `dim_date` over exactly
`min(...)..max(...)` of that real set. In this run that range is
2026-01-01 to 2026-09-25 (the low end driven by the same `EVT-9990001` /
`RWD-9990001` test-fixture rows discussed in §7, which really do carry a
2026-01-01 timestamp on their `c`/insert occurrences — a real date, just an
unusually early one, not fabricated).

No separate `dim_time` (time-of-day) dimension was built: no documented KPI
in docs/22 requires sub-day granularity, and `transaction_timestamp` /
`event_timestamp` remain available as full-precision fact attributes for
any future time-of-day analysis — listed as future/open, not silently
dropped.

## 13. FX / Currency Analytical Approach

**No FX conversion is implemented.** `dim_currency` carries `minor_unit`
and `is_active` from `silver/iso_currency`, and every fact retains its
transaction-native `currency_key` — amounts are never summed across
currencies. `fact_transactions`/`fact_settlements` use GBP, EUR, and USD
(verified, §3) with no documented exchange-rate-to-single-currency
requirement anywhere in docs/9-26. Silver does carry an `fx_rate` dataset
(S4), but no business rule in this project defines *which* rate/date/pair
applies to which transaction — inventing one would violate this phase's
"no silent business rules" instruction. **Documented as an open/future
decision** (§20), not resolved here: a future KPI requiring a single
reporting currency would need an explicit, documented FX-conversion rule
first.

## 14. Merchant / MCC / Country Relationship

`fact_transactions` gets `mcc_key` directly from the CDC transaction's own
`mcc_code` (never through `dim_merchant`, since `merchant.mcc_code` is
permanently `NULL` — §3). `country_key` has **no real source signal at
all** — `merchant.country_code` is permanently `NULL` and no other
Silver/CDC Silver column carries a country for a transaction — so it is
always `UNKNOWN_KEY`, explicitly, rather than inferring a country from
(for example) the issuer's country or the client's country, which would be
a fabricated business relationship not supported by real data.

## 15. Client / Legal Entity / GLEIF Relationship

`dim_client.legal_entity_key` resolves `silver_cdc_client.lei` against
`silver/legal_entity.lei` — clean, 12/12, verified (§3/§8). This is the one
real, documented API-Silver-to-CDC-Silver dimensional relationship in the
model. `dim_legal_entity.entity_jurisdiction` is carried through
byte-for-byte (verified in
`tests/test_gold_dimensions.py::test_build_dim_legal_entity_preserves_entity_jurisdiction_exactly`),
per the non-negotiable rule in docs/21 §8. The `entity_jurisdiction →
silver_country` join (docs/21 open decision #2) is **still not built** —
carried forward as open (§20), consistent with the instruction not to
silently resolve inherited open decisions.

## 16. Gold-Level Data Quality Validation

`src/gold/validation.py` — deliberately **does not** re-check business
columns (required/positive/enum rules already enforced by Silver). It
checks what only a dimensional model can uniquely break:

- **Surrogate-key uniqueness** per dimension (`validate_surrogate_key_uniqueness`).
- **Natural-key uniqueness** per dimension (`validate_natural_key_uniqueness`)
  — would catch a real-world duplicate business entity slipping through
  Silver as two surrogate keys for one thing.
- **Fact grain** (`validate_fact_grain`) — no two fact rows share the same
  grain-defining key tuple.
- **FK integrity** (`validate_fk_integrity`) — every non-`UNKNOWN` fact FK
  must resolve to a real dimension row; `UNKNOWN_KEY` is always accepted.

All four raise `GoldDataQualityError` (hard-fail) rather than
quarantine-and-continue: unlike Bronze/Silver/CDC layers, Gold's inputs have
already passed per-record validation, so a Gold DQ failure indicates a
join/registry bug, not a business-data condition to set aside (documented
in `src/gold/config.py`). Verified with both positive and negative
(corruption) test cases in `tests/test_gold_validation.py`.

## 17. Reconciliation & the Control-Total Identity

`src/gold/reconciliation.py` reuses `src.silver.common.reconcile_counts` —
the same generic mechanism as every prior layer, not a new framework.

- `fact_transactions` / `fact_settlements` / `fact_reconciliation`: exact
  1:1 row-count identity against their CDC Silver source (no exclusions).
- `fact_transaction_events` / `fact_rewards`: `source_rows == fact_rows +
  delete_marker_rows` (§7's documented exclusion), hard-checked.
- **Control-total identity** (never hardcoded): `compute_control_totals()`
  sums `fact_transactions.amount` where `transaction_status='APPROVED'`,
  `fact_settlements.settlement_amount`, and
  `fact_reconciliation.expected_amount` fresh from Gold `Decimal` data every
  run; `verify_control_total_identity()` raises if the three don't match
  exactly. Verified in this run: all three equal **£435,106.16** (3,464
  approved transactions = 3,464 settlements = 3,464 reconciliation rows) —
  the same figure the project has produced since S9, now derived through
  five additional transformation layers (Postgres → Debezium → Kafka → CDC
  Bronze → CDC Silver → Gold) without drifting.

## 18. Idempotency & Rebuild Strategy

**Full rebuild every run** (matches the "simple overwrite" precedent
already established for `mcc`/`country`/`iso_currency`/`card_issuer` in
S3) — every dimension and fact is recomputed fresh from current
Silver/CDC Silver state, not incrementally merged. Idempotency is
guaranteed entirely by the surrogate-key registry (§9) being append-only
and non-renumbering. Verified directly: running `src.gold.pipeline.run()`
twice produces identical dimension/fact row counts, an identical
`dim_merchant` registry, and identical control totals
(`tests/test_gold_pipeline_integration.py::test_pipeline_is_idempotent_on_rerun`).

## 19. KPI Domain Readiness

| Domain | Supported by | Notes |
|---|---|---|
| A. Transaction/payment intelligence | `fact_transactions` + `dim_date`/`dim_currency`/`dim_mcc` | approval rate, volume by MCC/currency/date |
| B. Merchant intelligence | `fact_transactions` + `dim_merchant` (`merchant_mart`, §21) | per-merchant volume, approval/decline mix |
| C. Client/program intelligence | `fact_transactions` + `dim_client`/`dim_program`/`dim_campaign` | volume by client/program; campaign-level requires joining `dim_offer`→`fact_rewards` |
| D. Settlement | `fact_settlements` | settlement volume/fees by batch/date/currency |
| E. Reconciliation | `fact_reconciliation` | match/exception rates, variance by date |
| F. Rewards/CLO | `fact_rewards` + `dim_offer` | qualified vs. not-qualified reward volume by offer/campaign |
| G. Data engineering operations | reconciliation results + row counts returned by `pipeline.run()` | not yet a dedicated fact/mart — see `operations_mart` (§21, future) |

No metric above requires a relationship this phase didn't verify exists in
real data (§3/§8).

## 20. Data Mart Layer

Implemented as **SQL views inside `gold.duckdb`** — not physical tables,
not dbt models (dbt orchestration is explicitly out of scope for S14).
Two representative marts are built now (`src/gold/pipeline.py::_create_mart_views`):

- **`merchant_mart`**: per-merchant transaction count, approved-amount
  total, declined count, joined through `dim_mcc`.
- **`transaction_mart`**: transaction count and amount total by date,
  currency, and status.

The remaining five named in this phase's scope
(`client_program_mart`, `settlement_mart`, `reconciliation_mart`,
`rewards_mart`, `operations_mart`) follow the **identical pattern** — a
`CREATE OR REPLACE VIEW` over the same Gold tables — and are documented here
as concrete future work, not silently dropped:

- `client_program_mart`: `fact_transactions` grouped through
  `dim_program`→`dim_client`.
- `settlement_mart`: `fact_settlements` grouped by `settlement_batch_id`/date.
- `reconciliation_mart`: `fact_reconciliation` grouped by `match_status`/date.
- `rewards_mart`: `fact_rewards` grouped through `dim_offer`→`dim_campaign`.
- `operations_mart`: pipeline reconciliation/row-count results (§17/§18),
  which today are returned as a Python dict from `pipeline.run()` rather
  than persisted anywhere queryable — the concrete gap this mart would
  close.

## 21. Silver vs. Gold vs. Data Mart — Explicit Distinction

- **API Silver / CDC Silver**: one row per real-world entity/event, typed,
  validated, business-meaningful, but shaped like the **source system**
  (OLTP tables, OSM/GLEIF/ISO records) — not yet a dimensional model. No
  surrogate keys, no conformed cross-source joins, no unknown-member
  handling.
- **Gold**: the dimensional model itself — surrogate keys, star-schema
  facts/dimensions, conformed joins across API Silver and CDC Silver,
  unknown-member fallback, grain/FK integrity guarantees. Shaped for
  **general-purpose analytical querying**, not any one report.
- **Data Marts**: pre-aggregated, **consumption-shaped** views on top of
  Gold, each answering a specific KPI domain (§19) directly (e.g.
  `merchant_mart` is already grouped/summed — a BI tool doesn't need to
  know the star schema to use it). This project implements marts as views
  (§20), not separate physical copies, so there is exactly one durable
  source of the numbers (the Gold Parquet files) and the marts can never
  drift out of sync with it.

## 22. Real Local Verification (live, no external API calls)

Performed against the real local Silver/CDC Silver data already on disk
(no Bronze regeneration, no API calls):

- Full pipeline run (`src.gold.pipeline.run()`) — succeeds, all
  reconciliations pass, control-total identity holds exactly.
- Idempotency: pipeline run twice, dimension/fact row counts, surrogate-key
  registries, and control totals all identical.
- `gold.duckdb` created, queryable, `merchant_mart`/`transaction_mart` views
  present and reconcile (`SUM(merchant_mart.approved_amount_total) ==
  approved_transaction_total`, verified exactly).
- Bronze/API-Silver/watermark files' mtimes unchanged before/after running
  the Gold pipeline (verified with `os.stat().st_mtime_ns` diffing in
  `tests/test_gold_pipeline_integration.py`) — confirms the read-only
  contract in `src/gold/__init__.py` is actually honored, not just stated.
- 65 new tests (see §23) — 100% pass.

## 23. Operational Visibility (no new GUI tooling installed)

Consistent with every prior phase, no new GUI was installed. Tools already
available and applicable to Gold:

- **DuckDB CLI** (`duckdb data/gold/gold.duckdb`) or the Python API — ad hoc
  SQL against every dimension/fact/mart.
- **Any Parquet-aware tool** (`pq.read_table`, pandas, `duckdb.read_parquet`)
  — the durable Parquet files under `data/gold/` are the ground truth
  independent of the DuckDB file.
- **pgAdmin / `psql`** (already available from S10) — still the tool for
  inspecting the OLTP source, upstream of everything in this phase.
- **Kafka CLI / Kafka Connect REST API** (already available from S11/S12) —
  unchanged, not touched by this phase.
- **Future**: `dbt-duckdb` CLI (S15+), Power BI via ODBC or direct Parquet
  import (S15+) — both explicitly out of scope here, per this phase's
  technology boundary.

## 24. Known Limitations / Open Decisions

**Carried forward, unresolved (per instruction — not silently closed):**

1. `merchant.country_code` / `merchant.mcc_code` permanently `NULL` (docs/21) —
   still the reason `fact_transactions.country_key` is always `UNKNOWN_KEY`.
2. OSM `shop=vacant` permanence (docs/21/22) — unchanged, still present in
   `dim_merchant` as real data.
3. Merchant/GLEIF upsert conflict policy (docs/21) — Gold is Type 1
   overwrite for every non-merchant dimension anyway, so this remains a
   Silver-layer question, not newly resolved here.
4. `iso_currency.minor_unit` blank handling (docs/21 open decision #5) —
   `dim_currency.minor_unit` carries whatever Silver produced, untouched.
5. `card_issuer.issuer_name` blank handling (docs/21 open decision #4) —
   carried into `dim_card_issuer.issuer_name` as-is.
6. `entity_jurisdiction → silver_country` join (docs/21 open decision #2) —
   **still not built** (§15); `dim_legal_entity` has no `country_key`.
7. S6 address-schema documentation discrepancy — unaffected by Gold, which
   does not use the address sub-fields analytically.
8. `entity_creation_date` representation — carried through unmodified in
   `dim_legal_entity` (not currently used as a Gold date key).
9. Typed "transaction history" dataset (docs/26 §3) — still deferred; CDC
   Bronze retains full raw history if ever needed.
10. Kafka multi-partition production design, schema registry, connector
    transaction metadata, CDC quarantine retry edge case (docs/24/25/26) —
    all unaffected by and unchanged by this phase.

**New, S14-specific open decisions (not silently resolved):**

11. **FX/reporting-currency conversion** (§13) — no rule exists in any prior
    phase's documentation for which rate/date applies to which transaction;
    not invented here. A future cross-currency KPI needs this decided
    first.
12. **5 of 7 data marts** (§20) are documented, not yet built as views —
    same pattern as the 2 implemented ones, straightforward follow-up.
13. **`operations_mart`** specifically has no durable home yet for pipeline
    run metadata (row counts, reconciliation results) — currently returned
    as an in-memory dict from `pipeline.run()` only.
14. **No `dim_time` (time-of-day)** dimension — no documented KPI requires
    it yet; full-precision timestamps remain on fact rows if ever needed.
15. **Postgres-as-a-second-warehouse-target** (docs/21 open decision #7, for
    Silver) — DuckDB was chosen for *Gold* in this phase; whether Silver
    itself ever gets a warehouse target remains open and unaffected.

## 25. Next Phase Boundary

S15+ = Airflow orchestration / dbt models over this Gold layer / Power BI
dashboards / cloud deployment. **Not started here**, per this phase's
explicit technology boundary. This phase produces exactly what a future
Airflow DAG would call as one task and what a future dbt project would
either replace incrementally or sit downstream of — the Gold Parquet files
and `gold.duckdb` are the stable interface either would consume.
