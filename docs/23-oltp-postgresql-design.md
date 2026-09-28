# 23 — OLTP PostgreSQL Design (Phase S10)

Status: **IMPLEMENTED AND RUNNING**. This document covers the real PostgreSQL
OLTP database built in Phase S10, seeded from S9's synthetic Parquet data. It
does **not** cover CDC/Kafka/Debezium, Bronze ingestion of this data, Silver,
Gold, or orchestration — all explicitly deferred to future phases (§16).

## 1. Purpose

S9 produced synthetic business data as Parquet files — a reasonable stand-in,
but not an "operational system." FinPay's real architecture has its
transactional data originate in an actual application database, not a batch
file. S10 makes that real: a running PostgreSQL instance holding the same S9
data, normalized as genuine operational tables (not analytical/dimensional
ones), that a future CDC pipeline can actually tail.

## 2. Relationship to External APIs

None. S10 makes zero external API calls (no GLEIF/OSM/Frankfurter). It reads
only `data/synthetic_oltp/*.parquet` (S9's local output) and writes only to
the local PostgreSQL container.

## 3. Relationship to Bronze/Silver — Why OLTP Is Separate From the Data Lake

This project has always had two independent data origins (docs/08 §1):
external/public APIs + reference data flowing through **Bronze → Silver**
(S1-S8, unchanged, untouched by S10), and the FinPay application's own
transactional data, which belongs to a **separate operational system**
(PostgreSQL OLTP) that Bronze will *later* ingest FROM via CDC — it is not
itself a Bronze/Silver artifact. Conflating the two would be wrong: an OLTP
database is normalized for transactional integrity (many small, fast,
constraint-checked writes); Bronze/Silver are file-based, append/upsert,
batch-oriented. S10 keeps them fully separate: `src/oltp/` never imports
`src.ingestion` or `src.silver`'s transform modules, and nothing under
`data/bronze/`, `data/watermarks/`, or existing Silver transformation logic
was touched (confirmed in the completion report).

## 4. Database Architecture — Conflict Discovered and How It Was Resolved

**Discovered before writing any code** (per this phase's own "inspect first"
instruction): `docker/docker-compose.yml` and `.env` **already existed** in
this repository, and `docker ps` showed a healthy, 26-hours-running container
`merchantmcc_postgres` — pre-provisioned ahead of this phase. Its database is
named **`merchantmcc`**, not `finpay_oltp` as this phase's prompt suggested
("Prefer a clean PostgreSQL database structure such as: finpay_oltp").

This is reported, not silently resolved either way: per this phase's own
explicit instruction — "If an existing Docker structure already exists, reuse
it instead of creating a competing setup" — the existing container, database,
`.env`, and `docker-compose.yml` were **reused exactly as they already were**.
No new database was created, no container was recreated, no credentials were
changed. Instead, a dedicated schema `finpay` was created **inside** the
existing `merchantmcc` database (satisfying §4's "use a dedicated application
schema if appropriate" without discarding pre-existing, working
infrastructure). All 11 OLTP tables live in `finpay.*`.

```
docker exec merchantmcc_postgres psql -U merchantmcc -d merchantmcc
=> \dn            -- schemas: public, finpay
=> \dt finpay.*    -- 11 tables
```

## 5. Table Inventory

| Table | Rows loaded | Grain |
|---|---:|---|
| `finpay.clients` | 12 | one client (real GLEIF LEI-backed) |
| `finpay.programs` | 8 | one program |
| `finpay.campaigns` | 12 | one campaign |
| `finpay.offers` | 20 | one offer |
| `finpay.cardholders` | 200 | one tokenized cardholder |
| `finpay.card_tokens` | 261 | one card token |
| `finpay.transactions` | 4,000 | one transaction attempt |
| `finpay.transaction_events` | 7,548 | one lifecycle event |
| `finpay.settlements` | 3,464 | one settled transaction |
| `finpay.reconciliation` | 3,464 | one match/exception |
| `finpay.reward_events` | 2,676 | one reward evaluation |

No `dim_*`/`fact_*`/`gold_*`/`mart_*` tables exist anywhere in this schema —
purely normalized operational tables, per this phase's explicit instruction.

## 6. Relationships / Primary & Foreign Keys

Full DDL: `src/oltp/schema.py::DDL_STATEMENTS`. Summary:

```
clients (PK client_id, UNIQUE lei)
   └─ programs (PK program_id, FK client_id)
        ├─ campaigns (PK campaign_id, FK program_id)
        │     └─ offers (PK offer_id, FK campaign_id)
        └─ cardholders (PK cardholder_id, FK enrolled_program_id -> programs)
              └─ card_tokens (PK token_id, FK cardholder_id)
                    └─ transactions (PK transaction_id, FK token_id)
                          ├─ transaction_events (PK event_id, FK transaction_id)
                          ├─ settlements (PK settlement_id, FK transaction_id UNIQUE)
                          │     └─ reconciliation (PK reconciliation_id, FK settlement_id UNIQUE)
                          └─ reward_events (PK reward_id, FK transaction_id, FK offer_id -> offers)
```

`transactions.merchant_id`/`mcc_code`/`currency_code` and several
country-code columns reference **real Silver dimensions** (`silver_merchant`,
`silver_mcc`, `silver_iso_currency`, `silver_country`) that live in a
completely different storage layer (Parquet, not this database) — PostgreSQL
cannot declare a `REFERENCES` constraint against data it doesn't hold, so
these stay plain `TEXT`/`CHAR` columns, and their referential validity against
Silver is checked in application code (`src/oltp/validation.py`), exactly as
S9's own validator already did.

### Real dependency order (calculated, not assumed)

`src/oltp/schema.py::load_order()` topologically sorts the declared FK graph
(`TABLE_DEPENDENCIES`) with Kahn's algorithm — it does **not** hard-code the
prompt's suggested conceptual order. The two orders mostly agree, but differ
in one place: `reward_events` only depends on `transactions` and `offers` —
**not** on `settlements`/`transaction_events` — so the real, calculated order
loads it immediately after `transactions`, interleaved with (not after)
`transaction_events`/`settlements`:

```
clients, programs, campaigns, cardholders, card_tokens, offers,
transactions, reward_events, settlements, transaction_events, reconciliation
```

This is a correct topological order (every dependency is satisfied before the
table that needs it) and was verified by loading successfully in this exact
sequence with zero constraint violations.

## 7. Indexes — and why each one exists

Only OLTP lookup patterns, never analytical/aggregation indexes:

| Index | Justification |
|---|---|
| `transactions(token_id)` | "show this card's transaction history" — a real cardholder-service/fraud-check lookup |
| `transactions(merchant_id)` | "show this merchant's transactions" — a merchant-portal lookup |
| `transactions(transaction_timestamp)` | "recent activity for this token/merchant" / batch cutoff queries — an operational time-bound lookup, not a BI trend query |
| `transaction_events(transaction_id)` | "get lifecycle status for this transaction" — used whenever a transaction's current state is displayed |
| `reward_events(transaction_id)` | "show rewards applied to this transaction" |
| `card_tokens(cardholder_id)` | "list this cardholder's tokens" |

`settlements.transaction_id` and `reconciliation.settlement_id` are declared
`UNIQUE`, which PostgreSQL auto-indexes — no separate index was added for
those. Small reference tables (`clients`, `programs`, `campaigns`, `offers` —
8-20 rows each) were deliberately **not** given secondary indexes; at that
cardinality an index adds pure overhead with no lookup benefit.

## 8. Data Types — Parquet → PostgreSQL Conversion

| Concept | S9 Parquet type | PostgreSQL type | Why |
|---|---|---|---|
| Identifiers (`*_id`, `lei`, `bin_range`, ...) | `string` | `VARCHAR(n)`/`CHAR(n)` | never numeric — same discipline as Silver |
| Monetary amounts (`amount`, `*_amount`, `variance`, `offer_value`) | `double` (float64) | `NUMERIC(18,2)`/`NUMERIC(18,4)` | **exact** decimal representation required for money; float64 is not. The loader converts via `Decimal(str(value))`, never `Decimal(value)` directly, specifically to avoid surfacing IEEE-754 binary-float noise for values that were always clean 2-4-decimal amounts. Verified exactly against a real sample in `tests/test_oltp.py::test_financial_amounts_preserved_exactly_for_every_transaction`. |
| Dates (`*_date`) | `string` (ISO 8601) | `DATE` | parsed via `date.fromisoformat()` before insert |
| Timestamps (`*_timestamp`) | `string` (ISO 8601) | `TIMESTAMP` | parsed via `datetime.fromisoformat()` before insert |
| `mcc_confidence` | `double` | `NUMERIC(3,2)` CHECK 0-1 | a confidence score, not money, but still exact-decimal for consistency and constraint-checkability |
| Enums (`status`, `client_type`, `event_type`, ...) | `string` | `VARCHAR(n)` + `CHECK (... IN (...))` | PostgreSQL `ENUM` types were considered and rejected — they're painful to alter later (a real operational concern this early-stage schema doesn't need yet); `CHECK` gives the same guarantee with ordinary `ALTER TABLE ... DROP/ADD CONSTRAINT` flexibility |
| `source_type` | `string` (`"synthetic"`) | `VARCHAR(10)` + `CHECK (source_type = 'synthetic')` | DB-enforced, not just convention — see §14 |

## 9. Loading Process

`src/oltp/loader.py::load_all()`:

1. Compute the real load order (§6).
2. For each table: read `data/synthetic_oltp/<table>.parquet`, coerce
   date/timestamp/decimal columns to native Python types, `COPY` the rows into
   a per-transaction `TEMP` staging table (`ON COMMIT DROP`), then
   `INSERT ... SELECT FROM staging ON CONFLICT (<primary key>) DO NOTHING`
   into the real table.
3. Report `inserted`/`skipped_existing` per table.
4. Commit once, at the very end, only if every table succeeded.

## 10. Idempotency Strategy (chosen, not silent)

**`INSERT ... ON CONFLICT (primary key) DO NOTHING`**, fed by a fast `COPY`
into a staging table — not truncate-and-reload. Reasoning: this loader
represents an OLTP system's *initial seed*, but the strategy is chosen for
what an OLTP database's semantics actually are, not just for this one run — a
live operational database would never truncate its transaction ledger on
every deploy. `ON CONFLICT DO NOTHING` gives both correctness (a rerun against
the same Parquet snapshot inserts nothing new — verified:
`tests/test_oltp.py::test_rerunning_loader_inserts_zero_new_rows`) and speed
(`COPY`'s bulk protocol, not row-by-row `INSERT`).

## 11. Transaction Strategy

The **entire** load — all 11 tables, in dependency order — runs inside **one**
PostgreSQL transaction (`psycopg`'s default `autocommit=False`; a single
`connect()`, one `commit()` only at the very end, `rollback()` on any
exception). If any table fails partway (a constraint violation on, say,
table 8 of 11), everything — including the 7 tables that "succeeded" earlier
in that same run — rolls back together. The database is never left partially
loaded. Proven with an isolated, disposable fixture (new, non-colliding
primary keys, e.g. `CLI-9001`) whose last table deliberately violates a CHECK
constraint: `tests/test_oltp.py::test_load_all_rolls_back_completely_on_failure`
confirms every one of that fixture's rows — including the client and
transaction that "loaded" earlier in the same run — are absent afterward.

## 12. Control Totals (reproduced inside PostgreSQL, not hardcoded)

`src/oltp/validation.py::verify_control_totals()` recomputes S9's identity
using live `SUM()`/`COUNT()` queries against the database, and
`verify_control_totals_match_source()` cross-checks those against numbers
freshly recomputed from the Parquet source (never a hardcoded constant):

```
gross_approved_transaction_amount = £435,106.16
gross_settlement_amount           = £435,106.16
reconciliation_expected_total     = £435,106.16
```

Plus: settlement coverage (3,464 settlements = 3,464 approved transactions),
reconciliation coverage (3,464 = 3,464 settlements), event composition (4,000
AUTHORIZATION events = 4,000 transactions).

## 13. How to Start PostgreSQL

```bash
cd docker
docker compose up -d
```
(Already running for this phase — reused, not restarted, per §4.)

## 14. How to Initialize the Schema

```bash
.venv/Scripts/python.exe -c "from src.oltp import database; database.init_schema()"
```
Safe to rerun — every statement is `CREATE ... IF NOT EXISTS`, never
destructive (no `DROP`).

## 15. How to Load S9 Data / How to Verify

```bash
.venv/Scripts/python.exe -c "from src.oltp.loader import load_all; print(load_all().as_dict())"
.venv/Scripts/python.exe -c "from src.oltp.validation import verify_all; print(verify_all())"
```

## 16. Future CDC Entry Point (NOT implemented in S10)

```
[S10, done]                [future S11+]
PostgreSQL OLTP  ────────▶  Debezium CDC  ──▶  Kafka  ──▶  Bronze  ──▶  Silver ──▶ ...
```

Design choices made *now* specifically to keep that later step simple, per
this phase's "future CDC compatibility" instruction — none added merely
because "CDC might need it":

- **Stable, deterministic primary keys** on every table (`CLI-0001`,
  `TXN-0000001`, ...) — never a mutable natural key.
- **`transaction_timestamp` / `event_timestamp` / `settlement_date` /
  `reconciled_date`** already exist because the *business model* requires them
  (S9), not as CDC scaffolding — Debezium can key off them later for free.
- **A clear, append-heavy transaction lifecycle** (`transaction_events` is
  itself already an event log) — a natural fit for CDC's own append-oriented
  model.
- **No `created_at`/`updated_at` audit columns were added.** Every S9 table's
  business timestamp already IS its meaningful creation/event time
  (`onboarding_date`, `transaction_timestamp`, `event_timestamp`, ...); a
  separate audit-timestamp pair would duplicate that without a currently
  justified use, so none was added — an explicit choice, not an oversight.
- Logical replication (`wal_level=logical`, publication/slot setup) was
  **not** configured — that's Debezium/S11's own setup work, not a schema
  concern.

## 17. Explicitly Deferred (NOT started in S10)

Debezium, Kafka, Kafka Connect, Spark/PySpark/Spark Structured Streaming,
Airflow, dbt, OLAP warehouse, dimensional model, Gold layer, data marts,
Power BI, Snowflake, Databricks, ClickHouse, Trino, cloud deployment. `src/oltp/`
contains no code for any of these.

## 18. Open Decisions

- **Database naming** (§4): `merchantmcc` (pre-existing) vs. the prompt's
  suggested `finpay_oltp` — resolved by reuse + a dedicated `finpay` schema;
  flagged for confirmation this is acceptable long-term, or whether a true
  rename/new database is wanted later.
- All Silver-layer open items (merchant `country_code`, `shop=vacant`
  permanence, merchant/GLEIF upsert-conflict policy, ISO `minor_unit`, card
  issuer `issuer_name`, GLEIF→country join, warehouse target, S6 address-
  schema/`entity_creation_date` decisions) — untouched, carried forward.
- All S9-scoped open items (synthetic MCC crosswalk's future status, docs/11
  fact-naming reconciliation, no FX-converted transactions, reversals don't
  adjust settlement totals) — untouched, carried forward.
