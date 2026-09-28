# 22 — Synthetic Business Data Design (Phase S9)

Status: **IMPLEMENTED (generator + validator), NOT YET INGESTED**. This document
covers the synthetic OLTP business-domain foundation built in Phase S9. It does
**not** cover Bronze ingestion of this data (a future phase), Gold, dbt, or any
orchestration — those remain deferred per §17.

## 1. Purpose

Silver (S1-S8) gives this project real, validated reference and entity data —
merchants, MCC, country, currency, card-issuer BIN ranges, and GLEIF legal
entities — but none of it is a *transaction*. FinPay's seven business problems
(merchant intelligence, issuer reporting, network reporting, program-owner
reporting, rewards/CLO, settlement & reconciliation, data-engineering
operations) all require a transaction lifecycle, which cannot legally or
practically come from real card data. S9 builds a clearly-labeled, reproducible,
real-dimension-grounded **synthetic OLTP layer** representing the operational
system (future PostgreSQL) that a future Bronze ingestion phase will extract
from — closing the gap between "real reference data" and "the future Gold
layer" without inventing a disconnected, non-traceable dataset.

## 2. Business-Domain Model

```
                     REAL (Silver, S1-S8)                 SYNTHETIC (S9, this phase)
                 ┌───────────────────────────┐        ┌───────────────────────────────┐
                 │ silver_legal_entity (GLEIF)│───lei─→│ clients (issuer/network/       │
                 │ silver_mcc                 │        │          program_owner)        │
                 │ silver_country              │        │   └─ programs                  │
                 │ silver_iso_currency          │        │        └─ campaigns            │
                 │ silver_card_issuer (BIN)     │        │             └─ offers           │
                 │ silver_merchant (OSM)        │        │   cardholders ─ card_tokens     │
                 └───────────────────────────┘        │        └─ transactions            │
                                                        │             ├─ transaction_events │
                                                        │             ├─ settlements        │
                                                        │             │     └─ reconciliation│
                                                        │             └─ reward_events       │
                                                        └───────────────────────────────┘
```

## 3. Source vs Synthetic Data Boundary

Every synthetic table carries `source_type = "synthetic"` on every row — the
same `source_type` convention docs/08 already specifies ("carried from Bronze
through to any delivered file"). Field-by-field boundary:

| Table | Real fields | Synthetic fields |
|---|---|---|
| `clients` | `lei`, `legal_name`, `country_code` (all copied verbatim from a real, distinct `silver_legal_entity` row) | `client_id`, `client_type`, `onboarding_date`, `status` |
| `programs`/`campaigns` | `currency_code` (real ISO code) | everything else |
| `offers` | `eligible_mcc_code` (real MCC code, when set) | everything else |
| `cardholders` | `country_code` (real alpha-2) | `pseudonym` (a deliberately non-realistic label like `Cardholder-000123` — never a fabricated real-looking name, per the project's no-fabricated-PII rule) |
| `card_tokens` | `bin_range`, `card_brand` (real) | `token_id` (a synthetic `TKN-<hex>` string — never a real/Luhn-valid PAN structure) |
| `transactions` | `merchant_id` (real), `mcc_code` (real *code*, synthetically *assigned* — see §17), `currency_code` (real) | `amount`, `status`, timestamps |
| all facts | — | fully synthetic, referencing the above |

No real card numbers, no fabricated realistic personal names/addresses, no
invented merchants/MCC/currency/country/BIN values anywhere.

## 4. OLTP Schema (PostgreSQL — documented, not yet implemented)

**PostgreSQL status**: not installed in this environment (`psql`/`pg_ctl`
absent; no `postgresql` Windows service). Per this phase's own instructions —
"document the exact required setup... do not introduce cloud infrastructure...
do not create Docker changes unless explicitly required" — no database was
installed or started. The schema below is the proposed DDL for the future
PostgreSQL OLTP; S9 implements only its Python-generated Parquet equivalent
(`data/synthetic_oltp/*.parquet`), one file per table, matching this schema's
columns exactly.

**Proposed local setup (when approved)**: PostgreSQL 16, installed via the
official free/open-source Windows installer (or `winget install PostgreSQL.PostgreSQL`),
a single local instance, one database `finpay_oltp`, no cloud/managed service.
Docker is available in this environment but was not used, since Docker changes
were not explicitly requested.

```sql
-- Operational / source-system tables (the future OLTP) — 1:1 with the
-- synthetic Parquet tables' columns; identical grain and keys.
CREATE TABLE clients (
    client_id        VARCHAR(10) PRIMARY KEY,
    lei               CHAR(20) NOT NULL UNIQUE,
    legal_name        VARCHAR NOT NULL,
    client_type       VARCHAR(20) NOT NULL CHECK (client_type IN ('ISSUER','NETWORK','PROGRAM_OWNER')),
    country_code      CHAR(2),
    onboarding_date   DATE NOT NULL,
    status            VARCHAR(10) NOT NULL,
    source_type       VARCHAR(10) NOT NULL DEFAULT 'synthetic'
);

CREATE TABLE programs (
    program_id        VARCHAR(10) PRIMARY KEY,
    client_id         VARCHAR(10) NOT NULL REFERENCES clients(client_id),
    program_name      VARCHAR NOT NULL,
    program_type      VARCHAR(20) NOT NULL,
    currency_code     CHAR(3) NOT NULL,
    status            VARCHAR(10) NOT NULL,
    start_date        DATE NOT NULL,
    source_type       VARCHAR(10) NOT NULL DEFAULT 'synthetic'
);

CREATE TABLE campaigns (
    campaign_id       VARCHAR(10) PRIMARY KEY,
    program_id        VARCHAR(10) NOT NULL REFERENCES programs(program_id),
    campaign_name     VARCHAR NOT NULL,
    start_date        DATE NOT NULL,
    end_date          DATE NOT NULL,
    status            VARCHAR(10) NOT NULL,
    source_type       VARCHAR(10) NOT NULL DEFAULT 'synthetic'
);

CREATE TABLE offers (
    offer_id                 VARCHAR(10) PRIMARY KEY,
    campaign_id              VARCHAR(10) NOT NULL REFERENCES campaigns(campaign_id),
    eligible_mcc_code        CHAR(4),                     -- real MCC code, NULL = any MCC
    offer_type                VARCHAR(20) NOT NULL CHECK (offer_type IN ('CASHBACK_PCT','FIXED_AMOUNT')),
    offer_value                NUMERIC(18,4) NOT NULL CHECK (offer_value > 0),
    min_transaction_amount     NUMERIC(18,2) NOT NULL CHECK (min_transaction_amount > 0),
    currency_code               CHAR(3) NOT NULL,
    status                       VARCHAR(10) NOT NULL,
    valid_from                   DATE NOT NULL,
    valid_to                     DATE NOT NULL,
    source_type                  VARCHAR(10) NOT NULL DEFAULT 'synthetic'
);

CREATE TABLE cardholders (
    cardholder_id       VARCHAR(10) PRIMARY KEY,
    pseudonym            VARCHAR NOT NULL,     -- never a realistic fabricated name
    country_code          CHAR(2) NOT NULL,
    enrolled_program_id    VARCHAR(10) NOT NULL REFERENCES programs(program_id),
    enrollment_date         DATE NOT NULL,
    source_type              VARCHAR(10) NOT NULL DEFAULT 'synthetic'
);

CREATE TABLE card_tokens (
    token_id         VARCHAR(24) PRIMARY KEY,   -- synthetic token, never a real/Luhn-valid PAN
    cardholder_id    VARCHAR(10) NOT NULL REFERENCES cardholders(cardholder_id),
    bin_range        CHAR(6) NOT NULL,           -- real BIN, FK to the Silver card_issuer reference (not enforceable in-DB across layers)
    card_brand       VARCHAR NOT NULL,
    token_status     VARCHAR(10) NOT NULL,
    issued_date      DATE NOT NULL,
    source_type      VARCHAR(10) NOT NULL DEFAULT 'synthetic'
);

CREATE TABLE transactions (
    transaction_id          VARCHAR(12) PRIMARY KEY,
    token_id                 VARCHAR(24) NOT NULL REFERENCES card_tokens(token_id),
    merchant_id               VARCHAR NOT NULL,     -- real OSM merchant_id, FK to Silver (cross-layer, not in-DB enforceable)
    mcc_code                   CHAR(4) NOT NULL,      -- real MCC code, synthetically ASSIGNED per merchant (see §17)
    mcc_confidence               NUMERIC(3,2) NOT NULL,
    currency_code                CHAR(3) NOT NULL,
    amount                        NUMERIC(18,2) NOT NULL CHECK (amount > 0),
    transaction_timestamp          TIMESTAMP NOT NULL,
    status                          VARCHAR(10) NOT NULL CHECK (status IN ('APPROVED','DECLINED')),
    decline_reason                  VARCHAR(30),
    auth_code                        VARCHAR(6),
    source_type                      VARCHAR(10) NOT NULL DEFAULT 'synthetic'
);

CREATE TABLE transaction_events (
    event_id           VARCHAR(12) PRIMARY KEY,
    transaction_id      VARCHAR(12) NOT NULL REFERENCES transactions(transaction_id),
    event_type            VARCHAR(20) NOT NULL CHECK (event_type IN ('AUTHORIZATION','CAPTURE','REVERSAL')),
    event_timestamp         TIMESTAMP NOT NULL,
    event_status              VARCHAR(20) NOT NULL,
    source_type                 VARCHAR(10) NOT NULL DEFAULT 'synthetic'
);

CREATE TABLE settlements (
    settlement_id          VARCHAR(12) PRIMARY KEY,
    transaction_id          VARCHAR(12) NOT NULL UNIQUE REFERENCES transactions(transaction_id),
    settlement_batch_id       VARCHAR(20) NOT NULL,
    settlement_date             DATE NOT NULL,
    settlement_amount            NUMERIC(18,2) NOT NULL CHECK (settlement_amount > 0),
    settlement_currency            CHAR(3) NOT NULL,
    fee_amount                       NUMERIC(18,2) NOT NULL CHECK (fee_amount >= 0),
    net_amount                        NUMERIC(18,2) NOT NULL CHECK (net_amount > 0),
    source_type                        VARCHAR(10) NOT NULL DEFAULT 'synthetic'
);

CREATE TABLE reconciliation (
    reconciliation_id     VARCHAR(12) PRIMARY KEY,
    settlement_id           VARCHAR(12) NOT NULL UNIQUE REFERENCES settlements(settlement_id),
    expected_amount           NUMERIC(18,2) NOT NULL,
    actual_amount               NUMERIC(18,2) NOT NULL,
    variance                     NUMERIC(18,2) NOT NULL,
    match_status                   VARCHAR(10) NOT NULL CHECK (match_status IN ('MATCHED','EXCEPTION')),
    reconciled_date                  DATE NOT NULL,
    source_type                        VARCHAR(10) NOT NULL DEFAULT 'synthetic'
);

CREATE TABLE reward_events (
    reward_id               VARCHAR(12) PRIMARY KEY,
    transaction_id            VARCHAR(12) NOT NULL REFERENCES transactions(transaction_id),
    offer_id                    VARCHAR(10) NOT NULL REFERENCES offers(offer_id),
    qualification_status          VARCHAR(15) NOT NULL CHECK (qualification_status IN ('QUALIFIED','NOT_QUALIFIED')),
    reward_amount                   NUMERIC(18,2) NOT NULL CHECK (reward_amount >= 0),
    reward_currency                   CHAR(3) NOT NULL,
    event_timestamp                     TIMESTAMP NOT NULL,
    source_type                           VARCHAR(10) NOT NULL DEFAULT 'synthetic'
);
```

**Operational vs analytical distinction**: everything above is *operational* —
one row per business event, no aggregation, no dimensional modeling, no SCD.
Future analytical dimensions (`dim_client`, `dim_program`, `dim_offer`, ...) and
facts (`fact_transactions`, ...) are **derived FROM** these tables in a future
Gold phase — none of the OLTP tables are Gold tables themselves (§15 maps this
explicitly). No Gold table was built inside this schema, per this phase's
explicit instruction.

## 5. Synthetic Dataset Definitions, Grain, Keys — Summary Table

| Table | Grain (one row = ) | PK | Key FKs |
|---|---|---|---|
| `clients` | one client entity | `client_id` | `lei` → real `silver_legal_entity` |
| `programs` | one loyalty/rewards program | `program_id` | `client_id` → `clients` (PROGRAM_OWNER only), `currency_code` → real `silver_iso_currency` |
| `campaigns` | one campaign | `campaign_id` | `program_id` → `programs` |
| `offers` | one offer | `offer_id` | `campaign_id` → `campaigns`, `eligible_mcc_code` → real `silver_mcc` (nullable) |
| `cardholders` | one tokenized cardholder | `cardholder_id` | `enrolled_program_id` → `programs`, `country_code` → real `silver_country` |
| `card_tokens` | one card token | `token_id` | `cardholder_id` → `cardholders`, `bin_range` → real `silver_card_issuer` |
| `transactions` | one transaction attempt (**fact_transactions** grain) | `transaction_id` | `token_id` → `card_tokens`, `merchant_id` → real `silver_merchant`, `mcc_code` → real `silver_mcc`, `currency_code` → real `silver_iso_currency` |
| `transaction_events` | one lifecycle event (**fact_transaction_events** grain) | `event_id` | `transaction_id` → `transactions` |
| `settlements` | one settled transaction per batch (**fact_settlements** grain) | `settlement_id` | `transaction_id` → `transactions` (UNIQUE) |
| `reconciliation` | one reconciliation match/exception (**fact_reconciliation** grain) | `reconciliation_id` | `settlement_id` → `settlements` (UNIQUE) |
| `reward_events` | one reward evaluation (**fact_rewards** grain) | `reward_id` | `transaction_id` → `transactions`, `offer_id` → `offers` |

`fact_merchant_daily` (one row per merchant per day) is **not** a synthetic
OLTP table — it is not an operational/source-system concept, it's a Gold-layer
aggregate over `fact_transactions` (group by `merchant_id`, `date(transaction_timestamp)`).
The source data (`transactions.merchant_id` + `transactions.transaction_timestamp`)
already contains everything needed to derive it later; nothing further was
built for it in S9, per "prepare the domain for" rather than "build now."

## 6. Relationship Diagram (ASCII)

```
silver_legal_entity ──lei──┐
                            ▼
                        clients (ISSUER / NETWORK / PROGRAM_OWNER)
                            │ (PROGRAM_OWNER only)
                            ▼
                        programs ──currency_code──→ silver_iso_currency
                            │
                            ▼
                        campaigns
                            │
                            ▼
                        offers ──eligible_mcc_code──→ silver_mcc (nullable)
                            ▲
                            │ (program match + MCC scope + date window)
programs ──enrolled_program_id── cardholders ──country_code──→ silver_country
                            │
                            ▼
                        card_tokens ──bin_range──→ silver_card_issuer
                            │
                            ▼
silver_merchant ──merchant_id──→ transactions ──mcc_code──→ silver_mcc
                            │        │
                            │        ├──────────────→ transaction_events (1:N)
                            │        │
                            │        ├──(APPROVED only)──→ settlements (1:1)
                            │        │                          │
                            │        │                          ▼
                            │        │                    reconciliation (1:1)
                            │        │
                            │        └──(offer match only)──→ reward_events (0:1) ──offer_id──→ offers
```

## 7. Generation Rules, Counts, Distributions

All constants live in `src/synthetic/generators.py` — nothing here is implicit.

| Entity | Count | Rule |
|---|---|---|
| clients | 12 | 3 ISSUER + 3 NETWORK + 6 PROGRAM_OWNER; sampled from 10,000 real distinct GLEIF LEIs (sorted, then `rng.sample`) |
| programs | 8 | round-robin over PROGRAM_OWNER clients; currency from `[GBP,GBP,GBP,EUR,EUR,USD]` (weighted) |
| campaigns | 12 | 8 straddle `REFERENCE_DATE` (→ ACTIVE), 2 fully past (→ ENDED), 2 fully future (→ SCHEDULED) |
| offers | 20 | round-robin over campaigns; 60% get a real `eligible_mcc_code`, 40% are MCC-unrestricted; `offer_type` alternates CASHBACK_PCT/FIXED_AMOUNT |
| cardholders | 200 | round-robin over programs; 70% `country_code=GB`, 30% a random other real country |
| card_tokens | 200-400 | every cardholder gets 1 token, +30% chance of a 2nd; `bin_range` uniform-random over all 374,788 real BIN rows |
| transactions | 4,000 | uniform-random token, uniform-random real merchant (all 521 eligible); `amount` ~ `Uniform(2.00, 250.00)`; timestamp uniform over the 90 days ending `REFERENCE_DATE`; 88% target approval rate (actual realized: 86.6%) |
| transaction_events | derived | every transaction: 1 AUTHORIZATION; APPROVED: +1 CAPTURE (1-120 min later); 3% of eligible APPROVED transactions (only those ≥14 days before `REFERENCE_DATE`, so a reversal timestamp never exceeds it): +1 REVERSAL (1-13 days after capture) |
| settlements | = approved count | 1-2 days after the transaction date; `fee_amount` = fixed 1.5% of `settlement_amount` (not randomized — keeps the fee control total exact) |
| reconciliation | = settlement count | 97% MATCHED (variance 0), 3% EXCEPTION (a deterministic nonzero delta from a fixed small set) |
| reward_events | ≤ approved count | one row per APPROVED transaction that matches an in-scope, date-valid offer for its cardholder's program (first offer by `offer_id` wins ties) |

## 8. Random Seed / Reproducibility

`SEED = 20260921` (`src/synthetic/common.py`), threaded explicitly through a
single `random.Random(SEED)` instance passed into every generator function —
**never** the global `random` module. Every date is computed relative to a
fixed `REFERENCE_DATE = date(2026, 9, 21)`, never `date.today()`/`datetime.now()`,
so output is byte-identical across machines, days, and repeated runs given the
same seed and the same Silver snapshot. Verified in
`tests/test_synthetic_generation.py::test_generation_is_deterministic_given_same_seed`
and `test_pipeline_run_reproduces_identical_output_across_calls`.

## 9. Lifecycle Rules

```
AUTHORIZATION (always, 1 per transaction)
     │
     ├── DECLINED → lifecycle ends here (no CAPTURE, no settlement, no reward)
     │
     └── APPROVED → CAPTURE (1-120 min later)
                        │
                        ├── settlement (1-2 days later, full amount, 1.5% fixed fee)
                        │        └── reconciliation (0-2 days later; 97% matched, 3% exception)
                        │
                        └── (3% of eligible cases) REVERSAL (1-13 days after capture)
                                  — event-level only; does NOT reverse settlement/
                                    reconciliation amounts in this v1 (documented
                                    scope limit, see §17)
```

Reward evaluation happens independently for every APPROVED transaction against
its cardholder's enrolled program's offers (date-window + MCC-scope match).

## 10. Data-Quality Rules

Implemented in `src/synthetic/validation.py::validate_all()`, which the
pipeline calls unconditionally and which **raises** `SyntheticDataQualityError`
(a `RuntimeError`) on any violation — never just logs one:

- primary-key uniqueness (all 11 tables)
- 1:1 uniqueness for `settlements.transaction_id` and `reconciliation.settlement_id`
- foreign-key validity against real Silver dimensions (LEI, MCC, currency,
  country, merchant, BIN) and against sibling synthetic tables
- positive monetary values (`amount`, `offer_value`, `min_transaction_amount`,
  `settlement_amount`, `net_amount`)
- `source_type == "synthetic"` on every row of every table
- transaction-timestamp window bound
- lifecycle ordering (exactly one AUTHORIZATION per transaction; CAPTURE iff
  APPROVED; REVERSAL only after a CAPTURE; events chronologically ordered)
- settlement consistency (`settlement_amount == transaction.amount`,
  `net_amount == settlement_amount - fee_amount`, settlement not before its
  own transaction)
- reconciliation arithmetic (`variance == actual_amount - expected_amount`;
  MATCHED ⇒ variance 0; EXCEPTION ⇒ variance ≠ 0; not reconciled before its
  own settlement)
- reward qualification consistency (QUALIFIED ⇒ `reward_amount > 0`;
  NOT_QUALIFIED ⇒ `reward_amount == 0`; only references APPROVED transactions)

Negative fixtures proving each rule actually rejects bad data live in
`tests/test_synthetic_validation.py` (22 tests) — deliberately kept separate
from the primary generated dataset, which is clean by construction.

## 11. Financial Control Totals

| Control total | Formula | Real result |
|---|---|---|
| Settlement coverage | `count(settlements) == count(transactions WHERE status='APPROVED')` | 3,464 = 3,464 ✓ |
| Reconciliation coverage | `count(reconciliation) == count(settlements)` | 3,464 = 3,464 ✓ |
| Event composition | `count(AUTHORIZATION) == count(transactions)`; `count(CAPTURE) == count(approved)` | 4,000 = 4,000; 3,464 = 3,464 ✓ |
| **Gross amount identity** | `SUM(transactions.amount WHERE APPROVED) == SUM(settlements.settlement_amount) == SUM(reconciliation.expected_amount)` | **£435,106.16 = £435,106.16 = £435,106.16** ✓ |

No accounting rule beyond these was invented — reversals do not adjust any of
these totals in v1 (§17).

## 12. KPI Readiness Matrix

Every KPI the S9 brief lists, and the source table(s)/columns it will read
from once Gold exists (no KPI is computed now — design-only, per instruction):

| Domain | KPI | Source columns |
|---|---|---|
| Merchant intelligence | txn count/volume/avg value | `transactions.{amount,status}` |
| | approval/decline rate | `transactions.status` |
| | active merchants | distinct `transactions.merchant_id` |
| | volume by MCC/category | `transactions.mcc_code` → `silver_mcc` |
| | volume by country | `transactions.merchant_id` → `silver_merchant.country_code` (open, §16) |
| | daily trend | `transactions.transaction_timestamp` |
| Issuer reporting | txn count/volume, approved/declined | `transactions` × `card_tokens.bin_range` → `silver_card_issuer` (issuer proxy) |
| | settlement amount, fees | `settlements.{settlement_amount,fee_amount}` |
| Network reporting | auth/clearing/settlement counts | `transaction_events.event_type`, `settlements` |
| | currency distribution | `transactions.currency_code` |
| | lifecycle completion rate | `transaction_events` per `transaction_id` |
| Program owner | eligible/qualified txns, reward amount | `reward_events.{qualification_status,reward_amount}` |
| | redemption count/rate | `reward_events` count / `transactions` (APPROVED) count |
| | campaign performance | `reward_events` → `offers` → `campaigns` |
| Rewards/CLO | offer activation rate | `offers.status`/`valid_from`/`valid_to` vs evaluation date |
| | eligible/qualified txns, reward value, cost | `reward_events` |
| Reconciliation | matched/unmatched, expected/actual, variance | `reconciliation.*` |
| | exception count/aging | `reconciliation.match_status`, `reconciled_date - settlement_date` |
| Data-engineering ops | pipeline runs, records processed/rejected, DQ failures | not part of this phase's synthetic *business* data — belongs to the existing Silver `reconcile_counts`/quarantine machinery (S2-S8), already real, not synthetic |

## 13. Mapping: Synthetic Source → Future Silver → Future Gold

| Synthetic OLTP table | Future Silver (not built yet) | Future Gold |
|---|---|---|
| `clients` | `silver_client` (upsert, keyed by `client_id`) | `dim_client` (Type 1) |
| `programs` | `silver_program` | `dim_program` (Type 1) |
| `offers` | `silver_offer` | `dim_offer` (Type 1) |
| `campaigns` | `silver_campaign` | `dim_campaign` |
| `cardholders` | `silver_cardholder` | `dim_customer` |
| `card_tokens` | `silver_card_token` | `dim_card_token` |
| `transactions` | `silver_transaction` | `fact_transactions` |
| `transaction_events` | `silver_transaction_event` | `fact_transaction_events` |
| `settlements` | `silver_settlement` | `fact_settlements` |
| `reconciliation` | `silver_reconciliation` | `fact_reconciliation` |
| `reward_events` | `silver_reward_event` | `fact_rewards` |
| — | (existing) `silver_merchant` | `dim_merchant` (**SCD Type 2**) |
| — | (existing) `silver_mcc` | `dim_merchant_category` (Type 1) |
| — | (existing) `silver_country` | `dim_country` |
| — | (existing) `silver_iso_currency` | `dim_currency` |
| — | none | `dim_date`, `dim_time` (standard generated calendar dims, no source table) |
| — | none | `fact_merchant_daily` (aggregated from `fact_transactions`, §5) |

**SCD2 readiness for `dim_merchant`**: `silver_merchant.py` already carries
everything a future Gold SCD2 build needs — `record_hash` (content
fingerprint) and `source_updated_timestamp`/`bronze_run_id` (change/version
markers) — via its existing upsert design (S5-S8). No new history mechanism
was needed from S9: merchant history accrues naturally as Bronze OSM is
re-ingested over time and Silver's upsert detects real changes via
`record_hash`. `dim_client`/`dim_program`/`dim_offer` are Type 1 per the
approved architecture, so no equivalent history requirement applies to them.

## 14. Mapping to the Seven FinPay Business Problems

| # | Problem | Primary synthetic tables |
|---|---|---|
| 1 | Merchant intelligence | `transactions` + real `silver_merchant`/`silver_mcc` |
| 2 | Issuer reporting | `transactions` + `card_tokens` + real `silver_card_issuer` |
| 3 | Network reporting | `transaction_events`, `settlements` |
| 4 | Program-owner reporting | `programs`, `campaigns`, `reward_events` |
| 5 | Rewards / CLO | `offers`, `reward_events` |
| 6 | Settlement & reconciliation | `settlements`, `reconciliation` |
| 7 | Data-engineering operations | Silver's existing `reconcile_counts`/quarantine/lineage machinery (real, not synthetic) |

## 15. Explicitly Deferred Technologies

PostgreSQL implementation (schema documented only, §4), Bronze ingestion of
this synthetic data, dbt, Airflow, Kafka, Debezium, PySpark/Spark Structured
Streaming, Databricks, Snowflake, ClickHouse, Trino, Power BI/DAX, Gold table
implementation, cross-currency/FX-converted transactions, partial
capture/settlement, `POINTS_MULTIPLIER` offers, reversal-driven settlement
adjustment. All out of scope for S9 by explicit instruction.

## 16. Open Decisions

**Carried forward, untouched:** merchant `country_code`, `shop=vacant`
permanence, merchant/GLEIF upsert-conflict policy, ISO `minor_unit`, card
issuer `issuer_name`, GLEIF→country join, warehouse target, S6 address-schema
documentation discrepancy, `entity_creation_date` representation.

**New, S9-scoped decisions** (required for S9 to function; do not resolve any
Silver-layer item above):

1. **Synthetic per-merchant MCC assignment** (`src/synthetic/mcc_crosswalk.py`)
   is a low-confidence keyword crosswalk built for synthetic-transaction
   purposes only. It is the first draft of the `ref_osm_category_to_mcc` table
   docs/11 already anticipated, but is **not** an approved real crosswalk and
   does **not** touch `silver_merchant.mcc_code` (still NULL, still reserved).
   **Needs a decision**: should this stay synthetic-only, or become the seed
   of the real, approved crosswalk?
2. **`docs/11-data-model.md`'s fact-table naming/grain (`fact_transaction`,
   separate `fact_authorization`/`fact_clearing`/`fact_settlement`,
   `fact_offer_interaction`) conflicts with this S9 phase's explicit fact list**
   (`fact_transactions`, combined `fact_transaction_events`,
   `fact_settlements`, `fact_reconciliation`, `fact_rewards`,
   `fact_merchant_daily`). S9 was built to this phase's explicit, current
   instruction (marked "must not be silently changed"). **docs/11 was not
   edited** — it should be reconciled with this document in a future
   documentation pass, not silently overwritten.
3. **Transaction currency has no cross-border/FX conversion** — every
   transaction is denominated in its cardholder's program currency; no use of
   `silver_fx_rate` beyond confirming the 3-currency universe (GBP/EUR/USD) is
   real. A genuine FX-conversion model would need historical daily rates this
   project's real Bronze FX data (a single date) doesn't yet have.
4. **Reversals don't adjust settlement/reconciliation totals** in this v1 — a
   deliberate scope limit, not a silent gap (§9, §15).
