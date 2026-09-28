# 26 — CDC Bronze → CDC Silver (Phase S13)

Status: **IMPLEMENTED**. Scope boundary is exact:

```
S12: Kafka → CDC Bronze                                  [already done]
S13: CDC Bronze → CDC Silver (THIS PHASE)                 data/silver_cdc/
S14: CDC Silver → Gold/OLAP                                [future, not started]
```

## 1. Architecture

```
data/bronze_cdc/<table>/  (S12, immutable, permanent history)
        │
        ▼
 src/cdc/silver.py :: process_table()
        │
        ├─ read every Bronze record, sorted by (kafka_partition, kafka_offset)
        ├─ ingestion dedup (checkpoint: topic+partition+offset, reused from S12)
        ├─ tombstone routing (never validated, never quarantined)
        ├─ typed extraction (src/cdc/silver_types.py)
        ├─ validation (src/cdc/silver.py::_validate_typed_record)
        ├─ current-state state machine (source.lsn authoritative) OR
        │  event-log append (PK-reuse-after-delete aware)
        ├─ write Parquet, write checkpoint, write deleted-key memory
        └─ reconcile (src.silver.common.reconcile_counts, reused)
        │
        ▼
data/silver_cdc/silver_cdc_<entity>/data.parquet          (9 current-state tables)
data/silver_cdc/silver_cdc_<entity>/run_<ts>Z.parquet      (2 event-log tables)
data/quarantine_cdc_silver/<dataset>/run_<ts>/rejected.jsonl
```

## 2. Real CDC Bronze Structures Inspected (not assumed)

Re-verified directly against `data/bronze_cdc/` in this phase (not carried over
from memory of S12's docs), across `clients`, `transactions`, `settlements`,
`reconciliation`, `programs`, `campaigns`, `offers`, `cardholders`,
`card_tokens`, `transaction_events`, `reward_events`:

- **NUMERIC** columns → decimal **strings** in `after`/`before` (e.g.
  `"amount": "173.53"`) — confirms S11's `decimal.handling.mode=string`.
- **DATE** columns → **int32 days since epoch** (e.g. `"onboarding_date": 19955`
  = 2024-08-20) — Debezium's `io.debezium.time.Date` logical type.
- **TIMESTAMP** columns → **int64 microseconds since epoch** (e.g.
  `"transaction_timestamp": 1783955166000000` = 2026-07-13 15:06:06 UTC).
- Every other column: plain JSON string/null.
- Primary keys: single-column for all 11 tables, exactly matching
  `src/oltp/schema.py`'s declared `PRIMARY KEY` per table.

## 3. Silver Datasets Implemented — and the Decision Behind Them

**Design note, stated up front**: unlike the original API-sourced Silver
modules (`src/silver/mcc.py` etc., each hand-written because each source has
a genuinely different raw shape), all 11 CDC tables share exactly ONE raw
shape (the Debezium envelope) and differ only in their column list/types —
fully known from `src/oltp/schema.py`'s DDL. `src/cdc/silver_tables.py`
defines this once, as data; `src/cdc/silver.py` is a single engine that
processes every table uniformly from it. A deliberate, different-but-
justified adaptation to a genuinely more uniform problem, not a departure
from "reuse existing patterns" for its own sake.

| # | Silver dataset | OLTP table | Mode |
|---|---|---|---|
| 1 | `silver_cdc_client` | clients | current_state |
| 2 | `silver_cdc_program` | programs | current_state |
| 3 | `silver_cdc_campaign` | campaigns | current_state |
| 4 | `silver_cdc_offer` | offers | current_state |
| 5 | `silver_cdc_cardholder` | cardholders | current_state |
| 6 | `silver_cdc_card_token` | card_tokens | current_state |
| 7 | `silver_cdc_transaction` | transactions | current_state |
| 8 | `silver_cdc_settlement` | settlements | current_state |
| 9 | `silver_cdc_reconciliation` | reconciliation | current_state |
| 10 | `silver_cdc_transaction_event` | transaction_events | event_log |
| 11 | `silver_cdc_reward_event` | reward_events | event_log |

**Current-state vs event-history decision (§6, documented)**: 9 tables are
genuine "current state" business entities — upserted, keyed by PK, one row
per business key, latest-by-`source.lsn` wins. 2 tables
(`transaction_events`, `reward_events`) are inherently append-only event logs
already in OLTP itself (one immutable row per business event) — modeled as
pure append, never collapsed to "latest."

**A separate *typed* "transaction history" dataset was explicitly deferred**
(not silently dropped): CDC Bronze (S12) already permanently retains the
complete raw event history for every table, including `transactions` — every
`c`/`u`/`d` event remains queryable there forever. Nothing is lost by not
duplicating it into a second typed Silver shape now; a well-scoped future
addition, not a gap.

## 4. Typed Transformation Rules

`src/cdc/silver_types.py` — explicit PyArrow types throughout
(`src/cdc/silver_schema.py`), never inferred for financial fields:

| Raw Debezium encoding | Silver type |
|---|---|
| decimal string (`"42.00"`) | `pa.decimal128(18, 4)` via `Decimal(str(value))` — never float |
| epoch-day int | `pa.date32()` |
| epoch-microsecond int | `pa.timestamp("us")` |
| plain string | `pa.string()` |

## 5. Financial Data Rules

Every monetary column (`amount`, `settlement_amount`, `fee_amount`,
`net_amount`, `expected_amount`, `actual_amount`, `variance`,
`offer_value`, `min_transaction_amount`, `reward_amount`) stays
`Decimal`/`decimal128` end to end — never floating point. Positive-value
constraints (`amount`, `settlement_amount`, `net_amount`, `offer_value`,
`min_transaction_amount` > 0) enforced during validation, matching the same
business rules already enforced in `src/oltp/schema.py`'s `CHECK` constraints
— not invented here, just re-verified at the Silver layer.

## 6. Primary/Business Keys

Every PK taken directly from `src/oltp/schema.py`'s declared `PRIMARY KEY` —
all 11 are single-column in this schema (no composite keys exist in the real
OLTP design), verified against real CDC records. PK uniqueness verified
after transformation for every current-state table (exact match), and for
event-log tables via the `(PK, kafka_offset)` invariant (§9) — a business PK
*may* legitimately repeat across separate lifecycle instances (see §9), but
no single Kafka message is ever written to Silver twice.

## 7. Ordering / Freshness / Conflict Handling

**`source.lsn`** (PostgreSQL's own WAL log sequence number) is authoritative
for business freshness — a single, monotonically increasing position,
comparable across every table in this database, not just within one.
`kafka_offset` is used only for ingestion-level dedup (§9, inherited from
S12) — never for deciding which business version wins. `source.ts_ms` is
retained for lineage/display only. This extends (not contradicts) the
existing Silver convention: `src/silver/merchant.py`/`legal_entity.py` used a
business timestamp for exactly this purpose because that's what was
available there; CDC Bronze offers a strictly more precise, authoritative
position, so it's used instead — a documented, justified refinement, not an
invented rule.

## 8. Operation Semantics

| Op | Handling |
|---|---|
| `r` (snapshot) | treated as an insert/upsert — `after` is the current state |
| `c` (insert) | `after` is the current state |
| `u` (update) | `after` is the new current state; `source.lsn` decides if it actually wins |
| `d` (delete) | `before` identifies the row; removed from current-state output; **never** converted into an update |
| `t` (tombstone) | never business data — routed to its own counter, never validated, never quarantined (structural guarantee: the tombstone branch in `process_table` returns before any JSON parsing) |

## 9. Out-of-Order / Stale / Delete Handling — the State Machine

`src/cdc/silver.py::_apply_current_state_event()` (12 dedicated unit tests,
`tests/test_cdc_silver_state_machine.py`):

- Newer `lsn`, different content → **update**.
- Newer `lsn`, same content (hash match) → **unchanged**, row left completely
  untouched (matching `src/silver/merchant.py`'s exact convention).
- Older `lsn` (arriving late, regardless of delivery order) → **stale_skipped**,
  never overwrites newer state — proven with the exact `INSERT → UPDATE(newer)
  → UPDATE(older)` scenario this phase specifies.
- Delete with newer `lsn` than anything seen → **deleted**, removed from
  current-state output; a **persisted, cross-run "deleted-key" memory**
  (`data/silver_cdc/_deleted_keys/<dataset>.json`) remembers this so a
  *later* stale event can never resurrect it, even across separate `run()`
  invocations (not just within one).
- Delete with an OLDER `lsn` than current state → stale, ignored (current
  state preserved).
- A fresh `c` for a previously-deleted key, with a newer `lsn` → legitimate
  re-creation, accepted as a new insert (PK reuse is real, observed OLTP
  behavior — not resurrection of stale data).
- An `u` arriving against an already-deleted key with a *newer* `lsn` than
  the delete → a genuine anomaly (should not occur in real Postgres CDC) —
  **quarantined for investigation, never silently resurrected**.
- Same `lsn`, different content → **quarantined** as an unresolved conflict
  (mirrors `src/silver/merchant.py`'s equal-timestamp-conflict handling) —
  no invented "latest run wins."

Event-log tables (§13) use an analogous but simpler PK-active/deleted
tracking: a `c`/`r` for a PK currently marked active is a genuine duplicate
(quarantined); a `d` is always appended (never dropped — delete lineage must
remain traceable even for an event log) and marks the PK reusable; a later
`c` for the same, now-inactive PK is accepted as a legitimate new lifecycle
instance. This was **found to matter on real data**: this project's own S10
idempotency-test fixture (`tests/test_oltp.py`) repeatedly inserts and
deletes the same hardcoded `event_id`/`reward_id` across many separate test
sessions — genuine, observed CDC history, not synthetic noise.

## 10. Idempotency

Reuses `src.cdc.checkpoint`'s exact `read_checkpoint`/`write_checkpoint`
functions (the same module S12 uses for Kafka→Bronze), pointed at a
*different* root (`data/silver_cdc/_checkpoints/`) so Bronze's "Kafka
position consumed" checkpoint and Silver's "Bronze position consumed"
checkpoint never collide — same mechanism, different stage, not a new
framework. Identity is `topic + partition + offset`, **never**
`silver_processed_at_utc` (explicitly forbidden by this phase) and never
`transaction_id` alone. Verified live: rerunning all 11 tables against
unchanged Bronze data produced zero new writes, 100% `duplicates_skipped`.

## 11. Validation

Reuses `src/silver/validation.py`'s conventions (required/positive/enum
checks), applied via `src/cdc/silver_tables.py`'s per-column metadata. **A
real defect found and fixed in this phase**: validating a DELETE's `before`
image against the *full* business-column rule set (required fields, positive
amounts, valid enums) is wrong — a delete's job is only to identify *which*
row to remove, so its non-PK fields carry no semantic meaning. This was
discovered via real data: some historical delete events (from before S12's
`REPLICA IDENTITY FULL` fix, docs/25 §17) have every non-PK column
blank/zero, and were being incorrectly quarantined outright, which in turn
left two real, already-deleted transactions incorrectly visible in
`silver_cdc_transaction`'s current state (breaking the control-total
identity — see §14). Fixed: delete-mode validation (`is_delete=True`)
checks *only* PK presence.

## 12. Quarantine

Reuses `src/silver/quarantine.py` unchanged. Every rejected record retains:
source table, operation, Kafka topic/partition/offset, `source.lsn`, the
original raw `before`/`after` payload, and the specific failing check name
(`missing_pk_*`, `required_field_*`, `positive_*`, `invalid_value_*`,
`duplicate_primary_key`, `conflict_same_lsn`, `conflict_update_after_delete`,
`missing_payload_for_operation`). Tombstones structurally cannot reach
quarantine (§8) — verified both by a live test and by direct source
inspection.

## 13. Lineage

Fields kept, and why (no field without a reason): `source_name` (constant
`"cdc"` — keeps this pipeline distinguishable from API Silver's own
`source_name` values, §19), `kafka_topic`/`kafka_partition`/`kafka_offset`
(exact Kafka identity of the winning event), `source_lsn` (the ordering
authority, §7), `operation` (which Debezium op produced this row's current
content), `source_timestamp_ms` (Debezium's own event time, exact copy),
`cdc_received_at_utc` (when S12 first wrote it to Bronze — copied, not
regenerated), `silver_processed_at_utc` (when this transform ran —
**explicitly not** an idempotency key), `record_hash` (current-state tables
only, change detection).

## 14. Reconciliation & Control Totals

Formula (identical shape for every dataset, reusing
`src.silver.common.reconcile_counts`, never a second framework):

```
events_read = accepted + deleted + stale_skipped + duplicates_skipped + quarantined + tombstones_handled
```

`accepted` covers inserted+updated+unchanged (current_state) or persisted-
non-delete (event_log); `deleted` covers a current-state removal or an
event-log delete-row append. Verified PASS/under-count/over-count behavior
(`tests/test_cdc_silver_reconciliation.py`); held exactly, with zero
unexplained gap, across all 11 real tables.

**Control totals — recomputed dynamically, never hardcoded**:
```
approved transaction total (Silver) == settlement total (Silver) == reconciliation expected total (Silver)
                                     ==
                        the same three totals, queried live from PostgreSQL
```
All confirmed exactly equal in this phase's real verification (see
completion report).

## 15. CDC Silver vs Existing API Silver

Kept fully distinct, per this phase's explicit instruction: `src/cdc/silver.py`
never imports or writes to anything under `src/silver/` datasets or
`data/silver/`; it only *reuses* `src/silver/common.py`,`validation.py`,
`quarantine.py` as generic, dataset-agnostic frameworks (exactly as S7-S12
already established the precedent for). `source_name="cdc"` vs the API
path's various real source names keeps the two lineages visibly separate for
whatever future Gold layer eventually reconciles them.

## 16. Real Local Verification (§21, live, no external API calls)

A real, controlled `INSERT → UPDATE → UPDATE → DELETE` sequence was run
against PostgreSQL and traced through the complete real local stack:
PostgreSQL → WAL → Debezium → Kafka → S12's CDC Bronze consumer → S13's CDC
Silver engine — confirmed at every stage (see completion report for the
literal captured output). A separate `INSERT → DELETE` sequence confirmed
removal from current-state while Bronze's permanent history (the `c`+`d`
pair) remained fully intact and queryable — proving delete lineage is
traceable without destroying history.

## 17. Operational Visibility (no new GUI built, per this phase's instruction)

| System | How to inspect |
|---|---|
| PostgreSQL | `docker exec merchantmcc_postgres psql -U merchantmcc -d merchantmcc` |
| Kafka | `docker exec merchantmcc_kafka /kafka/bin/kafka-topics.sh --list` / `kafka-console-consumer.sh` |
| Debezium/Kafka Connect | `GET http://localhost:8083/connectors/finpay-postgres-cdc-connector/status` |
| CDC Bronze | `pyarrow.parquet` / any Parquet-aware tool against `data/bronze_cdc/` |
| CDC Silver | same, against `data/silver_cdc/` — plain Parquet files, readable from Python, DuckDB, pandas, etc. |

## 18. Known Limitations / Open Decisions

- A typed, dedicated "transaction history" Silver dataset (distinct from the
  current-state `silver_cdc_transaction`) remains deferred (§3) — CDC
  Bronze already serves this need in raw form.
- The historical pre-`REPLICA IDENTITY FULL` delete events (docs/25 §17)
  are now correctly processed by the PK-only delete validation fix (§11),
  but this is a reminder that any *future* similar gap in Postgres's
  replica-identity configuration would reintroduce sparse before-images —
  worth monitoring, not re-fixed defensively beyond what real data required.
- No composite-key tables exist in the current OLTP schema, so the
  composite-key preservation instruction (§9 of this phase) has no real
  case to exercise yet — the PK handling is written generically
  (`pk_fields: tuple[str, ...]`) and would already support one if added.
- All Silver-layer, S9-layer, S10-layer, S11-layer, and S12-layer open
  decisions carried forward untouched.

## 19. Next Phase Boundary

S14 = CDC Silver → Gold/OLAP. Not started here.
