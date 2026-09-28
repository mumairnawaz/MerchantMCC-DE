# 25 — CDC Bronze Design (Phase S12)

Status: **IMPLEMENTED**. Scope boundary is exact: `Kafka -> CDC Bronze`. This
phase does not build Silver CDC, Gold, OLAP, data marts, dbt, Airflow, Spark,
or Power BI.

```
S11: PostgreSQL → Debezium → Kafka                     [already done]
S12: Kafka → CDC Bronze (THIS PHASE)                    data/bronze_cdc/
S13: CDC Bronze → CDC Silver                            [future, not started]
```

## 1. Architecture

```
Kafka topics (finpay.finpay.<table>, 11 total)
        │
        ▼
 src/cdc/consumer.py :: run()
        │
        ├─ poll (confluent_kafka.Consumer, manual offset commit)
        ├─ parse envelope (src/cdc/envelope.py) ── validate / reject
        ├─ checkpoint dedup (src/cdc/checkpoint.py) ── topic+partition+offset
        ├─ buffer per (table, kafka_partition)
        ├─ flush: write Parquet (PyArrow, src/cdc/bronze_schema.py)
        ├─ write checkpoint (only after a confirmed write)
        ├─ commit Kafka offsets (only after checkpoint write)
        └─ reconcile (src.silver.common.reconcile_counts, reused)
        │
        ▼
data/bronze_cdc/<table>/partition=<n>/run_<timestamp>Z.parquet
data/quarantine_cdc/<table>/run_<timestamp>/rejected.jsonl   (only if malformed events occur — none did in real verification)
data/bronze_cdc/_checkpoints/<topic>/partition_<n>.json
```

## 2. Kafka → CDC Bronze Flow

`run()` is a **finite batch consumer** — it drains what's currently available
and returns a report, matching every other `src/ingestion/*.py::run()` in
this project (not a perpetual streaming daemon). A future scheduler (S13+,
out of scope here) would invoke it repeatedly.

## 3. Consumer Design

`src/cdc/consumer.py`. The per-message decision logic (`process_message`) is
deliberately separated from Kafka I/O — it operates on a `RawMessage`
(topic/partition/offset/key/value), not `confluent_kafka.Message` directly,
so it's fully unit-testable without a live broker
(`tests/test_cdc_consumer_unit.py`, 13 tests).

**Dependency decision, reported as required** (this phase's own instruction
"stop and report before adding a Kafka client"): no Kafka client existed in
this project before S12 (S11 deliberately avoided one — verification used
Kafka Connect's REST API plus `docker exec` + Kafka's own CLI tools). S12's
actual objective — a persistent, checkpointed, offset-controlled consumer
application — cannot be built that way. Chose **`confluent-kafka`** (the
librdkafka-based, industry-standard, actively maintained client, with
prebuilt Windows wheels — installed cleanly, no compiler needed) over
`kafka-python` (less actively maintained) or `aiokafka` (unneeded async
complexity for a batch consumer).

## 4. Debezium Event Structure (re-verified for S12, not assumed from S11's docs)

Confirmed against real messages again in this phase — the key is itself a
schema+payload envelope:
```json
{"schema": {...}, "payload": {"transaction_id": "TXN-0000001"}}
```
Value envelope matches docs/24 §13 exactly (`before`/`after`/`source`/`op`/
`ts_ms`/`transaction`), independently re-confirmed here.

## 5. Supported Operations

`r` (snapshot), `c` (insert), `u` (update), `d` (delete) — the exact four
codes demonstrated in S11, nothing invented (`src/cdc/envelope.py::VALID_OPS`).
An unrecognized code is quarantined, never silently accepted or dropped.

## 6. Tombstone Handling

A Kafka tombstone (`value = null`) is parsed into `ParsedEvent(is_tombstone=True,
operation="t", ...)`. **`"t"` is not a real Debezium code** — it's assigned by
this consumer solely so a tombstone has a value in the same `operation`
Bronze column as real events, and is explicitly documented as synthetic
(`src/cdc/envelope.py`). A tombstone is **never** confused with a business
`"d"` delete: a `"d"` event has a populated (non-null) value with `before`
set; a tombstone has no value at all. Both are preserved as **separate**
Bronze rows — verified with a real delete+tombstone pair
(`tests/test_cdc_bronze_integration.py::test_delete_and_tombstone_are_distinct_bronze_records`).
For reconciliation purposes, tombstones are counted in their own
`tombstones_handled` bucket, distinct from `events_persisted`, even though
physically both are appended to the same Bronze file.

## 7. Bronze Schema

`src/cdc/bronze_schema.py` — every field from this phase's own list, no more:
`source_topic`, `kafka_partition`, `kafka_offset`, `event_key`, `operation`,
`before`, `after`, `source_connector`, `source_name`, `source_schema`,
`source_table`, `source_database`, `source_lsn`, `source_timestamp_ms`,
`snapshot_indicator`, `transaction_id`, `transaction_total_order`,
`transaction_data_collection_order`, `cdc_received_at_utc`.

**`transaction_id`/`transaction_total_order`/`transaction_data_collection_order`
are always NULL in this deployment** — verified, not assumed: Debezium's
`provide.transaction.metadata` was never enabled on the connector (S11), so
`payload.transaction` is always `null` in every real message inspected.
Stored as NULL per this phase's own instruction ("if a field is unavailable,
store NULL rather than inventing it") rather than silently omitted.

`before`/`after`/`event_key` are raw JSON **text**, never flattened into
typed analytical columns (§8 of this phase — Bronze CDC is a raw/audit layer;
business typing belongs to the future Silver CDC phase). Verified:
`tests/test_cdc_envelope.py::test_raw_payload_preserved_verbatim_not_flattened`.

## 8. Storage Layout

```
data/bronze_cdc/<table>/partition=<kafka_partition>/run_<timestamp>Z.parquet
```

Partitioned by **Kafka partition** — structural CDC metadata, not a business
field (this phase explicitly forbids business-field partitioning). All 11
topics currently have exactly one partition (S11's `topic.creation.default.
partitions=1`), so today this is always `partition=0`, but the mechanism is
generic and correct regardless. Each consumer run that has new data for a
table writes a **new**, uniquely-timestamped file (`run_<timestamp>Z.parquet`,
matching this project's existing Bronze `run_<timestamp>` convention from
`src/ingestion/common.py`) — an existing file is never reopened or rewritten,
so immutability holds at the filesystem level, not just as a logical rule.

## 9. Idempotency

**Two layers, both required** (see `src/cdc/checkpoint.py`'s module
docstring for the full reasoning): Kafka consumer-group offset commits
(manual, `enable.auto.commit=False`) minimize the redelivery window, but
cannot alone prevent a duplicate write if the consumer crashes *after*
writing Bronze but *before* committing the offset. A per-`(topic, partition)`
**checkpoint file** (`data/bronze_cdc/_checkpoints/<topic>/partition_<n>.json`,
mirroring `src/ingestion/watermark.py`'s existing design) records the highest
offset already durably persisted; any redelivered message with
`offset <= checkpoint` is skipped as a duplicate — correct because Kafka
guarantees strictly increasing per-partition offsets. **Identity is
`topic + partition + offset`**, never `transaction_id` (one business
transaction produces multiple CDC events, per this phase's explicit warning)
— verified: `tests/test_cdc_consumer_unit.py::test_idempotency_key_is_topic_partition_offset_not_transaction_id`.

Proven two ways: (1) reprocessing with the **same** consumer group found zero
new messages (Kafka's own commit worked); (2) reprocessing with a **brand
new** consumer group (forcing full Kafka redelivery of already-processed
messages) still produced zero new Bronze writes — the checkpoint alone caught
every duplicate. Both scenarios independently verified live (see completion
report).

## 10. Offset/Checkpoint Strategy — Exact Order

```
poll → parse/validate → dedup-check (checkpoint) → buffer
  → [end of batch] → write Bronze Parquet → write checkpoint → commit Kafka offset
```

If the Bronze write for a table fails, that table's checkpoint is **not**
updated and its Kafka offsets are **not** committed — the next `run()` call
will legitimately reprocess it. This is per-table isolation, not an
all-or-nothing transaction across all 11 topics: unlike `src/oltp/loader.py`
(where OLTP tables have real FK relationships to each other, requiring
atomicity), CDC Bronze topics are independent streams with no cross-topic
dependency at this layer — a documented, deliberate design difference from
S10's loader, not an oversight.

## 11. Failure Handling

| Case | Handling |
|---|---|
| Invalid JSON | quarantined (`envelope_validation`) |
| Missing Debezium envelope (`payload` absent) | quarantined |
| Unknown operation code | quarantined |
| Null tombstone | explicitly recognized (§6), never quarantined, never confused with `"d"` |
| Duplicate Kafka offset | skipped via checkpoint, no duplicate Bronze record |
| Storage failure | checkpoint/offset commit both withheld for that table |
| Kafka broker restart | consumer reconnects (confluent-kafka's own retry) |
| Consumer process restart | resumes from committed offset + checkpoint; verified with zero loss/duplication (completion report) |

Reused `src.silver.quarantine` (already dataset-agnostic, not Silver-specific
logic) rather than building a second quarantine mechanism, per this phase's
explicit instruction.

## 12. Reconciliation

`events_read = events_persisted + duplicates_skipped + quarantined + tombstones_handled`,
enforced per table per run via `src.silver.common.reconcile_counts` (reused,
not reimplemented — the same generic helper Silver's own reconciliation uses).
Raises `RuntimeError` on any mismatch. Verified PASS/under-count/over-count
behavior in `tests/test_cdc_reconciliation.py`.

## 13. Lineage

Every Bronze CDC record answers "where from" (`source_topic`,
`kafka_partition`, `kafka_offset`, `source_connector/name/schema/table/
database`, `source_lsn`) and "when/how ingested" (`source_timestamp_ms` =
Debezium's own event time, `cdc_received_at_utc` = this consumer's own
ingestion time — analogous to existing Bronze's `ingestion_timestamp_utc`
convention).

## 14. Replay Strategy

Reprocessing an already-consumed message (same or fresh consumer group) is a
verified no-op: record count before == record count after (never `N + N`).
See `tests/test_cdc_bronze_integration.py::test_replay_does_not_duplicate_bronze_events`.

## 15. Restart Behavior

Verified live: insert a uniquely-IDed test transaction, run the consumer
until its Bronze record set is observed and stable, then run a second, fresh
`run()` call (a full process "restart" — a brand new `Consumer` object,
same group/checkpoint) — the persisted offset set is byte-identical
before/after, with no duplicate offsets introduced.

## 16. Real Verification Results

All performed against the real local stack, no external API calls:

- **Snapshot**: `clients` Bronze holds ≥12 `op="r"` records, live-matching
  PostgreSQL's current row count (recomputed each time, never hardcoded).
- **Control total**: Bronze's own stored snapshot payloads, summed from the
  raw `after` JSON, reproduce the known £435,106.16 gross-approved-amount
  identity — cross-checked against a fresh live query, not a literal.
- **INSERT/UPDATE/UPDATE/DELETE**: a real test transaction (`c`→`u`→`u`→`d`)
  fully captured in Bronze, in-order, offsets strictly increasing — matching
  this phase's own worked example exactly.
- **DELETE + tombstone**: both present as two distinct Bronze rows.
- **Replay**: proven no-op.
- **Restart**: proven no loss/duplication.
- **Full real drain**: all 11 topics, ~21,831 real accumulated Bronze CDC
  records as of this report (see completion report for the per-table
  breakdown) — zero quarantined events (no malformed messages ever
  encountered in the real stack; the quarantine path itself is separately
  unit- and integration-tested with fabricated malformed input).

## 17. A Real Defect Found and Fixed: `REPLICA IDENTITY`

**Found during S12's own DELETE verification, not assumed away**: PostgreSQL's
default `REPLICA IDENTITY DEFAULT` only includes **primary-key** columns in
the WAL "before" image for `UPDATE`/`DELETE` — every non-PK column
(`amount`, `status`, ...) arrived in Debezium's `before` payload as an
incorrect placeholder value rather than the true prior value. This directly
undermines CDC Bronze's stated purpose ("preserve the original Debezium
information necessary for downstream reconstruction and auditing").

**Fix**: `ALTER TABLE finpay.<table> REPLICA IDENTITY FULL;` applied to all
11 tables — a standard, minimal, well-known PostgreSQL/Debezium configuration
step for exactly this scenario. Non-destructive: no column, constraint, or
data change; purely a WAL/logical-replication behavior setting. Verified:
`SELECT relreplident` now `'f'` (full) on all 11 tables; OLTP row counts and
the £435,106.16 control total independently re-verified unchanged
immediately after. A fresh DELETE test after the fix correctly shows the true
prior `amount` in `before`.

This is reported as a Debezium/PostgreSQL configuration fix, not an OLTP
schema (columns/constraints/data) change, and falls within this phase's own
allowance: "S11 Debezium configuration untouched **unless S12 requires a
verified defect fix**."

## 18. Known Limitations

- If a Bronze write succeeds but the *quarantine* write for the same run
  subsequently fails to commit its offsets (a table with both persisted and
  quarantined records, where the persist step itself failed), a retry could
  re-quarantine the same malformed messages under a new run-id — redundant
  audit records, not silent data loss, not observed in real verification.
- Single-partition topics (inherited from S11) mean this consumer never
  exercises true multi-partition ordering/rebalancing — the partition-based
  storage design is correct and ready for it, but untested at that scale.
- No dead-letter replay tooling for quarantined CDC events yet (future work).
- `provide.transaction.metadata` was never enabled on the Debezium connector,
  so `transaction_id`/`transaction_total_order`/`transaction_data_collection_order`
  are always NULL in this deployment (§7) — enabling it is a future,
  independent decision, not made here.

## 19. Future S13 Boundary

S13 = **CDC Bronze → CDC Silver**: parse the raw `before`/`after` JSON text
into typed, validated, business columns (the transformation explicitly
**not** done here), apply per-table upsert/append semantics analogous to the
existing Silver layer's `merchant`/`legal_entity`/`fx_rate` patterns, and
reconcile against CDC Bronze's own counts. Not started in S12.
