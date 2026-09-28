# 24 — PostgreSQL → Debezium → Kafka CDC (Phase S11)

Status: **IMPLEMENTED**. Scope boundary is exact: this phase ends at Kafka
topics containing real CDC events. It does **not** cover a Kafka consumer,
CDC Bronze, CDC Silver, OLAP, Gold, or any orchestration — those are later,
explicitly separate phases (§18).

## 1. Why CDC Exists in FinPay

S9/S10 built a real PostgreSQL OLTP database, but a batch export/reload of
that database (like S10's own `load_all()`) is not how a production
transaction-processing pipeline stays current — it would mean periodically
re-reading the whole table, which is slow, misses the exact moment of change,
and cannot distinguish an update from a delete after the fact. Change Data
Capture solves this by reading PostgreSQL's own write-ahead log (WAL) — the
durability mechanism Postgres already uses internally for crash recovery — so
every insert/update/delete becomes an ordered event stream, in real time,
without querying the tables at all.

## 2. Why PostgreSQL Is the OLTP Source

Established in S10 (docs/23): PostgreSQL is FinPay's operational system of
record for the synthetic business domain (clients, programs, transactions,
...). CDC's job in this architecture is to expose *that* system's changes,
not to replace it.

## 3. Why Debezium

Debezium is the de facto standard open-source CDC platform for exactly this
job: it wraps PostgreSQL's native logical replication protocol, runs inside
ordinary Kafka Connect, and produces a well-defined, schema-carrying JSON
event for every change — no custom WAL-parsing code, no polling. This
phase's instructions explicitly required Debezium over a hand-rolled
Python poller, for the same reason: reinventing WAL decoding is real
engineering risk for zero benefit when a maintained, widely-deployed
connector already does it correctly.

## 4. Why Kafka

Kafka is the durable, ordered, replayable transport between an OLTP source
and every downstream consumer this architecture will eventually have (future
CDC Bronze, potentially others). Debezium's Kafka Connect deployment model
means "produce to Kafka" is not a separate integration — it *is* how
Debezium works.

## 5. PostgreSQL Logical Replication

**Found before any change was made** (§3 of this phase's own instructions):
`wal_level = replica` — logical replication was **not** already enabled.
This is a real, necessary configuration change, not something invented for
its own sake. `wal_level` cannot be changed with `SET` at runtime; it
requires a server restart. Changed via `docker/docker-compose.yml`'s
`postgres` service:

```yaml
command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10"]
```

`max_replication_slots`/`max_wal_senders` were already at PostgreSQL's
default of 10 each (one connector needs one of each) — listed explicitly
anyway so the CDC requirement is visible directly in the compose file, not
just implied by an unlisted default.

**Applying this required a container restart** (`docker compose up -d
postgres`). The named volume `merchantmcc_postgres_data` was untouched — data
integrity was independently re-verified immediately after the restart via
`src.oltp.validation.verify_all()`, which reported all 11 tables' exact row
counts and the £435,106.16 control-total identity unchanged. No table was
truncated or recreated.

## 6. WAL (Write-Ahead Log)

PostgreSQL writes every change to the WAL before applying it to the actual
table data, for crash recovery. `wal_level = logical` adds enough extra
information to that log (which rows changed, old + new values) for a
*logical decoding plugin* to reconstruct row-level change events from it —
this is the mechanism Debezium reads, not a query against the tables.

## 7. Publication

A **dedicated** publication, `finpay_publication`, created via
`src/cdc/publication.py::ensure_publication()`:

```sql
CREATE PUBLICATION finpay_publication FOR TABLE
    finpay.clients, finpay.programs, finpay.campaigns, finpay.offers,
    finpay.cardholders, finpay.card_tokens, finpay.transactions,
    finpay.transaction_events, finpay.settlements, finpay.reconciliation,
    finpay.reward_events;
```

Explicit table list, **not** `FOR ALL TABLES` — no unrelated database/schema
is exposed to CDC. `ensure_publication()` is idempotent: if the publication
already exists it verifies the table set is still exactly these 11 and
leaves it untouched; it never drops/recreates a publication that already
matches, and raises loudly (rather than silently fixing it) if something
external has changed its scope.

## 8. Replication Slot

**Not created by this project's own code.** Debezium's PostgreSQL connector
creates the replication slot itself, named `finpay_slot` (set via the
connector's `slot.name` config), the first time it starts. `src/cdc/
publication.py::replication_slot_info()` only *reads* `pg_replication_slots`
— its emptiness before the connector starts, and non-emptiness afterward, is
real evidence the connector actually began streaming, not an assumption.

## 9. Debezium Connector

`src/cdc/connector.py::build_connector_config()` — registered via Kafka
Connect's plain-HTTP REST API (`POST /connectors`) using `requests` (already
an approved dependency; no new Kafka client library):

| Setting | Value | Why |
|---|---|---|
| `connector.class` | `io.debezium.connector.postgresql.PostgresConnector` | — |
| `database.hostname`/`port`/`user`/`password`/`dbname` | from `.env` via `OltpSettings` | never hardcoded — same mechanism as `src/oltp/config.py` |
| `topic.prefix` | `finpay` | see §10 |
| `schema.include.list` | `finpay` | only the FinPay schema |
| `table.include.list` | all 11 `finpay.*` tables, explicit | never wildcarded |
| `plugin.name` | `pgoutput` | PostgreSQL's **built-in** logical decoding plugin (available natively since PG10) — no extra extension needed in the `postgres:16-alpine` image, avoiding an image rebuild |
| `publication.name` | `finpay_publication` | the publication created in §7 |
| `publication.autocreate.mode` | `disabled` | the publication must already exist (created explicitly, not left to Debezium to auto-create with possibly different scope) |
| `slot.name` | `finpay_slot` | explicit, not Debezium's auto-generated default |
| `snapshot.mode` | `initial` | the 11 tables are already populated (S10) — a full initial snapshot is required before streaming begins |
| `decimal.handling.mode` | `string` | monetary `NUMERIC` columns appear as human-readable decimal strings in the JSON payload (e.g. `"12.34"`), not opaque base64 — a deliberate choice for this fintech-focused portfolio project's data transparency, not Debezium's own default (`precise`, which base64-encodes) |

Credentials are read from `.env` via the same `OltpSettings` dataclass S10
already uses — never a literal in source code, never logged, never committed.

**Real finding, not assumed**: Kafka Connect's own REST API echoes the full
connector config — including `database.password` in plaintext — back in its
`POST /connectors` and `GET /connectors/<name>` responses. This is standard
Kafka Connect behavior (it has no secret redaction unless a `ConfigProvider`
is configured), not a bug in this project's code, but a real operational risk
worth flagging (§20) rather than silently glossing over. This repo's own code
never logs or persists that response.

## 10. Kafka Topics

Debezium's own naming convention: `<topic.prefix>.<schema>.<table>`. With
`topic.prefix=finpay` and `schema.include.list=finpay`, every table gets:

```
finpay.finpay.clients
finpay.finpay.programs
finpay.finpay.campaigns
finpay.finpay.offers
finpay.finpay.cardholders
finpay.finpay.card_tokens
finpay.finpay.transactions
finpay.finpay.transaction_events
finpay.finpay.settlements
finpay.finpay.reconciliation
finpay.finpay.reward_events
```

Immediately identifies source database context (`finpay` = the FinPay
platform), schema (`finpay`), and table — exactly the convention this
phase's brief suggested as an example. Topics are Debezium/Kafka-Connect-
created automatically per table (`topic.creation.enable=true`) — never
manually created per event.

## 11. Initial Snapshot

With `snapshot.mode=initial` and 11 already-populated tables, the connector's
first action on startup is a **consistent snapshot** of all current rows in
each table, emitted as CDC-shaped events with `payload.op = "r"` (read/
snapshot — **not** `"c"`), before it transitions to streaming live WAL
changes (`payload.op = "c"`/`"u"`/`"d"` from that point on). The connector
reaches Kafka Connect's `RUNNING` state only after the snapshot completes and
streaming begins.

## 12. INSERT / UPDATE / DELETE Events

Standard Debezium PostgreSQL operation codes:

| Code | Meaning |
|---|---|
| `r` | snapshot read (initial snapshot only, §11) |
| `c` | create (INSERT) |
| `u` | update |
| `d` | delete |

## 13. CDC Event Structure

Debezium's Kafka Connect worker in this setup is configured with JSON
converters and schemas enabled (the `debezium/connect` image's own default —
verified, not assumed, by inspecting real captured messages, not by reading
Debezium's documentation and guessing). A real snapshot event, consumed via
`docker exec merchantmcc_kafka kafka-console-consumer.sh` against
`finpay.finpay.clients` (payload only, schema omitted for brevity):

```json
{
  "before": null,
  "after": {
    "client_id": "CLI-0001",
    "lei": "2138003MPKWZNB34SE20",
    "legal_name": "APS ISSUE DISCRETIONARY TRUST",
    "client_type": "ISSUER",
    "country_code": "GB",
    "onboarding_date": 19955,
    "status": "ACTIVE",
    "source_type": "synthetic"
  },
  "source": {
    "version": "3.0.0.Final", "connector": "postgresql", "name": "finpay",
    "ts_ms": 1789987830220, "snapshot": "first_in_data_collection",
    "db": "merchantmcc", "sequence": "[null,\"43666144\"]",
    "schema": "finpay", "table": "clients", "txId": 831, "lsn": 43666144
  },
  "transaction": null,
  "op": "r",
  "ts_ms": 1789987830723
}
```

A real INSERT event on `finpay.finpay.transactions` (after a genuine
`amount = 42.00` row, `decimal.handling.mode=string` confirmed working):

```json
"after": {
  "transaction_id": "TXN-CDCTST01", "token_id": "TKN-...", "merchant_id": "node:cdc-test",
  "mcc_code": "5999", "mcc_confidence": "0.50", "currency_code": "GBP",
  "amount": "42.00", "transaction_timestamp": 1767261600000000,
  "status": "APPROVED", "decline_reason": null, "auth_code": "CDCTST", "source_type": "synthetic"
}
```

**Corrections made after actually inspecting real messages** (this phase's
own "do not assume exact event fields" instruction, applied to an earlier
draft of this document too, not just to the implementation):
- `before` is `null` for insert/snapshot-read; populated for update/delete;
  `after` is `null` for delete.
- **`DATE` columns (e.g. `onboarding_date`) are NOT ISO strings** — they're
  encoded as `io.debezium.time.Date`, a Kafka Connect logical type carried as
  a plain `int32` = days since the Unix epoch (`19955` here). `TIMESTAMP`
  columns similarly arrive as large integers (`io.debezium.time.*Timestamp`
  variants, microseconds/nanoseconds since epoch depending on precision), not
  strings — a genuine, verified correction to what an earlier draft of this
  document assumed before real messages were inspected.
- `NUMERIC` columns (`amount`, `mcc_confidence`, ...) **are** plain decimal
  strings (`"42.00"`), confirming `decimal.handling.mode=string` (§9) works
  as configured — this assumption held up under inspection.
- `source.lsn`/`source.txId` are the PostgreSQL WAL position and transaction
  ID that produced the event — the durable ordering key. A real DELETE was
  additionally observed to produce a second, **null-value "tombstone"**
  record immediately after the delete event itself (for Kafka log-compaction
  support) — a real Debezium behavior a test had to be corrected for (see
  the S11 completion report's defects section).

## 14. Source Metadata

`payload.source` carries exactly what a downstream consumer needs to
reconstruct provenance without re-deriving it: `connector`, `db`, `schema`,
`table`, `txId`, `lsn`, `ts_ms` (source commit time, distinct from
`payload.ts_ms`, Debezium's own processing time).

## 15. Ordering and Offsets

Within one Kafka topic-partition, message order == WAL commit order for that
table (Debezium preserves this). Each topic here is created with a single
partition (`topic.creation.default.partitions=1`), so **per-table ordering
is total**, not just per-partition — a deliberate simplicity choice for this
phase's single-node dev setup (documented, not silently assumed). Kafka's own
offset (per-partition, monotonically increasing) is the delivery-position
marker; `source.lsn` is the *origin* position in PostgreSQL's WAL — downstream
consumers needing exactly-once/idempotent semantics will need both, per §17
(explicitly deferred here, not implemented).

## 16. Failure/Restart Behavior

Kafka Connect persists connector offsets in its own internal topic
(`finpay_connect_offsets`); connector *configuration* is persisted in
`finpay_connect_configs`. **Actually verified** (`docker compose restart
connect`, not assumed): the connector was automatically restored and resumed
`RUNNING` state after the container restart, with no manual re-registration
needed. The `clients` topic's offset (checked via `kafka-get-offsets.sh`
before/after) went from 12 to 15 — **not** 24, confirming no duplicate
re-snapshot occurred; the 3 extra messages were independently traced to a
real, legitimate insert+delete from an unrelated concurrently-running OLTP
test fixture (`tests/test_oltp.py`'s isolated `CLI-9001` rollback test),
themselves correctly captured as real CDC events — further incidental
confirmation the pipeline works, not noise. The replication slot's
`restart_lsn` advanced normally (not reset) across the restart.

## 17. Idempotency Considerations (deferred, documented only — §13 of this phase's instructions)

This phase does **not** build a downstream deduplication mechanism. A future
Kafka consumer will need to key on the combination of: **Kafka topic +
partition + offset** (delivery-level dedup, e.g. after a consumer restart
re-reads a not-yet-committed range) and/or **`source.lsn`** (origin-level
dedup, authoritative regardless of how many times Kafka redelivers it) and
**`payload.op`** (an `"u"`/`"d"` must never be treated as a fresh `"c"`).
None of this is implemented here — it belongs to the future CDC Bronze
consumer.

## 18. Future Kafka → Bronze Architecture (NOT implemented here)

```
[S11, done]                              [future phase]
PostgreSQL OLTP → Debezium → Kafka  ────▶  Python consumer → CDC Bronze → CDC Silver → OLAP/Gold
```

## 19. What Is Deliberately NOT Implemented in S11

A Kafka consumer of any kind (Python or otherwise), CDC Bronze, CDC Silver,
OLAP/Gold/data marts, dbt, Airflow, Spark/PySpark, Databricks, Snowflake,
ClickHouse, Trino, Power BI, exactly-once/dedup logic, multi-partition
topics, TLS/SASL security hardening (this is a local dev-only stack, not
internet-exposed), schema registry (plain JSON+schema envelope was used
instead — Avro/Protobuf + a registry is a reasonable future upgrade, not
required for this phase's objective).

## 20. Operational Risks

- **A real, pre-existing defect was found and fixed in this phase**: the
  `postgres` service's healthcheck used `pg_isready -U ${POSTGRES_USER}` —
  `${POSTGRES_USER}` is resolved by the `docker compose` CLI itself at
  compose-file-parse time, from whatever directory/`.env` context it happens
  to be invoked from, not from the container's own environment. This silently
  broke (`pg_isready: option requires an argument: U`, healthcheck
  permanently failing) as soon as the container was recreated from a
  different invocation context than however it was originally started —
  latent since before this phase (S10 never triggered it, since S10 reused
  an already-running container rather than recreating it). It blocked this
  phase's `connect` service (`depends_on: condition: service_healthy`)
  outright. Fixed with the smallest correction: `"$$POSTGRES_USER"` (escaped
  so Compose passes a literal shell variable into the container's own
  `CMD-SHELL`, which resolves it from `env_file` — correct regardless of
  invocation context). See `docker/docker-compose.yml`'s inline comment.
- **Kafka Connect's REST API exposes the connector's password in plaintext**
  in its own responses (§9) — a real, verified finding, not a theoretical
  concern. Mitigated in production Debezium deployments via Kafka Connect's
  `ConfigProvider` mechanism (e.g. pulling secrets from a vault at runtime
  instead of embedding them in the connector config) — not implemented here,
  out of scope for a local dev-only stack.
- **Replication slot retention**: a replication slot that stops being read
  (e.g. Debezium stops permanently without being properly removed) causes
  PostgreSQL to retain WAL indefinitely, which can fill disk. Not a risk at
  this phase's dev scale, but a real production concern — monitoring
  `pg_replication_slots.restart_lsn` lag is the standard mitigation, not
  implemented here (out of scope).
- **Single-partition topics** (§15) mean per-table throughput is bounded by
  one Kafka partition — fine for this dataset's scale, would need revisiting
  before any real production volume.
- **No schema evolution strategy** is defined yet (e.g. adding a column to
  `transactions` later) — deferred with the schema registry decision above.
- **`decimal.handling.mode=string`** (§9) trades a small amount of type
  safety (downstream must re-parse the string) for human-readability during
  this project's development/verification — worth reconsidering once a real
  downstream consumer exists.

## Architecture Diagram

```
                    ┌─────────────────────────┐
                    │   PostgreSQL 16 OLTP     │
                    │   db: merchantmcc        │
                    │   schema: finpay         │
                    │   wal_level = logical    │
                    │                          │
                    │   finpay_publication ────┼──▶ WAL (pgoutput)
                    │   (11 explicit tables)   │
                    └───────────┬──────────────┘
                                │ logical replication
                                ▼
                    ┌─────────────────────────┐
                    │  replication slot:       │
                    │  finpay_slot             │
                    └───────────┬──────────────┘
                                │
                    ┌───────────▼──────────────┐
                    │  Debezium PostgreSQL      │
                    │  connector (Kafka Connect)│
                    │  finpay-postgres-cdc-     │
                    │  connector                │
                    └───────────┬──────────────┘
                                │ produces
                                ▼
      ┌─────────────────────────────────────────────────┐
      │  Kafka topics (topic.prefix=finpay)               │
      │  finpay.finpay.clients                             │
      │  finpay.finpay.programs                            │
      │  finpay.finpay.transactions   ◀── snapshot (op=r)  │
      │  finpay.finpay.transaction_events  + live (c/u/d)  │
      │  ... (11 total)                                    │
      └─────────────────────────────────────────────────┘
                                │
                     [S11 boundary — nothing further
                      implemented in this phase]
                                ▼
                     future: Kafka consumer → CDC Bronze
```
