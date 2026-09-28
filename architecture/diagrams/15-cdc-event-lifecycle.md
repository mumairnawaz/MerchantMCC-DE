# 15. CDC Event Lifecycle (INSERT / UPDATE / DELETE)

How a single row-level change in PostgreSQL becomes a durable, idempotent fact in Gold —
the mechanism CDC exists to provide, shown for all three operation types.

```mermaid
sequenceDiagram
    participant PG as PostgreSQL
    participant WAL as WAL (logical replication slot)
    participant DBZ as Debezium
    participant K as Kafka topic
    participant CONS as CDC Bronze consumer
    participant CKPT as Checkpoint file<br/>(topic, partition)
    participant BRONZE as CDC Bronze<br/>(immutable)
    participant SILVER as CDC Silver<br/>(current-state)

    Note over PG,SILVER: INSERT
    PG->>WAL: INSERT finpay.transactions
    WAL->>DBZ: logical decode
    DBZ->>K: event {op: "c", after: {...}, lsn}
    CONS->>K: poll
    K-->>CONS: event
    CONS->>CKPT: read last_persisted_offset
    CONS->>BRONZE: append (offset > checkpoint)
    CONS->>CKPT: write new offset
    SILVER->>BRONZE: read new records
    SILVER->>SILVER: upsert current-state row

    Note over PG,SILVER: UPDATE
    PG->>WAL: UPDATE ... SET decline_reason=...
    WAL->>DBZ: logical decode
    DBZ->>K: event {op: "u", before, after, lsn}
    CONS->>BRONZE: append (new immutable record)
    SILVER->>SILVER: replace current-state row<br/>(history preserved in Bronze)

    Note over PG,SILVER: DELETE
    PG->>WAL: DELETE FROM transactions WHERE id=...
    WAL->>DBZ: logical decode
    DBZ->>K: tombstone event {op: "d"} + null value
    CONS->>BRONZE: append tombstone (immutable)
    SILVER->>SILVER: remove row from current-state view<br/>(Bronze history NOT destroyed)
```

**Idempotency, concretely**: the checkpoint file tracks `last_persisted_offset` per
`(topic, partition)`, independent of Kafka's own consumer-group offset. If the same
message is delivered twice (a real, expected Kafka possibility), the consumer compares
its offset against the checkpoint and skips anything already persisted — reprocessing
produces zero duplicate Bronze records.

**Full history vs. current state, concretely**: CDC Bronze is append-only and never
rewritten — every insert, update, and delete for a row remains queryable forever. CDC
Silver derives a current-state view from that same history, so a delete correctly removes
a row from Silver's live view without erasing the fact that it ever existed in Bronze.

Full detail: [`docs/25-cdc-bronze-design.md`](../../docs/25-cdc-bronze-design.md),
[`docs/26-cdc-silver-design.md`](../../docs/26-cdc-silver-design.md).
