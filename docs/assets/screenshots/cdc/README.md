# CDC Evidence (PostgreSQL → Debezium → Kafka → Bronze → Silver)

## Real evidence (captured 2026-09-28)

**A real checkpoint file** — proves the idempotency mechanism described in
[`architecture/diagrams/15-cdc-event-lifecycle.md`](../../../architecture/diagrams/15-cdc-event-lifecycle.md)
is not just documented but actually running (`data/bronze_cdc/_checkpoints/finpay.finpay.transactions/partition_0.json`):
```json
{
  "topic": "finpay.finpay.transactions",
  "partition": 0,
  "last_persisted_offset": 4633,
  "updated_at_utc": "2026-09-27T18:00:18Z"
}
```

**Real, current consumer lag — zero, proving Bronze is fully caught up with Kafka:**
```
GROUP                      TOPIC                              CURRENT-OFFSET  LOG-END-OFFSET  LAG
finpay-cdc-bronze-consumer finpay.finpay.transaction_events   7617            7617            0
finpay-cdc-bronze-consumer finpay.finpay.settlements          3533            3533            0
```

**A real, previously-executed end-to-end business-change demonstration** (documented in
full, with real LSN/offset/value evidence, earlier in this project's history): a single
reversible `UPDATE` on a real transaction's `decline_reason` was traced through
WAL → Debezium → Kafka → CDC Bronze → CDC Silver → Gold, confirming the control total
correctly stayed unchanged (an attribute-only change, not an amount change). See
[`docs/24-postgresql-debezium-kafka-cdc.md`](../../../docs/24-postgresql-debezium-kafka-cdc.md).

**Caption**: *"CDC — a real checkpoint file and zero consumer lag, proving the
Bronze consumer is idempotent and fully caught up with live Kafka data."*

## Manual screenshot checklist

- [ ] `docker exec merchantmcc_kafka kafka-console-consumer.sh` showing a real CDC event
      payload for one transaction (safe to show — synthetic data, no real PII)
- [ ] A side-by-side of a PostgreSQL row and its corresponding CDC Silver row
