# 3. CDC Pipeline

```mermaid
flowchart LR
    PG[("PostgreSQL / OLTP<br/>synthetic fintech DB<br/>11 tables")] -->|logical replication| DBZ["Debezium connector"]
    DBZ -->|row-level insert/update/delete| KAFKA["Kafka<br/>one topic per table"]
    KAFKA --> CONS["CDC Bronze consumer<br/>Python"]
    CONS --> CBronze[("CDC Bronze<br/>immutable, append-only")]
    CBronze --> CSILVER["CDC Silver processor"]
    CSILVER --> CS_CURRENT[("CDC Silver<br/>current-state view")]
    CSILVER -.->|full history preserved| CBronze
    CONS -.->|checkpoint per topic/partition| CKPT[("Checkpoint files<br/>last persisted offset")]
    CKPT -.->|idempotent reprocessing| CONS
```

A real, **locally demonstrated** CDC architecture — a single-broker Kafka instance and one
Debezium connector in Docker, not a production-scale multi-broker cluster. PostgreSQL's
own logical replication feeds Debezium, which emits row-level insert/update/delete events
per table into dedicated Kafka topics. A Python consumer persists every event into
immutable CDC Bronze (append-only; nothing is ever rewritten), tracked by a checkpoint
file per `(topic, partition)` — independent of Kafka's own consumer-group offset — so
reprocessing the same messages twice produces zero duplicates. CDC Silver applies
per-key upsert logic to derive a current-state view while Bronze retains full history;
deletes arrive as Kafka tombstones and correctly remove rows from the current-state view
without erasing their Bronze lineage.

Orchestrated by Airflow's `merchantmcc_cdc_pipeline` DAG (`*/15 * * * *`) — see
[`06-airflow-orchestration.md`](06-airflow-orchestration.md). Full detail:
[`docs/24-postgresql-debezium-kafka-cdc.md`](../../docs/24-postgresql-debezium-kafka-cdc.md),
[`docs/25-cdc-bronze-design.md`](../../docs/25-cdc-bronze-design.md),
[`docs/26-cdc-silver-design.md`](../../docs/26-cdc-silver-design.md).
