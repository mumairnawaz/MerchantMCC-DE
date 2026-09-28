"""The S12 CDC Bronze consumer: Kafka -> validate -> dedup -> persist -> checkpoint -> commit.

Conceptual flow (docs/25 §10), implemented exactly in this order per message
batch, per (topic, partition):

    poll Kafka -> read event -> validate -> persist Bronze -> confirm
    persistence -> write checkpoint -> commit Kafka offset

If Bronze persistence fails for a topic, that topic's checkpoint is NOT
updated and its Kafka offsets are NOT committed — the next run() will
legitimately reprocess it (retried, not lost). Other topics that succeeded in
the same run are unaffected: each Kafka topic here is an independent stream
with no cross-topic FK dependency at the Bronze layer (unlike src/oltp/
loader.py's all-or-nothing transaction, which was correct there because
OLTP tables DO have FK relationships to each other — CDC Bronze topics don't).

This is a finite BATCH consumer — run() drains what's currently available and
returns a report, matching every other src/ingestion/*.py `run()` in this
project, not a perpetual streaming daemon.
"""

import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
from confluent_kafka import Consumer, TopicPartition

from src.cdc.bronze_schema import CDC_BRONZE_SCHEMA
from src.cdc.checkpoint import CHECKPOINT_ROOT, read_checkpoint, write_checkpoint
from src.cdc.consumer_config import CdcConsumerSettings
from src.cdc.consumer_config import settings as default_settings
from src.cdc.envelope import EnvelopeValidationError, ParsedEvent, parse_message
from src.ingestion.common import utc_now_iso
from src.silver.common import reconcile_counts
from src.silver.quarantine import build_rejection, write_quarantine

BRONZE_CDC_ROOT = Path("data/bronze_cdc")
QUARANTINE_CDC_ROOT = Path("data/quarantine_cdc")


@dataclass
class RawMessage:
    """A topic/partition/offset/key/value tuple — deliberately not
    confluent_kafka.Message itself, so the core per-message logic below is
    unit-testable without a live broker (tests/test_cdc_consumer_unit.py)."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


@dataclass
class TopicRunResult:
    topic: str
    events_read: int = 0
    events_persisted: int = 0
    tombstones_handled: int = 0
    duplicates_skipped: int = 0
    quarantined: int = 0
    written_paths: list[Path] = field(default_factory=list)
    quarantine_path: Path | None = None
    max_offset_seen: dict[int, int] = field(default_factory=dict)  # partition -> max offset
    checkpoint_committed: bool = False


def _table_name_from_topic(topic: str) -> str:
    # "finpay.finpay.transactions" -> "transactions" (topic.prefix, then
    # schema, then table — src/cdc/connector.py's own TOPIC_PREFIX/SCHEMA_NAME)
    return topic.rsplit(".", 1)[-1]


def build_bronze_record(msg: RawMessage, parsed: ParsedEvent) -> dict[str, Any]:
    return {
        "source_topic": msg.topic,
        "kafka_partition": msg.partition,
        "kafka_offset": msg.offset,
        "event_key": parsed.event_key_json,
        "operation": parsed.operation,
        "before": parsed.before_json,
        "after": parsed.after_json,
        "source_connector": parsed.source_connector,
        "source_name": parsed.source_name,
        "source_schema": parsed.source_schema,
        "source_table": parsed.source_table,
        "source_database": parsed.source_database,
        "source_lsn": parsed.source_lsn,
        "source_timestamp_ms": parsed.source_timestamp_ms,
        "snapshot_indicator": parsed.snapshot_indicator,
        "transaction_id": parsed.transaction_id,
        "transaction_total_order": parsed.transaction_total_order,
        "transaction_data_collection_order": parsed.transaction_data_collection_order,
        "cdc_received_at_utc": utc_now_iso(),
    }


def process_message(
    msg: RawMessage,
    *,
    checkpoints: dict[tuple[str, int], int | None],
    buffers: dict[str, list[dict[str, Any]]],
    rejections: dict[str, list[dict[str, Any]]],
    results: dict[str, TopicRunResult],
) -> None:
    """Pure per-message logic — no Kafka I/O, no filesystem I/O. Shared by
    the real consumer loop and unit tests. Mutates the passed-in
    buffers/rejections/results dicts (grouped by table name)."""
    table = _table_name_from_topic(msg.topic)
    result = results.setdefault(table, TopicRunResult(topic=msg.topic))
    result.events_read += 1

    checkpoint_key = (msg.topic, msg.partition)
    last_persisted = checkpoints.get(checkpoint_key)
    if last_persisted is not None and msg.offset <= last_persisted:
        result.duplicates_skipped += 1
        return

    try:
        parsed = parse_message(msg.key, msg.value)
    except EnvelopeValidationError as exc:
        rejections.setdefault(table, []).append(
            build_rejection(
                original_raw_record={
                    "topic": msg.topic,
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "key": msg.key.decode("utf-8", errors="replace") if msg.key else None,
                    "value": msg.value.decode("utf-8", errors="replace") if msg.value else None,
                },
                error_reason=str(exc),
                failing_check_name="envelope_validation",
                source_bronze_run_id=f"kafka:{msg.topic}:{msg.partition}",
                source_record_identifier=f"{msg.topic}:{msg.partition}:{msg.offset}",
            )
        )
        result.quarantined += 1
        result.max_offset_seen[msg.partition] = max(result.max_offset_seen.get(msg.partition, -1), msg.offset)
        return

    record = build_bronze_record(msg, parsed)
    buffers.setdefault(table, []).append(record)
    if parsed.is_tombstone:
        result.tombstones_handled += 1
    else:
        result.events_persisted += 1
    result.max_offset_seen[msg.partition] = max(result.max_offset_seen.get(msg.partition, -1), msg.offset)


def _flush_table(
    table: str,
    records: list[dict[str, Any]],
    result: TopicRunResult,
    *,
    bronze_root: Path,
) -> None:
    """Writes one Parquet file per (topic, kafka_partition) touched this run —
    partitioned by the Kafka partition (structural CDC metadata), never by a
    business field (docs/25 §9). Append-only: a brand new, uniquely-timestamped
    file per run, matching this project's existing Bronze run_<timestamp>
    convention (src/ingestion/common.py) — never overwrites or rewrites a
    prior run's file, so immutability (§10) holds at the filesystem level too."""
    by_partition: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        by_partition[record["kafka_partition"]].append(record)

    run_id = utc_now_iso().replace(":", "").replace("-", "").rstrip("Z") + "Z"
    for partition, partition_records in by_partition.items():
        partition_dir = bronze_root / table / f"partition={partition}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        path = partition_dir / f"run_{run_id}.parquet"
        table_arrow = pa.Table.from_pylist(partition_records, schema=CDC_BRONZE_SCHEMA)
        import pyarrow.parquet as pq

        pq.write_table(table_arrow, path)
        result.written_paths.append(path)


@dataclass
class ConsumerRunReport:
    topics: dict[str, TopicRunResult] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            table: {
                "events_read": r.events_read,
                "events_persisted": r.events_persisted,
                "tombstones_handled": r.tombstones_handled,
                "duplicates_skipped": r.duplicates_skipped,
                "quarantined": r.quarantined,
                "checkpoint_committed": r.checkpoint_committed,
                "written_paths": [str(p) for p in r.written_paths],
                "quarantine_path": str(r.quarantine_path) if r.quarantine_path else None,
            }
            for table, r in self.topics.items()
        }


def run(
    *,
    settings: CdcConsumerSettings = default_settings,
    bronze_root: Path = BRONZE_CDC_ROOT,
    quarantine_root: Path = QUARANTINE_CDC_ROOT,
    checkpoint_root: Path = CHECKPOINT_ROOT,
    max_messages: int | None = None,
) -> ConsumerRunReport:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": settings.group_id,
            "enable.auto.commit": False,  # manual commit only, after a confirmed Bronze write — §12
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(settings.topics)

    checkpoints: dict[tuple[str, int], int | None] = {}
    buffers: dict[str, list[dict[str, Any]]] = {}
    rejections: dict[str, list[dict[str, Any]]] = {}
    results: dict[str, TopicRunResult] = {}
    total_read = 0

    try:
        idle_polls_remaining = max(1, int(settings.idle_timeout_seconds / settings.poll_timeout_seconds))
        while idle_polls_remaining > 0:
            msg = consumer.poll(timeout=settings.poll_timeout_seconds)
            if msg is None:
                idle_polls_remaining -= 1
                continue
            if msg.error():
                idle_polls_remaining -= 1
                continue

            idle_polls_remaining = max(1, int(settings.idle_timeout_seconds / settings.poll_timeout_seconds))
            raw = RawMessage(topic=msg.topic(), partition=msg.partition(), offset=msg.offset(), key=msg.key(), value=msg.value())

            key = (raw.topic, raw.partition)
            if key not in checkpoints:
                checkpoints[key] = read_checkpoint(raw.topic, raw.partition, root=checkpoint_root)

            process_message(raw, checkpoints=checkpoints, buffers=buffers, rejections=rejections, results=results)
            total_read += 1
            if max_messages is not None and total_read >= max_messages:
                break

        # ---- flush: persist Bronze, then checkpoint, then commit offsets ----
        for table, records in buffers.items():
            result = results[table]
            try:
                _flush_table(table, records, result, bronze_root=bronze_root)
            except Exception:
                # Storage failure: do NOT write a checkpoint, do NOT commit
                # offsets for this table's partitions — §12/§13. Leave
                # duplicates_skipped/quarantined counts as observed for the
                # report, but skip the rest of this table's flush entirely.
                continue

            offsets_to_commit = []
            for partition, max_offset in result.max_offset_seen.items():
                write_checkpoint(result.topic, partition, max_offset, root=checkpoint_root)
                offsets_to_commit.append(TopicPartition(result.topic, partition, max_offset + 1))
            if offsets_to_commit:
                consumer.commit(offsets=offsets_to_commit, asynchronous=False)
            result.checkpoint_committed = True

        for table, rejected in rejections.items():
            result = results[table]
            path = write_quarantine(table, rejected, quarantine_root=quarantine_root)
            result.quarantine_path = path
            # Quarantined-but-not-yet-flushed-via-buffer offsets still need a
            # checkpoint/commit if that table had NO successful bronze records
            # this run (e.g. every message this run was malformed) — handled
            # by folding quarantine offsets into max_offset_seen already, so
            # if the table had no `buffers` entry at all, commit here instead.
            if table not in buffers and result.max_offset_seen:
                offsets_to_commit = [TopicPartition(result.topic, p, o + 1) for p, o in result.max_offset_seen.items()]
                for partition, max_offset in result.max_offset_seen.items():
                    write_checkpoint(result.topic, partition, max_offset, root=checkpoint_root)
                consumer.commit(offsets=offsets_to_commit, asynchronous=False)
                result.checkpoint_committed = True

        # ---- reconciliation (reusing the existing generic helper — §16/§13 of this phase) ----
        for table, result in results.items():
            reconcile_counts(
                result.events_read,
                {
                    "events_persisted": result.events_persisted,
                    "tombstones_handled": result.tombstones_handled,
                    "duplicates_skipped": result.duplicates_skipped,
                    "quarantined": result.quarantined,
                },
                label=f"cdc_bronze:{table}",
            )

    finally:
        consumer.close()

    return ConsumerRunReport(topics=results)
