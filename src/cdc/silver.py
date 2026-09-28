"""CDC Bronze -> CDC Silver transformation engine (S13).

Reads data/bronze_cdc/<table>/ (S12's immutable raw Debezium records),
transforms/validates into typed business records, and writes:
  - "current_state" tables (9): one upserted snapshot file per table,
    data/silver_cdc/<dataset>/data.parquet — latest-by-source.lsn wins.
  - "event_log" tables (2): append-only, data/silver_cdc/<dataset>/run_<ts>Z.parquet

Ingestion idempotency (§11): reuses src.cdc.checkpoint's exact read/write
functions (the same module S12 uses for Kafka->Bronze), pointed at a
DIFFERENT root (SILVER_CDC_CHECKPOINT_ROOT) so Bronze's own "Kafka position
consumed" checkpoint and Silver's "Bronze position consumed" checkpoint never
collide — same mechanism, different stage, not a new framework.

Business freshness/ordering (§10): source.lsn is authoritative — a single,
monotonically increasing Postgres WAL position, comparable across ALL tables
in this database (not just within one). kafka_offset is only used for
ingestion-level dedup (already established in S12); it is never used to
decide which business version wins.

Out-of-order/stale/delete-after-update handling (§14): see
_apply_current_state_event()'s docstring for the full state machine — a
DELETE always removes the key from current state when newer than anything
seen so far; a stale (older-lsn) event of any kind never overwrites newer
state; an update arriving after a (newer) delete is a genuine anomaly and is
quarantined for investigation rather than silently resurrecting the row.
"""

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from src.cdc.checkpoint import read_checkpoint, write_checkpoint
from src.cdc.consumer import BRONZE_CDC_ROOT
from src.cdc.silver_schema import build_schema
from src.cdc.silver_tables import TABLE_SPECS, Column, TableSpec
from src.ingestion.common import utc_now_iso
from src.silver.common import compute_record_hash, reconcile_counts
from src.silver.quarantine import build_rejection, write_quarantine

SILVER_CDC_ROOT = Path("data/silver_cdc")
SILVER_CDC_CHECKPOINT_ROOT = Path("data/silver_cdc/_checkpoints")
DELETED_KEYS_ROOT = Path("data/silver_cdc/_deleted_keys")
QUARANTINE_CDC_SILVER_ROOT = Path("data/quarantine_cdc_silver")


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _pk_key(record: dict[str, Any], pk_fields: tuple[str, ...]) -> str:
    return json.dumps([record[f] for f in pk_fields], default=_json_default)


# ---- reading Bronze ----


def read_bronze_records(bronze_table: str, root: Path = BRONZE_CDC_ROOT) -> list[dict[str, Any]]:
    """Every Bronze CDC record for this table, across all partitions/runs,
    sorted by (kafka_partition, kafka_offset) — the real, authoritative
    Kafka delivery order this engine relies on (never invents its own)."""
    table_dir = root / bronze_table
    if not table_dir.exists():
        return []
    records: list[dict[str, Any]] = []
    for part_file in table_dir.rglob("*.parquet"):
        records.extend(pq.read_table(part_file).to_pylist())
    records.sort(key=lambda r: (r["kafka_partition"], r["kafka_offset"]))
    return records


# ---- deleted-key persistence (needed so a DELETE is remembered ACROSS runs,
# not just within one — see module docstring) ----


def _deleted_keys_path(dataset: str, root: Path = DELETED_KEYS_ROOT) -> Path:
    return root / f"{dataset}.json"


def _read_deleted_keys(dataset: str, root: Path = DELETED_KEYS_ROOT) -> dict[str, int]:
    path = _deleted_keys_path(dataset, root)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_deleted_keys(dataset: str, deleted_keys: dict[str, int], root: Path = DELETED_KEYS_ROOT) -> None:
    path = _deleted_keys_path(dataset, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(deleted_keys), encoding="utf-8")


# ---- transform + validate ----


def _extract_typed_record(columns: tuple[Column, ...], raw: dict[str, Any]) -> dict[str, Any]:
    return {c.name: c.converter(raw.get(c.name)) for c in columns}


def _validate_typed_record(typed: dict[str, Any], spec: TableSpec, *, is_delete: bool = False) -> tuple[bool, str | None]:
    """`is_delete=True` (the `before` image of a 'd' operation) validates ONLY
    that the primary key is present — a delete's job is to identify WHICH row
    to remove, so the rest of its business columns carry no semantic meaning
    and must not be validated as if they were a real business state. This was
    found to matter on real data: PostgreSQL's pre-S12-fix REPLICA IDENTITY
    DEFAULT setting (see docs/24 §17) left some historical delete events with
    only their PK populated and every other column blank/zero — correctly
    identifying the row to delete, but previously rejected outright by this
    function applying full business validation to a payload that was never
    meant to carry full business content. Fixed here, not by inventing a new
    business rule, but by recognizing which fields a delete actually needs."""
    for f in spec.pk_fields:
        if typed.get(f) is None or (isinstance(typed.get(f), str) and typed[f].strip() == ""):
            return False, f"missing_pk_{f}"
    if is_delete:
        return True, None
    for c in spec.columns:
        value = typed.get(c.name)
        if c.required and (value is None or (isinstance(value, str) and value.strip() == "")):
            return False, f"required_field_{c.name}"
        if c.positive and value is not None and not (value > 0):
            return False, f"positive_{c.name}"
        if c.valid_values is not None and value is not None and value not in c.valid_values:
            return False, f"invalid_value_{c.name}"
    return True, None


BUSINESS_HASH_EXCLUDE = set()  # every business column participates in the hash — no lineage fields are ever business columns here


@dataclass
class TableRunResult:
    dataset: str
    events_read: int = 0
    accepted: int = 0  # inserted + updated + unchanged (current_state) or persisted (event_log)
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    deleted: int = 0
    stale_skipped: int = 0
    duplicates_skipped: int = 0
    quarantined: int = 0
    tombstones_handled: int = 0
    written_paths: list[Path] = field(default_factory=list)
    quarantine_path: Path | None = None


def _quarantine_record(bronze_record: dict[str, Any], reason: str) -> dict[str, Any]:
    return build_rejection(
        original_raw_record={
            "kafka_topic": bronze_record["source_topic"],
            "kafka_partition": bronze_record["kafka_partition"],
            "kafka_offset": bronze_record["kafka_offset"],
            "operation": bronze_record["operation"],
            "source_lsn": bronze_record["source_lsn"],
            "before": bronze_record["before"],
            "after": bronze_record["after"],
        },
        error_reason=reason,
        failing_check_name=reason,
        source_bronze_run_id=f"{bronze_record['source_topic']}:{bronze_record['kafka_partition']}",
        source_record_identifier=f"{bronze_record['source_topic']}:{bronze_record['kafka_partition']}:{bronze_record['kafka_offset']}",
    )


@dataclass
class _KeyState:
    lsn: int
    deleted: bool
    record_hash: str | None
    record: dict[str, Any] | None


def _apply_current_state_event(
    key_state: dict[str, _KeyState],
    pk: str,
    op: str,
    lsn: int,
    typed: dict[str, Any] | None,
    record_hash: str | None,
    result: TableRunResult,
) -> str:
    """The out-of-order/stale/delete state machine (§14). Returns the
    disposition string for logging/testing. Never invents "latest processing
    run wins" — every decision is driven by comparing `lsn` (source.lsn,
    Postgres's own authoritative WAL position) against the last-seen lsn for
    this key, regardless of the ORDER events were read in.
    """
    existing = key_state.get(pk)

    if op == "d":
        if existing is None or lsn > existing.lsn:
            was_visible = existing is not None and not existing.deleted
            key_state[pk] = _KeyState(lsn=lsn, deleted=True, record_hash=None, record=None)
            if was_visible or existing is None:
                result.deleted += 1
            return "deleted"
        result.stale_skipped += 1
        return "stale_skipped"

    # r / c / u
    if existing is None:
        key_state[pk] = _KeyState(lsn=lsn, deleted=False, record_hash=record_hash, record=typed)
        result.inserted += 1
        return "inserted"

    if existing.deleted:
        if op == "c" and lsn > existing.lsn:
            key_state[pk] = _KeyState(lsn=lsn, deleted=False, record_hash=record_hash, record=typed)
            result.inserted += 1
            return "inserted"
        if lsn <= existing.lsn:
            result.stale_skipped += 1
            return "stale_skipped"
        # an update/snapshot against an already-deleted key, with a NEWER lsn
        # than the delete — a genuine anomaly (should not occur in real
        # Postgres CDC); never silently resurrected.
        return "conflict_update_after_delete"

    if lsn > existing.lsn:
        if record_hash == existing.record_hash:
            result.unchanged += 1
            return "unchanged"  # existing row left completely untouched, per Silver convention
        key_state[pk] = _KeyState(lsn=lsn, deleted=False, record_hash=record_hash, record=typed)
        result.updated += 1
        return "updated"
    if lsn == existing.lsn:
        if record_hash == existing.record_hash:
            result.unchanged += 1
            return "unchanged"
        return "conflict_same_lsn"
    result.stale_skipped += 1
    return "stale_skipped"


def _write_current_state(dataset: str, spec: TableSpec, key_state: dict[str, _KeyState], lineage_by_pk: dict[str, dict[str, Any]]) -> Path | None:
    rows = []
    for pk, state in key_state.items():
        if state.deleted or state.record is None:
            continue
        row = dict(state.record)
        row["record_hash"] = state.record_hash
        row.update(lineage_by_pk[pk])
        rows.append(row)

    path = SILVER_CDC_ROOT / dataset / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = build_schema(spec)
    if not rows:
        table = pa.Table.from_pylist([], schema=schema)
    else:
        table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path)
    return path


def _load_existing_current_state(dataset: str, spec: TableSpec) -> tuple[dict[str, _KeyState], dict[str, dict[str, Any]]]:
    path = SILVER_CDC_ROOT / dataset / "data.parquet"
    key_state: dict[str, _KeyState] = {}
    lineage_by_pk: dict[str, dict[str, Any]] = {}
    if path.exists():
        for row in pq.read_table(path).to_pylist():
            pk = _pk_key(row, spec.pk_fields)
            typed = {c.name: row[c.name] for c in spec.columns}
            key_state[pk] = _KeyState(lsn=row["source_lsn"], deleted=False, record_hash=row["record_hash"], record=typed)
            lineage_by_pk[pk] = {
                "source_name": row["source_name"],
                "kafka_topic": row["kafka_topic"],
                "kafka_partition": row["kafka_partition"],
                "kafka_offset": row["kafka_offset"],
                "source_lsn": row["source_lsn"],
                "operation": row["operation"],
                "source_timestamp_ms": row["source_timestamp_ms"],
                "cdc_received_at_utc": row["cdc_received_at_utc"],
                "silver_processed_at_utc": row["silver_processed_at_utc"],
            }
    # also restore deleted-key memory so cross-run stale-delete comparisons work
    for pk, lsn in _read_deleted_keys(dataset).items():
        if pk not in key_state or lsn > key_state[pk].lsn:
            key_state[pk] = _KeyState(lsn=lsn, deleted=True, record_hash=None, record=None)
    return key_state, lineage_by_pk


def process_table(bronze_table: str, *, bronze_root: Path = BRONZE_CDC_ROOT, silver_root: Path = SILVER_CDC_ROOT, checkpoint_root: Path = SILVER_CDC_CHECKPOINT_ROOT, deleted_keys_root: Path = DELETED_KEYS_ROOT, quarantine_root: Path = QUARANTINE_CDC_SILVER_ROOT) -> TableRunResult:
    spec = TABLE_SPECS[bronze_table]
    result = TableRunResult(dataset=spec.silver_dataset)

    bronze_records = read_bronze_records(bronze_table, root=bronze_root)

    checkpoints: dict[tuple[str, int], int | None] = {}
    max_offset_seen: dict[tuple[str, int], int] = {}
    rejections: list[dict[str, Any]] = []

    key_state: dict[str, _KeyState] = {}
    lineage_by_pk: dict[str, dict[str, Any]] = {}
    event_log_rows: list[dict[str, Any]] = []
    # event_log PK "active" tracking (§13/§6): a PK is legitimately reusable
    # after a real delete — e.g. this project's own S10 idempotency-test
    # fixture repeatedly inserts+deletes the SAME hardcoded event_id/reward_id
    # across many separate pytest sessions, which is real, observed CDC
    # history (docs/26 §17), not an anomaly. Only a `c`/`r` for a PK that is
    # CURRENTLY active (inserted, not yet deleted) is a genuine duplicate.
    # Reconstructed by replaying existing rows' own `operation` column in
    # (kafka_partition, kafka_offset) order — the same authoritative Kafka
    # ordering this whole engine relies on everywhere else.
    event_pk_active: dict[str, bool] = {}

    if spec.mode == "current_state":
        key_state, lineage_by_pk = _load_existing_current_state(spec.silver_dataset, spec)
    else:
        existing_dir = silver_root / spec.silver_dataset
        if existing_dir.exists():
            existing_rows: list[dict[str, Any]] = []
            for p in existing_dir.glob("*.parquet"):
                existing_rows.extend(pq.read_table(p).to_pylist())
            existing_rows.sort(key=lambda r: (r["kafka_partition"], r["kafka_offset"]))
            for row in existing_rows:
                pk = _pk_key(row, spec.pk_fields)
                event_pk_active[pk] = row["operation"] != "d"

    silver_processed_at = utc_now_iso()

    for record in bronze_records:
        topic, partition, offset = record["source_topic"], record["kafka_partition"], record["kafka_offset"]
        ckey = (topic, partition)
        if ckey not in checkpoints:
            checkpoints[ckey] = read_checkpoint(topic, partition, root=checkpoint_root)

        result.events_read += 1
        if checkpoints[ckey] is not None and offset <= checkpoints[ckey]:
            result.duplicates_skipped += 1
            continue

        max_offset_seen[ckey] = max(max_offset_seen.get(ckey, -1), offset)
        op = record["operation"]

        if op == "t":
            result.tombstones_handled += 1
            continue

        raw_json = record["after"] if op in ("r", "c", "u") else record["before"]
        if raw_json is None:
            rejections.append(_quarantine_record(record, "missing_payload_for_operation"))
            result.quarantined += 1
            continue

        raw = json.loads(raw_json)
        typed = _extract_typed_record(spec.columns, raw)
        ok, reason = _validate_typed_record(typed, spec, is_delete=(op == "d"))
        if not ok:
            rejections.append(_quarantine_record(record, reason))
            result.quarantined += 1
            continue

        pk = _pk_key(typed, spec.pk_fields)
        lineage = {
            "source_name": "cdc",
            "kafka_topic": topic,
            "kafka_partition": partition,
            "kafka_offset": offset,
            "source_lsn": record["source_lsn"],
            "operation": op,
            "source_timestamp_ms": record["source_timestamp_ms"],
            "cdc_received_at_utc": record["cdc_received_at_utc"],
            "silver_processed_at_utc": silver_processed_at,
        }

        if spec.mode == "event_log":
            if op == "d":
                # Always appended — a delete is itself a real, traceable
                # event, never silently dropped (§13) — and marks the PK
                # reusable by a genuinely later insert.
                event_pk_active[pk] = False
                row = dict(typed)
                row.update(lineage)
                event_log_rows.append(row)
                result.deleted += 1
                continue
            if event_pk_active.get(pk):
                rejections.append(_quarantine_record(record, "duplicate_primary_key"))
                result.quarantined += 1
                continue
            event_pk_active[pk] = True
            row = dict(typed)
            row.update(lineage)
            event_log_rows.append(row)
            result.accepted += 1
            continue

        record_hash = compute_record_hash(typed, [c.name for c in spec.columns])
        disposition = _apply_current_state_event(key_state, pk, op, record["source_lsn"], typed, record_hash, result)
        if disposition == "conflict_update_after_delete" or disposition == "conflict_same_lsn":
            rejections.append(_quarantine_record(record, disposition))
            result.quarantined += 1
        elif disposition in ("inserted", "updated"):
            lineage_by_pk[pk] = lineage
            result.accepted += 1
        elif disposition == "unchanged":
            result.accepted += 1
        # "deleted"/"stale_skipped" already counted inside _apply_current_state_event

    # ---- flush ----
    if spec.mode == "current_state":
        if bronze_records:  # only write if there was anything to do at all
            path = _write_current_state(spec.silver_dataset, spec, key_state, lineage_by_pk)
            if path:
                result.written_paths.append(path)
            deleted_keys = {pk: s.lsn for pk, s in key_state.items() if s.deleted}
            _write_deleted_keys(spec.silver_dataset, deleted_keys, root=deleted_keys_root)
    else:
        if event_log_rows:
            run_id = utc_now_iso().replace(":", "").replace("-", "").rstrip("Z") + "Z"
            path = silver_root / spec.silver_dataset / f"run_{run_id}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            table = pa.Table.from_pylist(event_log_rows, schema=build_schema(spec))
            pq.write_table(table, path)
            result.written_paths.append(path)

    if rejections:
        result.quarantine_path = write_quarantine(spec.silver_dataset, rejections, quarantine_root=quarantine_root)

    for (topic, partition), max_offset in max_offset_seen.items():
        write_checkpoint(topic, partition, max_offset, root=checkpoint_root)

    reconcile_counts(
        result.events_read,
        {
            "accepted": result.accepted,
            "deleted": result.deleted,
            "stale_skipped": result.stale_skipped,
            "duplicates_skipped": result.duplicates_skipped,
            "quarantined": result.quarantined,
            "tombstones_handled": result.tombstones_handled,
        },
        label=f"cdc_silver:{spec.silver_dataset}",
    )

    return result


def run(tables: list[str] | None = None, **kwargs: Any) -> dict[str, TableRunResult]:
    target_tables = tables or list(TABLE_SPECS.keys())
    return {t: process_table(t, **kwargs) for t in target_tables}
