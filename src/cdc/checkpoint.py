"""Local, file-based CDC Bronze checkpoint store — the idempotency mechanism
(docs/25 §9): one JSON file per (topic, partition), recording the highest
Kafka offset already durably persisted to Bronze for that partition.

Deliberately mirrors src/ingestion/watermark.py's exact design (a narrow
read/write interface over a small per-key JSON file) rather than inventing a
different mechanism — this project already has one established pattern for
"the last position we've durably processed up to," and CDC checkpointing is
the same problem at a different grain (per topic-partition instead of per
Bronze source).

Why this exists ON TOP OF Kafka's own consumer-group offset commits (not
instead of): Kafka's committed offset alone cannot prevent a duplicate Bronze
write if the consumer crashes AFTER writing Bronze but BEFORE committing the
offset — on restart it would legitimately be redelivered the same message(s).
Because a Kafka partition delivers messages in strictly increasing offset
order (docs/25 §11 — this consumer relies on that guarantee, never invents
its own reordering), "highest offset already persisted" is a correct and
sufficient dedup key: any redelivered message with offset <= the checkpoint
was already durably written and is safely skipped (src/cdc/consumer.py).
"""

import json
from pathlib import Path

from src.ingestion.common import utc_now_iso

CHECKPOINT_ROOT = Path("data/bronze_cdc/_checkpoints")


def _checkpoint_path(topic: str, partition: int, root: Path = CHECKPOINT_ROOT) -> Path:
    return root / topic / f"partition_{partition}.json"


def read_checkpoint(topic: str, partition: int, root: Path = CHECKPOINT_ROOT) -> int | None:
    """The highest offset already persisted for this (topic, partition), or
    None if nothing has ever been persisted — callers treat None as "process
    from the earliest available message"."""
    path = _checkpoint_path(topic, partition, root)
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("last_persisted_offset")


def write_checkpoint(topic: str, partition: int, last_persisted_offset: int, *, root: Path = CHECKPOINT_ROOT) -> Path:
    """Callers must only call this AFTER the corresponding Bronze write has
    been confirmed durable (the Parquet file written to disk) — never before,
    and never on a failed write. See src/cdc/consumer.py's flush order."""
    path = _checkpoint_path(topic, partition, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "topic": topic,
        "partition": partition,
        "last_persisted_offset": last_persisted_offset,
        "updated_at_utc": utc_now_iso(),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path
