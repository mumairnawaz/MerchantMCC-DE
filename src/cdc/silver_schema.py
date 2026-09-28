"""Builds the full PyArrow schema for a CDC Silver dataset: business columns
(src/cdc/silver_tables.py) + CDC lineage columns.

Lineage fields retained, and why (§17 — no field kept without a reason):
  - source_name             constant "cdc" — distinguishes this pipeline from
                             the existing API Bronze/Silver source_name values
                             (§19: the two paths must stay distinguishable)
  - kafka_topic/partition/offset   exact Kafka identity of the WINNING event —
                             traceability back to the exact message
  - source_lsn               the authoritative Postgres WAL position used for
                             ordering/freshness (§10) — kept for audit even
                             though the decision it drove is already applied
  - operation                which Debezium op (r/c/u/d) produced this row's
                             current content
  - source_timestamp_ms      Debezium's own event time (exact copy, int64 —
                              not reformatted, avoiding a second lossy
                              conversion beyond what silver_types.py already does)
  - cdc_received_at_utc       when S12's consumer first wrote this event to
                              CDC Bronze (copied from Bronze, not regenerated)
  - silver_processed_at_utc   when THIS S13 transform ran — explicitly NOT
                              used as an idempotency key (§11)
  - record_hash               current-state tables only — change detection,
                              same convention as src/silver/merchant.py
"""

import pyarrow as pa

from src.cdc.silver_tables import TableSpec

LINEAGE_COLUMNS = [
    ("source_name", pa.string()),
    ("kafka_topic", pa.string()),
    ("kafka_partition", pa.int32()),
    ("kafka_offset", pa.int64()),
    ("source_lsn", pa.int64()),
    ("operation", pa.string()),
    ("source_timestamp_ms", pa.int64()),
    ("cdc_received_at_utc", pa.string()),
    ("silver_processed_at_utc", pa.string()),
]


def build_schema(spec: TableSpec) -> pa.Schema:
    fields = [(c.name, c.pa_type) for c in spec.columns]
    fields.extend(LINEAGE_COLUMNS)
    if spec.mode == "current_state":
        fields.append(("record_hash", pa.string()))
    return pa.schema(fields)
