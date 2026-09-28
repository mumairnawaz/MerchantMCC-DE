"""PyArrow schema for CDC Bronze records — every field from docs/25 §7, no
more, no less. `before`/`after`/`event_key` are stored as raw JSON text, NOT
flattened into typed columns (§8 of this phase: Bronze CDC is a raw/audit
layer; business typing is Silver CDC's job, not built here).
"""

import pyarrow as pa

CDC_BRONZE_SCHEMA = pa.schema(
    [
        ("source_topic", pa.string()),
        ("kafka_partition", pa.int32()),
        ("kafka_offset", pa.int64()),
        ("event_key", pa.string()),  # raw JSON text of the Debezium key payload
        ("operation", pa.string()),  # r / c / u / d / t(ombstone — synthetic, see src/cdc/envelope.py)
        ("before", pa.string()),  # raw JSON text, NULL if absent
        ("after", pa.string()),  # raw JSON text, NULL if absent
        ("source_connector", pa.string()),
        ("source_name", pa.string()),
        ("source_schema", pa.string()),
        ("source_table", pa.string()),
        ("source_database", pa.string()),
        ("source_lsn", pa.int64()),
        ("source_timestamp_ms", pa.int64()),
        ("snapshot_indicator", pa.string()),
        ("transaction_id", pa.string()),
        ("transaction_total_order", pa.int64()),
        ("transaction_data_collection_order", pa.int64()),
        ("cdc_received_at_utc", pa.string()),
    ]
)
