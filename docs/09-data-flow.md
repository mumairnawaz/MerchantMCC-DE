# 09 — Data Flow

## Source to Bronze

Every one of the seven sources follows the identical ingestion shape:

```
API / File  →  fetch()  →  parse_for_metadata()  →  validation checks  →  write raw + metadata.json  →  Bronze
```

`fetch()` performs network I/O only. `parse_for_metadata()` is a pure function that
introspects the response just to count records/discover fields — it never reshapes or
transforms the data. This separation is what makes the parsing logic unit-testable without
any network access. Diagram: [`architecture/README.md`](../architecture/README.md).

## Bronze to Silver to Gold

```
BRONZE (raw, as received) → DATA QUALITY (structural + cross-source checks)
  → SILVER (cleaned, deduplicated, conformed keys, reference crosswalks applied)
  → TRANSFORMATION (business logic)
  → WAREHOUSE (dimensional schema)
  → GOLD MARTS (merchant performance, reconciliation, rewards/CLO, etc.)
```

## Synthetic data's entry point

Synthetic OLTP (program/campaign/offer) and synthetic transaction events enter Bronze
**separately** from the real/reference paths — they never pass through the same ingestion
code, and they're tagged `source_type = synthetic` from the moment they're generated, not
retroactively labeled.
