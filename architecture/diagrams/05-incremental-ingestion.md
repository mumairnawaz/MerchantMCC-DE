# 5. Incremental Ingestion Flow

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant B as Bronze / Watermark Store
    participant A as Live API

    Note over S,A: INITIAL LOAD
    S->>A: full scoped extraction
    A-->>S: complete initial dataset
    S->>B: write Bronze run, record watermark

    Note over S,A: SUBSEQUENT RUN
    S->>B: read previous watermark
    S->>A: request new/changed records only
    A-->>S: incremental result set
    S->>S: validate
    S->>B: write new Bronze run, update watermark
```

Applies identically to all three live sources (OSM, Frankfurter, GLEIF), each with its own
watermark field and query syntax — see [`docs/13-incremental-ingestion.md`](../../docs/13-incremental-ingestion.md)
for the source-specific mechanisms, all independently verified live.
