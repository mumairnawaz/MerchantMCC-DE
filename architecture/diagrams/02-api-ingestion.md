# 2. API Ingestion

```mermaid
flowchart TB
    subgraph OSM_S["OpenStreetMap Overpass — live"]
        O1["POST overpass-api.de/api/interpreter"]
        O2["No auth · User-Agent required"]
        O3["Weekly · newer: filter, watermark-driven"]
    end
    subgraph FX_S["Frankfurter — live"]
        F1["GET api.frankfurter.dev/v1/*"]
        F2["No auth"]
        F3["Daily · date-range incremental, watermark-driven"]
    end
    subgraph GLEIF_S["GLEIF LEI — live"]
        G1["GET api.gleif.org/api/v1/lei-records"]
        G2["No auth, no key, no account"]
        G3["Daily · lastUpdateDate incremental + pagination"]
    end
    subgraph REF_S["Reference data — ad hoc snapshot"]
        R1["MCC codes"]
        R2["Country reference"]
        R3["ISO 4217 currency"]
        R4["BIN/IIN issuer"]
    end

    O1 --> O2 --> O3 --> ABronze
    F1 --> F2 --> F3 --> ABronze
    G1 --> G2 --> G3 --> ABronze
    R1 --> ABronze
    R2 --> ABronze
    R3 --> ABronze
    R4 --> ABronze

    ABronze[("API Bronze<br/>raw, timestamped, per-source watermark")] --> DQ{"Validation"}
    DQ -->|pass| ASilver[("API Silver<br/>conformed")]
    DQ -->|fail| Q[("Quarantine")]
    ASilver --> GOLD[("Gold dimensions")]
```

All three live sources are independently verified against their real production
endpoints, including two real failures encountered and resolved during development (OSM:
missing `User-Agent` → HTTP 406; OSM's incremental query, too-short timeout → HTTP 504).
Each source has its own persisted watermark file, so a scheduled rerun only requests
new/changed records — never a full reload — and every record that fails validation is
routed to quarantine with a recorded reason, never silently dropped or repaired.

Orchestrated by Airflow's `merchantmcc_api_pipeline` DAG (`0 2 * * *`) — see
[`06-airflow-orchestration.md`](06-airflow-orchestration.md). Full source-by-source
detail: [`docs/06-data-source-catalog.md`](../../docs/06-data-source-catalog.md),
[`docs/13-incremental-ingestion.md`](../../docs/13-incremental-ingestion.md).
