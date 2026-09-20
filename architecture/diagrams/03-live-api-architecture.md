# 3. Live API Source Architecture

```mermaid
flowchart TB
    subgraph OSM_S["OpenStreetMap Overpass"]
        O1["POST overpass-api.de/api/interpreter"]
        O2["No auth · User-Agent required"]
        O3["Weekly · newer: filter incremental"]
    end
    subgraph FX_S["Frankfurter"]
        F1["GET api.frankfurter.dev/v1/*"]
        F2["No auth"]
        F3["Daily · date-range incremental"]
    end
    subgraph GLEIF_S["GLEIF LEI"]
        G1["GET api.gleif.org/api/v1/lei-records"]
        G2["No auth, no key, no account"]
        G3["Daily · lastUpdateDate incremental"]
    end

    O1 --> O2 --> O3 --> BRONZE
    F1 --> F2 --> F3 --> BRONZE
    G1 --> G2 --> G3 --> BRONZE
    BRONZE[("Bronze")]
```

All three mechanisms shown here were independently verified against the live production
endpoints, including two real failures encountered and resolved (missing User-Agent on
OSM → HTTP 406; too-short timeout on OSM's incremental filter → HTTP 504). Full detail:
[`docs/13-incremental-ingestion.md`](../../docs/13-incremental-ingestion.md).
