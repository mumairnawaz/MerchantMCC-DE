# 4. Real vs Reference vs Synthetic Data Architecture

```mermaid
flowchart TB
    subgraph REAL["REAL — live, verified"]
        R1["Merchant identity & location (OSM)"]
        R2["FX rates (Frankfurter)"]
        R3["Legal entities (GLEIF)"]
    end
    subgraph REFERENCE["REFERENCE — real, slow-changing"]
        F1["MCC classification"]
        F2["Country reference"]
        F3["ISO 4217 currency metadata"]
        F4["BIN/IIN issuer reference"]
    end
    subgraph SYNTHETIC["SYNTHETIC — generated, never real"]
        S1["Programs / campaigns / offers"]
        S2["Tokenized cardholders"]
        S3["Transactions"]
        S4["Authorization / clearing / settlement"]
        S5["Reconciliation breaks"]
        S6["Rewards / CLO events"]
    end

    REAL -. "enriches" .-> SYNTHETIC
    REFERENCE -. "classifies / enriches" .-> SYNTHETIC
    REFERENCE -. "classifies" .-> REAL
```

No synthetic record is ever labeled, formatted, or delivered in a way that implies it is a
real transaction, or that this project has access to real cardholder or private banking
data. This boundary is enforced with a `source_type` field carried from Bronze through to
any delivered output.
