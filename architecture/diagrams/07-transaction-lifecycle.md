# 7. Fintech Transaction Lifecycle (Synthetic)

Status: PLANNED — no transaction generator exists yet. This diagram documents the intended
synthetic event model. **All data in this flow is synthetic** — no real cardholder, PAN, or
transaction data is involved at any stage.

```mermaid
sequenceDiagram
    participant C as Synthetic Cardholder
    participant M as Merchant (real, via OSM)
    participant A as Authorization
    participant CL as Clearing
    participant ST as Settlement
    participant R as Reconciliation
    participant W as Rewards / CLO

    C->>M: synthetic transaction initiated
    M->>A: authorization request (synthetic)
    A-->>M: approved / declined (synthetic)
    A->>CL: clearing event (synthetic)
    CL->>ST: settlement event (synthetic)
    ST->>R: compared against authorization + clearing
    R-->>R: break detected or matched (derived)
    A->>W: MCC checked against offer eligibility (derived)
    W-->>C: reward/CLO event if matched (derived)
```

The transaction touches one real anchor point — the merchant, via its real OSM identity and
real MCC classification — everything else in this sequence is synthetic or derived from
synthetic data.
