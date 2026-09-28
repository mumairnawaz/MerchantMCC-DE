# 4. Gold Warehouse

The convergence point for both source paths — a DuckDB-backed star schema.

```mermaid
erDiagram
    dim_merchant ||--o{ fact_transactions : merchant_key
    dim_mcc ||--o{ fact_transactions : mcc_key
    dim_currency ||--o{ fact_transactions : currency_key
    dim_date ||--o{ fact_transactions : date_key
    dim_country ||--o{ dim_merchant : country_key
    dim_client ||--o{ dim_program : client_key
    dim_program ||--o{ dim_campaign : program_key
    dim_campaign ||--o{ dim_offer : campaign_key
    dim_mcc ||--o{ dim_offer : eligible_mcc_key
    dim_card_issuer ||--o{ fact_transactions : card_token_key
    dim_legal_entity ||--o{ dim_client : legal_entity_key
    fact_transactions ||--o| fact_settlements : transaction_id
    fact_settlements ||--o{ fact_reconciliation : settlement_id
    fact_transactions ||--o{ fact_transaction_events : transaction_id
    dim_offer ||--o{ fact_rewards : offer_key
```

- **API-sourced dimensions**: `dim_merchant`, `dim_mcc`, `dim_country`, `dim_currency`,
  `dim_card_issuer`, `dim_legal_entity`.
- **CDC-sourced dimensions/facts**: `dim_client`, `dim_program`, `dim_campaign`,
  `dim_offer`, `dim_cardholder`, `dim_card_token`, `fact_transactions`,
  `fact_transaction_events`, `fact_settlements`, `fact_reconciliation`, `fact_rewards`.
- Deterministic surrogate keys, a conformed `dim_date`, and an explicit **unknown-member**
  convention (surrogate key `-1`) so a fact whose dimension lookup misses is never
  silently dropped.

**Live values, queried directly from Gold** (not hardcoded anywhere in code or docs):

| Metric | Value |
|---|---|
| `fact_transactions` row count | 4,000 |
| Total transaction amount (all statuses) | £501,672.07 |
| Approved amount (control total) | £435,106.16 |
| Settlement amount | £435,106.16 |
| Reconciliation expected amount | £435,106.16 |
| Reconciliation actual amount | £434,846.91 |
| Reconciliation variance | −£259.25 |

Full detail: [`docs/27-gold-olap-design.md`](../../docs/27-gold-olap-design.md).
