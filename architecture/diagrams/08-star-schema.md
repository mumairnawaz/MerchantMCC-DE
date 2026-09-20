# 8. Conceptual Warehouse / Star Schema

Status: PROPOSED — no warehouse tables exist yet. Full field-by-field source classification
(real/reference/synthetic/derived) is in [`docs/11-data-model.md`](../../docs/11-data-model.md).

```mermaid
erDiagram
    dim_merchant ||--o{ fact_transaction : merchant_id
    dim_mcc ||--o{ fact_transaction : mcc_code
    dim_currency ||--o{ fact_transaction : currency_code
    dim_date ||--o{ fact_transaction : transaction_date
    dim_country ||--o{ dim_merchant : country_code
    dim_client ||--o{ dim_program : client_id
    dim_program ||--o{ dim_campaign : program_id
    dim_campaign ||--o{ dim_offer : campaign_id
    dim_mcc ||--o{ dim_offer : eligible_mcc_code
    dim_card_issuer ||--o{ fact_transaction : bin_range
    fact_transaction ||--o| fact_authorization : transaction_id
    fact_transaction ||--o| fact_clearing : transaction_id
    fact_transaction ||--o| fact_settlement : transaction_id
    fact_transaction ||--o{ fact_reconciliation : transaction_id
    fact_transaction ||--o{ fact_offer_interaction : transaction_id
    dim_offer ||--o{ fact_offer_interaction : offer_id
    fact_offer_interaction ||--o| fact_reward : interaction_id
```
