# DuckDB Evidence

## GUI availability

No standalone DuckDB CLI binary or GUI is installed in this environment (only the Python
`duckdb` package is used, invoked programmatically) — checked directly (`Get-Command
duckdb` → not found). Rather than install a new tool to produce a screenshot, this is
documented honestly and real query evidence is provided instead.

## Real evidence (captured 2026-09-28, `data/gold/gold.duckdb`, read-only)

**25 real tables/views** — the Gold star schema (`main` schema) plus the dbt-built marts
(`marts` schema):
```
main.dim_campaign        main.fact_reconciliation     marts.client_program_mart
main.dim_card_issuer     main.fact_rewards            marts.merchant_mart
main.dim_card_token      main.fact_settlements        marts.reconciliation_mart
main.dim_cardholder      main.fact_transaction_events  marts.rewards_mart
main.dim_client          main.fact_transactions        marts.settlement_mart
main.dim_country         main.merchant_mart            marts.transaction_mart
main.dim_currency        main.transaction_mart
main.dim_date            ...(13 dimensions total)
main.dim_legal_entity
main.dim_mcc
main.dim_merchant
main.dim_offer
main.dim_program
```

**A real control-total query, run just now:**
```sql
SELECT SUM(amount) FROM fact_transactions WHERE transaction_status='APPROVED';       -- 435106.1600
SELECT SUM(settlement_amount) FROM fact_settlements;                                  -- 435106.1600
SELECT SUM(expected_amount) FROM fact_reconciliation;                                 -- 435106.1600
```

**Caption**: *"DuckDB — the real Gold star schema (25 tables/views across `main` and
`marts` schemas) and the £435,106.16 control-total identity, queried live."*

## Manual screenshot checklist (if a DuckDB GUI is added later)

- [ ] Table list for `data/gold/gold.duckdb`
- [ ] The control-total query above, run interactively
