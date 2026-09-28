-- KPI domain A (transaction/payment intelligence, docs/27 §19). Grain: one
-- row per (full_date, currency_code, transaction_status). Re-implements the
-- same logic S14 originally created as a raw SQL view (see merchant_mart.sql
-- header for the mart-ownership migration note).

select
    full_date,
    year,
    month,
    month_name,
    currency_code,
    transaction_status,
    count(*) as transaction_count,
    sum(amount) as amount_total
from {{ ref('int_transactions_enriched') }}
group by full_date, year, month, month_name, currency_code, transaction_status
