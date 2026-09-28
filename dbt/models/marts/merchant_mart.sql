-- KPI domain B (merchant intelligence, docs/27 §19). Grain: one row per
-- merchant_key. Re-implements the same logic S14 originally created as a
-- raw SQL view in src/gold/pipeline.py — dbt now owns this definition (S15
-- migrates mart ownership from ad hoc Python SQL to dbt; the Python-side
-- view creation was removed to avoid two competing definitions of the same
-- name — see docs/28 §"Mart Ownership Migration").

select
    merchant_key,
    merchant_id,
    merchant_name,
    mcc_code,
    mcc_description,
    count(*) as transaction_count,
    sum(case when transaction_status = 'APPROVED' then amount else 0 end) as approved_amount_total,
    sum(case when transaction_status = 'DECLINED' then 1 else 0 end) as declined_count
from {{ ref('int_transactions_enriched') }}
group by merchant_key, merchant_id, merchant_name, mcc_code, mcc_description
