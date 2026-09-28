-- KPI domain D (settlement, docs/27 §19). Grain: one row per
-- (settlement_batch_id, currency_code). Documented in S14 (docs/27 §20) but
-- not built there — implemented here.

select
    settlement_batch_id,
    currency_code,
    min(settlement_date) as batch_start_date,
    max(settlement_date) as batch_end_date,
    count(*) as settlement_count,
    sum(settlement_amount) as settlement_amount_total,
    sum(fee_amount) as fee_amount_total,
    sum(net_amount) as net_amount_total
from {{ ref('int_settlements_enriched') }}
group by settlement_batch_id, currency_code
