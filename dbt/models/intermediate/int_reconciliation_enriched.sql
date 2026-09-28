-- Grain: one row per reconciliation_id (unchanged from fact_reconciliation).
-- reconciliation carries no currency of its own in CDC Silver (docs/26) —
-- joined through settlement_id to get settlement_batch_id/currency_code,
-- the same real relationship fact_reconciliation already encodes as a
-- degenerate join key (docs/27 §6).

with reconciliation as (
    select * from {{ ref('stg_fact_reconciliation') }}
),

dates as (
    select * from {{ ref('stg_dim_date') }}
),

settlements as (
    select * from {{ ref('int_settlements_enriched') }}
)

select
    r.reconciliation_id,
    r.settlement_id,
    s.settlement_batch_id,
    s.currency_code,
    r.date_key,
    d.full_date as reconciled_date,
    d.year,
    d.month,
    d.month_name,
    r.match_status,
    r.expected_amount,
    r.actual_amount,
    r.variance
from reconciliation r
left join dates d on r.date_key = d.date_key
left join settlements s on r.settlement_id = s.settlement_id
