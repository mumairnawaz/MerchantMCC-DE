-- Grain: one row per settlement_id (unchanged from fact_settlements).

with settlements as (
    select * from {{ ref('stg_fact_settlements') }}
),

dates as (
    select * from {{ ref('stg_dim_date') }}
),

currencies as (
    select * from {{ ref('stg_dim_currency') }}
)

select
    s.settlement_id,
    s.transaction_id,
    s.settlement_batch_id,
    s.date_key,
    d.full_date as settlement_date,
    d.year,
    d.month,
    d.month_name,
    s.currency_key,
    cu.currency_code,
    s.settlement_amount,
    s.fee_amount,
    s.net_amount
from settlements s
left join dates d on s.date_key = d.date_key
left join currencies cu on s.currency_key = cu.currency_key
