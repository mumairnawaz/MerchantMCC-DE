-- Grain: one row per (reward_id, kafka_offset) — unchanged from
-- fact_rewards (docs/27 §7: reward_id alone is not unique in real data,
-- see the S10 idempotency-test-fixture finding).

with rewards as (
    select * from {{ ref('stg_fact_rewards') }}
),

dates as (
    select * from {{ ref('stg_dim_date') }}
),

currencies as (
    select * from {{ ref('stg_dim_currency') }}
),

offers as (
    select * from {{ ref('stg_dim_offer') }}
),

campaigns as (
    select * from {{ ref('stg_dim_campaign') }}
)

select
    rw.reward_id,
    rw.kafka_offset,
    rw.transaction_id,
    rw.date_key,
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    rw.currency_key,
    cu.currency_code,
    rw.offer_key,
    o.offer_id,
    o.offer_type,
    o.campaign_key,
    c.campaign_id,
    c.campaign_name,
    rw.qualification_status,
    rw.reward_amount
from rewards rw
left join dates d on rw.date_key = d.date_key
left join currencies cu on rw.currency_key = cu.currency_key
left join offers o on rw.offer_key = o.offer_key
left join campaigns c on o.campaign_key = c.campaign_key
