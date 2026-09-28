-- Grain: one row per transaction_id (unchanged from fact_transactions,
-- docs/27 §6). Joins in the dimension attributes the marts layer actually
-- needs, using LEFT JOIN throughout — every FK on fact_transactions already
-- resolves to a real dimension row or the UNKNOWN_KEY (-1) member (S14
-- unknown-member convention, docs/27 §10), so a LEFT JOIN never silently
-- drops a transaction; it only ever produces the UNKNOWN dimension's own
-- attribute values (e.g. "Unknown Merchant") for the rows that were
-- already unresolved upstream.

with transactions as (
    select * from {{ ref('stg_fact_transactions') }}
),

dates as (
    select * from {{ ref('stg_dim_date') }}
),

merchants as (
    select * from {{ ref('stg_dim_merchant') }}
),

mccs as (
    select * from {{ ref('stg_dim_mcc') }}
),

currencies as (
    select * from {{ ref('stg_dim_currency') }}
),

programs as (
    select * from {{ ref('stg_dim_program') }}
),

clients as (
    select * from {{ ref('stg_dim_client') }}
)

select
    t.transaction_id,
    t.date_key,
    d.full_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.day_name,
    d.is_weekend,
    t.merchant_key,
    m.merchant_id,
    m.merchant_name,
    t.mcc_key,
    mc.mcc_code,
    mc.description as mcc_description,
    t.currency_key,
    cu.currency_code,
    t.program_key,
    p.program_id,
    p.program_name,
    p.program_type,
    t.client_key,
    cl.client_id,
    cl.legal_name as client_legal_name,
    t.card_token_key,
    t.cardholder_key,
    t.country_key,
    t.transaction_timestamp,
    t.amount,
    t.mcc_confidence,
    t.transaction_status,
    t.decline_reason,
    t.auth_code
from transactions t
left join dates d on t.date_key = d.date_key
left join merchants m on t.merchant_key = m.merchant_key
left join mccs mc on t.mcc_key = mc.mcc_key
left join currencies cu on t.currency_key = cu.currency_key
left join programs p on t.program_key = p.program_key
left join clients cl on t.client_key = cl.client_key
