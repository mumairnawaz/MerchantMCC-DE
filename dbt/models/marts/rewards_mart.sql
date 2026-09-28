-- KPI domain F (rewards/CLO, docs/27 §19). Grain: one row per
-- (offer_key, campaign_key, qualification_status). Documented in S14
-- (docs/27 §20) but not built there — implemented here.

select
    offer_key,
    offer_id,
    offer_type,
    campaign_key,
    campaign_id,
    campaign_name,
    qualification_status,
    count(*) as reward_event_count,
    sum(reward_amount) as reward_amount_total
from {{ ref('int_rewards_enriched') }}
group by offer_key, offer_id, offer_type, campaign_key, campaign_id, campaign_name, qualification_status
