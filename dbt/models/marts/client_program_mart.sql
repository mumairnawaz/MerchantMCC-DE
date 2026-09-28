-- KPI domain C (client/program intelligence, docs/27 §19). Grain: one row
-- per (client_key, program_key). Documented in S14 (docs/27 §20) but not
-- built there — implemented here as dbt's first new mart.

select
    client_key,
    client_id,
    client_legal_name,
    program_key,
    program_id,
    program_name,
    program_type,
    count(*) as transaction_count,
    sum(case when transaction_status = 'APPROVED' then amount else 0 end) as approved_amount_total,
    sum(case when transaction_status = 'DECLINED' then 1 else 0 end) as declined_count
from {{ ref('int_transactions_enriched') }}
group by client_key, client_id, client_legal_name, program_key, program_id, program_name, program_type
