-- KPI domain E (reconciliation, docs/27 §19). Grain: one row per
-- (reconciled_date, match_status). Documented in S14 (docs/27 §20) but not
-- built there — implemented here. sum(variance) is kept (it is a real,
-- additive column) but — per the same caveat docs/27 §6 states for
-- fact_reconciliation itself — it reads as a net over/under figure, not a
-- magnitude-of-error metric; not relabeled or reinterpreted here.

select
    reconciled_date,
    year,
    month,
    month_name,
    match_status,
    count(*) as reconciliation_count,
    sum(expected_amount) as expected_amount_total,
    sum(actual_amount) as actual_amount_total,
    sum(variance) as variance_total
from {{ ref('int_reconciliation_enriched') }}
group by reconciled_date, year, month, month_name, match_status
