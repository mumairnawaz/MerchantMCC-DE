-- Singular dbt test: the same control-total identity src/gold/reconciliation.py
-- verifies in Python (docs/27 §17) — approved transaction total == settlement
-- total == reconciliation expected total — re-verified here from the dbt
-- marts themselves. A dbt test passes when this query returns ZERO rows;
-- this SELECT returns a row (fails the test) only if the three totals
-- disagree by more than a whole-penny rounding tolerance.

with totals as (
    select
        (select sum(amount_total) from {{ ref('transaction_mart') }} where transaction_status = 'APPROVED') as approved_transaction_total,
        (select sum(settlement_amount_total) from {{ ref('settlement_mart') }}) as settlement_total,
        (select sum(expected_amount_total) from {{ ref('reconciliation_mart') }}) as reconciliation_expected_total
)

select *
from totals
where round(approved_transaction_total, 2) != round(settlement_total, 2)
   or round(approved_transaction_total, 2) != round(reconciliation_expected_total, 2)
