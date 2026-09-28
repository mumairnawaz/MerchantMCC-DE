-- Thin 1:1 pass-through over the S14 Gold table. Deliberately no
-- transformation: Gold data is already typed/validated (docs/27). This
-- model exists to establish dbt's own lineage/contract boundary (standard
-- dbt convention), not to re-transform anything.
select *
from {{ source('gold', 'dim_date') }}
