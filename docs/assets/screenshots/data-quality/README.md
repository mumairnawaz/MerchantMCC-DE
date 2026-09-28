# Data Quality Evidence

## Real evidence (verified 2026-09-28)

**Spark — deliberately injected defects, real reconciled outcome:**
```
1,010 Bronze records (synthetic generator, seeded, defects deliberately injected:
missing IDs, malformed timestamps, invalid statuses, duplicate transaction IDs)
        ↓
  875 valid  → Spark Silver
  135 rejected → routed to rejection output, each with a recorded reason
  ─────────────────────────
  1,010 total — fully reconciled, zero unaccounted records
```

**Gold / dbt — reconciliation identity, independently re-verified at two layers:**
```
Approved transaction amount (fact_transactions)  = £435,106.16
Settlement amount (fact_settlements)             = £435,106.16
Reconciliation expected amount (fact_reconciliation) = £435,106.16
```
Re-proven again from the dbt marts themselves by the `assert_control_total_identity`
singular test (73/73 dbt tests passing — see [`../dbt/`](../dbt/)).

**Reconciliation exceptions exist and are visible, not hidden**: the reconciliation mart's
own `actual_amount_total` (£434,846.91) legitimately differs from `expected_amount_total`
(£435,106.16) by −£259.25 — real, retained variance, not reconciled away.

**Caption**: *"Data Quality — Spark Bronze-to-Silver reconciliation showing 875 valid and
135 rejected records, fully accounted for."*

## Manual screenshot checklist

- [ ] A DuckDB query result showing the three £435,106.16 figures side by side
- [ ] The Spark Bronze→Silver job's console output showing the 875/135 split
- [ ] A quarantine directory listing (e.g. `data/quarantine/iso_currency/`) showing real
      rejected-record folders with timestamps

No credentials involved in any of this evidence — it is entirely business/reconciliation
data, all synthetic or from public reference sources.
