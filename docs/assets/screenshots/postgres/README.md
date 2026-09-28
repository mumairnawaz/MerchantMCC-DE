# PostgreSQL / OLTP Evidence

## CLI evidence (real, captured 2026-09-28 — no credentials, no PII)

**11 real tables in the `finpay` schema:**
```
campaigns · card_tokens · cardholders · clients · offers · programs ·
reconciliation · reward_events · settlements · transaction_events · transactions
```

**Real row counts (synthetic data — no real cardholder or payment data exists in this
project):**
```
clients               12
programs               8
campaigns             12
offers                20
cardholders          200
card_tokens           261
transactions        4,000
transaction_events  7,548
settlements         3,464
reconciliation      3,464
reward_events       2,676
```

**Caption**: *"PostgreSQL — the 11-table synthetic fintech OLTP schema and real row
counts feeding Debezium's CDC stream."*

## Reproducing this evidence

```bash
docker exec merchantmcc_postgres psql -U <user> -d merchantmcc -c "\dt finpay.*"
```
(row counts via a simple `SELECT COUNT(*)` per table — see `docs/23-oltp-postgresql-design.md`)

## Manual screenshot checklist

- [ ] `psql` or a DB client connected to the `finpay` schema, showing the table list above
- [ ] A `SELECT COUNT(*)` result for `transactions` (4,000) — no need to show row-level
      content, since these are synthetic records with no real value beyond the schema
      demonstration

**Never include a screenshot showing `POSTGRES_PASSWORD` or any connection string with a
credential in it** — connect using a client that doesn't display the password on screen,
or crop it out before saving.
