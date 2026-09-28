# PostgreSQL / OLTP Evidence

Real screenshots from pgAdmin 4 (v9.17), connected to the live `merchantmcc` database,
captured 2026-09-28.

### Schema browser + live activity dashboard

![PostgreSQL schema and activity dashboard](01-postgresql-schema.png)

The 11 real tables in the `finpay` schema (`campaigns`, `card_tokens`, `cardholders`,
`clients`, `offers`, `programs`, `reconciliation`, `reward_events`, `settlements`,
`transaction_events`, `transactions`), alongside pgAdmin's live server activity dashboard
— session count, transactions/sec, and tuple/block I/O, all reflecting real database
activity at capture time (visible spikes correspond to CDC/Airflow pipeline runs reading
and writing against this database).

### Analytical query against real data

![PostgreSQL analytical query](02-postgresql-analytical-query.png)

A real ad hoc SQL query run directly in pgAdmin's Query Tool against
`finpay.transactions` — merchant-level transaction counts, approved/declined splits, and
amounts via `FILTER (WHERE ...)` aggregates — demonstrating the schema holds real,
queryable relational data, not just empty table definitions.

## CLI evidence (real, captured 2026-09-28 — no credentials, no PII)

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

## Reproducing this evidence

```bash
docker exec merchantmcc_postgres psql -U <user> -d merchantmcc -c "\dt finpay.*"
```
(row counts via a simple `SELECT COUNT(*)` per table — see `docs/23-oltp-postgresql-design.md`)

Neither screenshot shows `POSTGRES_PASSWORD` or any connection string containing a
credential — pgAdmin's saved-connection UI does not display the password on screen.
