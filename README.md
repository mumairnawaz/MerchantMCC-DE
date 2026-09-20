# MerchantMCC DE

**MerchantMCC DE — Merchant, MCC & Transaction Data Engineering Platform**

A portfolio-grade Data Engineering platform simulating the merchant, MCC (Merchant
Category Code), and transaction data pipelines found in fintech / issuer-processing
environments — built entirely on public data, free/open-source tooling, and clearly
labelled synthetic data, with no confidential, proprietary, or employer-owned systems,
schemas, or data involved.

## Data model

- **Real public reference data**: MCC classification lists, merchant/business listings
  (OpenStreetMap), country and currency reference data.
- **Synthetic transaction data**: authorization, clearing, settlement, rewards/CLO
  events, generated in Python and referencing the real reference data above.
  Synthetic records are always explicitly labelled as synthetic — never presented as
  real card-network transactions, because no such data is publicly available.

## Architecture

Source → Ingestion → Bronze → Data Quality → Silver → Transformation →
Warehouse (PostgreSQL) → Gold Marts → Output (Power BI + CSV/Parquet) → Stakeholders

See [`docs/`](docs/) for phase-by-phase architecture notes.

## Local development

Start the isolated PostgreSQL service (container `merchantmcc_postgres`, volume
`merchantmcc_postgres_data`, network `merchantmcc_net`, host port `55432`):

```
docker compose --env-file .env -f docker/docker-compose.yml up -d
```

`--env-file .env` is required — Compose's own variable substitution (used in the
healthcheck and port mapping) only looks in the project directory's `.env`, which is
separate from the `env_file: ../.env` the Postgres service uses to load its own
runtime environment. Omitting it leaves the container running but reports it
`unhealthy`.

To stop (data persists in the named volume):

```
docker compose --env-file .env -f docker/docker-compose.yml down
```

To stop and remove the data volume:

```
docker compose --env-file .env -f docker/docker-compose.yml down -v
```

Copy `.env.example` to `.env` before first run (`.env` is gitignored).

## Status

Phase 1 — project foundation and isolated local infrastructure. No business
schemas, pipelines, or data have been created yet.
