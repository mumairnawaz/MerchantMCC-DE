# 04 — Project Scope

## In scope

- Real external data ingestion from three verified live APIs (OpenStreetMap Overpass,
  Frankfurter, GLEIF)
- Real reference/master data (MCC, country, ISO 4217, BIN/IIN)
- Synthetic financial event generation (programs, campaigns, offers, tokenized cardholders,
  transactions, authorization/clearing/settlement, reconciliation, rewards/CLO)
- Bronze → Silver → Gold medallion architecture with data quality gating
- Incremental, watermark-driven ingestion
- Dimensional warehouse modeling
- Orchestration (Apache Airflow)
- Dual-channel delivery: Power BI (internal) and governed CSV/Parquet (external
  stakeholders)

## Explicitly out of scope

- Any real cardholder data, real PANs, real customer transactions, or confidential
  employer/company information — never, under any circumstance
- Real-time/sub-second processing claims — all live sources are daily/weekly batch,
  described precisely as such
- Paid APIs or services as required (non-optional) dependencies
- Presenting synthetic data as if it were real banking data

## Zero-cost principle

The project prioritizes free public APIs, open-source technologies, free developer tiers,
and local Docker infrastructure — no unnecessary paid services, no credit-card-dependent
APIs where avoidable. If a future component genuinely requires payment, that will be
identified explicitly before implementation, never assumed or hidden.
