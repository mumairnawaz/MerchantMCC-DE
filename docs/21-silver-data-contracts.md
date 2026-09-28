# 21 — Silver Data Contracts

Status: **FROZEN DESIGN, NOT YET IMPLEMENTED**. This document is the implementation
contract for the Silver layer, produced from inspection of the actual repository
(all 7 Bronze ingestion modules, `common.py`, `validation.py`, `settings.py`,
`watermark.py`, `configs/sources.json`, and real Bronze data on disk). No Silver
code, Silver data, or dependency exists yet — see
[19-project-status.md](19-project-status.md) and [20-roadmap.md](20-roadmap.md).

Items marked **OPEN** are explicitly undecided and must not be silently resolved by
whoever implements this contract — they require a separate approval.

## 1. Bronze → Silver Architecture

```
                    BRONZE (existing, unchanged)
  mcc (CSV)  country (JSON)  iso_currency (CSV)  card_issuer (CSV)
  merchant_osm (JSON)  currency/Frankfurter (JSON)  gleif (JSON:API, paginated)
       │          │          │          │          │          │          │
       ▼          ▼          ▼          ▼          ▼          ▼          ▼
                    SILVER (this contract — not yet built)
  silver_mcc  silver_country  silver_iso_currency  silver_card_issuer
  silver_merchant  silver_fx_rate  silver_legal_entity
       │          │          │          │          │          │          │
       └──────────┴──────────┴──────────┴────┬─────┴──────────┴──────────┘
                                               ▼
                    (future, not this contract) WAREHOUSE / GOLD
```

Bronze is not modified by this contract or by any implementation of it. Silver reads
Bronze's raw files and Bronze's `metadata.json`; it never writes to `data/bronze/` or
touches `src/ingestion/`.

## 2. Silver Dataset Inventory

| Silver Dataset | Bronze Source | Grain (one row = ) | Primary/Business Key | Future Gold Object |
|---|---|---|---|---|
| `silver_mcc` | `mcc` | one MCC code | `mcc_code` | `dim_mcc` |
| `silver_country` | `country` | one country/territory | `country_code_alpha3` | `dim_country` |
| `silver_iso_currency` | `iso_currency` | one currency code (post-dedup) | `currency_code` | `dim_currency` |
| `silver_card_issuer` | `card_issuer` | one BIN range | `bin_range` | `dim_issuer` / `dim_card_issuer` (naming — OPEN, see §14) |
| `silver_merchant` | `merchant_osm` | one real OSM business node | `merchant_id` (derived `osm_type:osm_id`) | `dim_merchant` |
| `silver_fx_rate` | `currency` (Frankfurter) | one (rate_date, base_currency, quote_currency) | composite | `fact_fx_rate` |
| `silver_legal_entity` | `gleif` | one legal entity, identified by one LEI | `lei` | `dim_legal_entity` |

No dataset beyond these seven is justified by the current repository.

## 3. Complete Schemas / Data Contracts

### 3.1 silver_mcc

| Column | Type | Nullable | Source field | Transformation |
|---|---|---|---|---|
| mcc_code | CHAR(4) | NOT NULL (PK) | `mcc` | string, never int — leading zeros are real |
| description | VARCHAR | NOT NULL | `edited_description` | trim |
| description_combined | VARCHAR | NULL | `combined_description` | trim |
| description_usda | VARCHAR | NULL | `usda_description` | trim |
| description_irs | VARCHAR | NULL | `irs_description` | trim |
| irs_reportable | BOOLEAN | NULL | `irs_reportable` | "Yes"/"No" → bool |
| source_name | VARCHAR | NOT NULL | constant `"mcc"` | lineage |
| bronze_run_id | VARCHAR | NOT NULL | Bronze run directory name | lineage |
| ingestion_timestamp_utc | TIMESTAMP | NOT NULL | Bronze metadata | lineage |
| silver_processed_at_utc | TIMESTAMP | NOT NULL | DERIVED | lineage |
| silver_transform_version | VARCHAR | NOT NULL | DERIVED | lineage |

### 3.2 silver_country

| Column | Type | Nullable | Source field | Transformation |
|---|---|---|---|---|
| country_code_alpha3 | CHAR(3) | NOT NULL (PK) | `cca3` | uppercase |
| country_code_alpha2 | CHAR(2) | NOT NULL | `cca2` | uppercase |
| country_code_numeric | CHAR(3) | NULL | `ccn3` | some territories lack this |
| country_name | VARCHAR | NOT NULL | `name.common` | trim |
| country_official_name | VARCHAR | NULL | `name.official` | trim |
| region | VARCHAR | NULL | `region` | — |
| subregion | VARCHAR | NULL | `subregion` | — |
| capital | VARCHAR | NULL | `capital[0]` | first-of-array assumption; NULL if array empty |
| default_currency_code | CHAR(3) | NULL | DERIVED — first key of `currencies{}` | ambiguous for multi-currency territories, documented not resolved |
| is_independent | BOOLEAN | NULL | `independent` | — |
| is_un_member | BOOLEAN | NULL | `unMember` | — |
| source_name / bronze_run_id / ingestion_timestamp_utc / silver_processed_at_utc / silver_transform_version | — | NOT NULL | lineage | — |

### 3.3 silver_iso_currency

| Column | Type | Nullable | Source field | Transformation |
|---|---|---|---|---|
| currency_code | CHAR(3) | NOT NULL (PK, post-dedup) | `AlphabeticCode` | uppercase |
| currency_name | VARCHAR | NOT NULL | `Currency` | trim |
| currency_numeric_code | CHAR(3) | NULL | `NumericCode` | — |
| minor_unit | SMALLINT | NULL | `MinorUnit` | blank-handling rule — **OPEN**, see §14 |
| is_active | BOOLEAN | NOT NULL | DERIVED (`WithdrawalDate` blank → true) | — |
| withdrawal_date | DATE | NULL | `WithdrawalDate` | — |
| source_name / bronze_run_id / ingestion_timestamp_utc / silver_processed_at_utc / silver_transform_version | — | NOT NULL | lineage | — |

Raw Bronze has 449 rows (one per Entity/Currency pair); Silver deduplicates to 307
distinct `currency_code` rows (both figures independently confirmed against real
Bronze data).

### 3.4 silver_card_issuer

| Column | Type | Nullable | Source field | Transformation |
|---|---|---|---|---|
| bin_range | CHAR(6) | NOT NULL (PK) | `BIN` | string, verified all real values exactly 6 digits |
| card_brand | VARCHAR | NOT NULL | `Brand` | trim |
| card_type | VARCHAR | NULL | `Type` | — |
| card_category | VARCHAR | NULL | `Category` | — |
| issuer_name | VARCHAR | NULL | `Issuer` | 48.2% blank in real data; sentinel rule — **OPEN**, see §14 |
| has_known_issuer | BOOLEAN | NOT NULL | DERIVED (`Issuer` non-blank) | — |
| issuer_phone | VARCHAR | NULL | `IssuerPhone` | — |
| issuer_website | VARCHAR | NULL | `IssuerUrl` | — |
| issuer_country_alpha2 | CHAR(2) | NULL | `isoCode2` | uppercase |
| issuer_country_alpha3 | CHAR(3) | NULL | `isoCode3` | uppercase |
| issuer_country_name | VARCHAR | NULL | `CountryName` | trim |
| source_name / bronze_run_id / ingestion_timestamp_utc / silver_processed_at_utc / silver_transform_version | — | NOT NULL | lineage | — |

### 3.5 silver_merchant

| Column | Type | Nullable | Source field | Transformation |
|---|---|---|---|---|
| merchant_id | VARCHAR | NOT NULL (PK) | DERIVED | `f"{osm_type}:{osm_id}"` |
| osm_id | BIGINT | NOT NULL | `id` | — |
| osm_type | VARCHAR | NOT NULL | `type` | — |
| merchant_name | VARCHAR | NOT NULL | `tags.name` | **rows without a name are EXCLUDED, not nulled** |
| raw_shop_tag | VARCHAR | NULL | `tags.shop` | preserved raw — real data can be multi-valued (`"art;gift"`), not split |
| raw_amenity_tag | VARCHAR | NULL | `tags.amenity` | — |
| mcc_code | CHAR(4) | NULL | DERIVED | reserved — always NULL until an OSM→MCC crosswalk exists (not built) |
| latitude | DECIMAL | NOT NULL | `lat` | — |
| longitude | DECIMAL | NOT NULL | `lon` | — |
| address_city | VARCHAR | NULL | `tags."addr:city"` | — |
| address_postcode | VARCHAR | NULL | `tags."addr:postcode"` | — |
| address_street | VARCHAR | NULL | `tags."addr:street"` | — |
| phone | VARCHAR | NULL | `tags.phone` | — |
| website | VARCHAR | NULL | `tags.website` | — |
| opening_hours | VARCHAR | NULL | `tags.opening_hours` | — |
| country_code | CHAR(2) | NULL | DERIVED | **assumed from extract scope, not real per-record geocoding**; carry `country_code_source='assumed_from_extract_scope'` alongside |
| source_updated_timestamp | TIMESTAMP | NULL | `tags.timestamp` | only present when Bronze used `out meta;`; current real Bronze uses `out body;` and has none — must be nullable |
| record_hash | VARCHAR | NOT NULL | DERIVED | change-detection for upsert |
| source_name / bronze_run_id / ingestion_timestamp_utc / silver_processed_at_utc / silver_transform_version | — | NOT NULL | lineage | — |

Business exclusion rules applied before a row reaches Silver: `shop == "vacant"`
excluded; missing `tags.name` excluded. Both are documented project decisions, not
quarantine events (see §7).

### 3.6 silver_fx_rate

| Column | Type | Nullable | Source field | Transformation |
|---|---|---|---|---|
| rate_date | DATE | NOT NULL (PK part) | `date` (flat shape) or date-key inside `rates{}` (range shape) | see §14 note on the two Bronze shapes |
| base_currency | CHAR(3) | NOT NULL (PK part) | `base` | currently always `"EUR"`, stored as a real column, not hardcoded |
| quote_currency | CHAR(3) | NOT NULL (PK part) | key of the innermost `rates` dict | — |
| exchange_rate | DECIMAL(18,6) | NOT NULL | value | — |
| source_name / bronze_run_id / ingestion_timestamp_utc / silver_processed_at_utc / silver_transform_version | — | NOT NULL | lineage | — |

No optional columns — every field is structurally required by the grain.

### 3.7 silver_legal_entity

| Column | Type | Nullable | Source field | Transformation |
|---|---|---|---|---|
| lei | CHAR(20) | NOT NULL (PK) | `lei` | real ISO 17442 shape confirmed against all 10,000 real records |
| legal_name | VARCHAR | NOT NULL | `entity.legalName.name` | trim |
| legal_address_country | CHAR(2) | NULL | `entity.legalAddress.country` | real ISO alpha-2 — **distinct from jurisdiction** |
| legal_address_city | VARCHAR | NULL | `entity.legalAddress.city` | — |
| legal_address_region | VARCHAR | NULL | `entity.legalAddress.region` | often null in real data |
| legal_address_postal_code | VARCHAR | NULL | `entity.legalAddress.postalCode` | — |
| legal_address_lines | VARCHAR/ARRAY | NULL | `entity.legalAddress.addressLines` | — |
| hq_address_country | CHAR(2) | NULL | `entity.headquartersAddress.country` | — |
| hq_address_city | VARCHAR | NULL | `entity.headquartersAddress.city` | — |
| hq_address_region | VARCHAR | NULL | `entity.headquartersAddress.region` | — |
| hq_address_postal_code | VARCHAR | NULL | `entity.headquartersAddress.postalCode` | — |
| hq_address_lines | VARCHAR/ARRAY | NULL | `entity.headquartersAddress.addressLines` | — |
| legal_form_id | VARCHAR | NULL | `entity.legalForm.id` | — |
| entity_category | VARCHAR | NULL | `entity.category` | — |
| entity_status | VARCHAR | NOT NULL | `entity.status` | only `"ACTIVE"` observed; no value-set restriction imposed |
| entity_jurisdiction | VARCHAR | NOT NULL | `entity.jurisdiction` | **preserved EXACTLY as returned — see §8, non-negotiable** |
| entity_creation_date | DATE | NULL | `entity.creationDate` | — |
| registration_initial_registration_date | TIMESTAMP | NULL | `registration.initialRegistrationDate` | — |
| registration_last_update_date | TIMESTAMP | NOT NULL | `registration.lastUpdateDate` | source last-update timestamp; also drives the existing Bronze watermark |
| registration_status | VARCHAR | NOT NULL | `registration.status` | only `"ISSUED"` observed |
| registration_next_renewal_date | TIMESTAMP | NULL | `registration.nextRenewalDate` | — |
| bic | VARCHAR | NULL | `bic` | often null in real data |
| record_hash | VARCHAR | NOT NULL | DERIVED | change-detection for upsert |
| source_name / bronze_run_id / ingestion_timestamp_utc / silver_processed_at_utc / silver_transform_version | — | NOT NULL | lineage | — |

## 4. Normalization Rules

- Column names: `snake_case` throughout, regardless of Bronze's original casing.
- All code fields (`mcc_code`, `cca2/3`, `currency_code`, `bin_range`) kept as
  fixed-width strings, never numeric.
- Whitespace trimmed on all free-text fields.
- Empty/missing → SQL `NULL`, never `""` or an invented default, except where a
  sentinel is an explicit, approved business decision (none approved yet — see §14).
- Country/currency codes uppercased but **never normalized across code systems** —
  `GB-SCT` stays `GB-SCT`.
- Timestamps: UTC, ISO 8601, matching the project's existing convention throughout.
- Lineage columns are never overwritten or computed away.

## 5. Null Handling

Structural nulls (a field genuinely absent from the source) are preserved as `NULL`.
A `NULL` is never treated as equivalent to zero, empty string, or a default value.
`silver_card_issuer.issuer_name` and `silver_iso_currency.minor_unit` are the two
fields with a real, non-trivial null rate in production data (48.2% and a smaller
observed fraction respectively) — their handling rule is explicitly **OPEN** (§14),
not decided by this document.

## 6. Data-Quality Rules

**HARD FAIL** (row quarantined): malformed key pattern (`lei`, `mcc_code`,
`bin_range`, `cca2/3`, `currency_code` failing their regex); missing required field;
`latitude`/`longitude` out of valid range; `exchange_rate <= 0`.

**WARNING** (logged, row still loaded): missing optional field; record-count
deviation from the prior run beyond an expected range; referential-integrity
mismatch against another Silver dataset (e.g. an fx currency code not found in
`silver_iso_currency`).

**Bronze → Silver reconciliation** (hard fail on the reconciliation check itself,
independent of any single row): `Bronze record_count` (from Bronze's own
`metadata.json`) must equal `(Silver rows written) + (Silver rows quarantined) +
(Silver rows excluded by a documented business rule)`. Any gap means a record was
silently lost.

## 7. Quarantine Rules

```
data/quarantine/<dataset>/run_<timestamp>/rejected.jsonl
```
One JSON line per rejected record:
`{original_raw_record, error_reason, failing_check_name, source_bronze_run_id, source_record_identifier, rejected_at_utc}`.
Append-only, never overwritten. A quarantine file is created only when at least one
row is actually rejected. Business-rule *exclusions* (OSM `shop=vacant`, unnamed
nodes) are **not** quarantine events — they are documented, expected omissions,
counted separately in the reconciliation check (§6).

## 8. GLEIF Jurisdiction Preservation — Non-Negotiable

The real Step 6F initial run (10,000 records) returned:

| Value | Count |
|---|---|
| `GB` | 9,949 |
| `GB-SCT` | 34 |
| `GB-NIR` | 17 |

`silver_legal_entity.entity_jurisdiction` **must** preserve exactly whichever of
these (or any future value GLEIF returns) was in the Bronze record. No Silver
transformation may fold `GB-SCT` or `GB-NIR` into `GB`. A single `entity_jurisdiction`
column is used — **not** split into `jurisdiction_country_code` +
`jurisdiction_subdivision_code`, because GLEIF's real response has no such split to
derive it from; inventing a parsing rule (e.g. "first 2 characters") would be an
unverified assumption this project has already committed against (Step 6G). If a
derived country-only helper column is ever needed for joining to `silver_country`,
it must be **additive** (e.g. `jurisdiction_base_country_code`), never a replacement
— and remains **OPEN**, not decided here.

## 9. Incremental-Processing Semantics

| Dataset | Incremental? | Silver mode |
|---|---|---|
| mcc, country, iso_currency, card_issuer | No — Bronze has no watermark for these | Overwrite (full replace each refresh) |
| merchant_osm | Yes, via existing `data/watermarks/merchant_osm.json` | Upsert on `merchant_id`, latest `ingestion_timestamp_utc` wins |
| currency (fx_rate) | Yes, via existing `data/watermarks/currency.json` | Append (a past `rate_date` is immutable once published) |
| gleif | Yes, via existing `data/watermarks/gleif.json` | Upsert on `lei`, latest `registration_last_update_date` wins |

No CDC semantics are invented for the four static reference sources. Late-arriving
records for upsert datasets are accepted but never overwrite a row that already has
a newer source timestamp — last-write-by-source-timestamp, not last-write-by-
processing-order.

## 10. Idempotency Requirements

Re-running Silver against an unchanged Bronze run must be deterministic: identical
output except `silver_processed_at_utc`. Overwrite datasets achieve this trivially
(full replace). Upsert datasets achieve this by keying on their business key and
merging by `source_updated_timestamp`/`bronze_run_id` comparison, not by
processing order. No transformation may depend on wall-clock time, random values,
or non-reproducible external state beyond the lineage timestamp itself.

## 11. Lineage Columns

| Field | Applied to | Why |
|---|---|---|
| `source_name` | All 7 | Which Bronze source produced this row |
| `bronze_run_id` | All 7 | Which ingestion run produced it |
| `ingestion_timestamp_utc` | All 7 | When Bronze fetched it |
| `silver_processed_at_utc` | All 7 | When Silver transformed it |
| `silver_transform_version` | All 7 | Which transformation code version produced it |
| `source_updated_timestamp` | `silver_merchant` (conditional), `silver_fx_rate` (=`rate_date`), `silver_legal_entity` (=`registration_last_update_date`) only | The 4 reference sources have no such concept in their source at all |
| `record_hash` | `silver_merchant`, `silver_legal_entity` only | Only these two are upserted repeatedly across runs |

`source_record_id` is deliberately **not** added as a separate column anywhere — for
every dataset the natural source identifier already is the Silver primary key.

## 12. Storage Layout

```
data/
  bronze/<source>/run_<timestamp>/...          (existing, unchanged)
  watermarks/<source>.json                     (existing, unchanged)
  silver/
    mcc/                 single Parquet file, overwritten each run
    country/               single Parquet file, overwritten each run
    iso_currency/           single Parquet file, overwritten each run
    card_issuer/            single Parquet file, overwritten each run
    merchant/                single Parquet file, upsert/merge in place
    fx_rate/                  partitioned by rate_date, append-only
    legal_entity/              single Parquet file, upsert/merge in place
  quarantine/<dataset>/run_<timestamp>/rejected.jsonl
```
Reference datasets are always fully replaced → single-file snapshot. `merchant`/
`legal_entity` are real-world entities that mutate → upsert semantics keyed on their
natural key. `fx_rate` is the only genuinely time-series, append-accumulating
dataset → the only one partitioned.

## 13. Future Technology Integration Points

**dbt**: enters at Silver → Gold, once Silver Parquet datasets are loaded into a real
queryable warehouse (Postgres or DuckDB — warehouse choice itself OPEN). Not
introduced here; nothing in a warehouse yet for it to transform.

**Airflow**: orchestrates, per source, `Bronze ingest → Silver transform → DQ gate`.
Bronze fetch tasks get network retry/backoff; Silver transform tasks retry only on
environment/code error, never on a data-validity failure (that is a quarantine
outcome, not a task failure); the DQ gate blocks any downstream Gold task on a hard
reconciliation mismatch. Not built here.

**PySpark**: not justified for any of the 7 current Silver datasets — largest volume
(card_issuer, 374,788 rows) is trivial for single-machine Python. PySpark's real
entry point is a future, genuinely large/distributed synthetic transaction-event
workload, entirely separate from this contract.

**Kafka / Debezium / Spark Structured Streaming**: a separate path —
`Synthetic PostgreSQL OLTP → Debezium CDC → Kafka → Spark Structured Streaming →
Silver EVENT stream (distinct from this batch reference-data Silver) → Gold`. Never
merged into the pipeline this contract describes.

## 14. Open Decisions (explicitly not resolved by this document)

1. `dim_issuer` vs `dim_card_issuer` naming (future Gold, doesn't block Silver).
2. `entity_jurisdiction → silver_country` join mechanism (no automatic join built).
3. ISO currency `Entity` — dropped vs. moved to a bridge table.
4. `card_issuer.issuer_name` blank handling — raw-null vs. `'Unknown'` sentinel.
5. `iso_currency.minor_unit` blank handling — default-to-2-unless-withdrawn vs. leave null.
6. Bronze/code shape duality: Frankfurter (flat vs. range) and OSM (`out body` vs.
   `out meta`) — Silver must handle both; not a defect, but must not be assumed away.
7. Silver warehouse target (Postgres vs. DuckDB) for the eventual dbt entry point.
8. PyArrow as a new project dependency — requires explicit approval before installation.
