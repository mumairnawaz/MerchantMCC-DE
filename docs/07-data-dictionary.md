# 07 — Data Dictionary

Status: **PLANNED DATA CONTRACT** — these field lists define the intended Silver-layer
shape. They have not yet been implemented as tables, and should not be read as a claim
that ingestion code producing them exists in this repository yet (see
[19-project-status.md](19-project-status.md)).

## Merchant (PLANNED DATA CONTRACT)

| Field | Source |
|---|---|
| merchant_id | DERIVED |
| merchant_name | REAL (OSM) |
| merchant_category | DERIVED (OSM→MCC crosswalk) |
| mcc_code | DERIVED |
| latitude | REAL (OSM) |
| longitude | REAL (OSM) |
| country_code | DERIVED (assumption from extract region until true geocoding exists) |
| city | REAL (OSM, when present) |
| postal_code | REAL (OSM, when present) |
| street | REAL (OSM, when present) |
| website | REAL (OSM, when present) |
| phone | REAL (OSM, when present) |
| opening_hours | REAL (OSM, when present) |
| source_provider | DERIVED (constant, "osm_overpass") |
| source_record_id | REAL (OSM `id` + `type`) |
| source_updated_at | REAL (OSM `timestamp`, when `out meta;` is used) |
| ingestion_timestamp | DERIVED (pipeline-generated) |

## FX Rate (PLANNED DATA CONTRACT)

| Field | Source |
|---|---|
| base_currency | REAL (Frankfurter) |
| quote_currency | REAL (Frankfurter) |
| rate | REAL (Frankfurter) |
| rate_date | REAL (Frankfurter) |
| source_provider | DERIVED (constant, "frankfurter") |
| ingestion_timestamp | DERIVED (pipeline-generated) |

## Legal Entity (PLANNED DATA CONTRACT)

| Field | Source |
|---|---|
| lei | REAL (GLEIF) |
| legal_name | REAL (GLEIF) |
| country_code | REAL (GLEIF) |
| city | REAL (GLEIF) |
| region | REAL (GLEIF) |
| postal_code | REAL (GLEIF) |
| legal_form | REAL (GLEIF) |
| entity_category | REAL (GLEIF) |
| entity_status | REAL (GLEIF) |
| jurisdiction | REAL (GLEIF) |
| creation_date | REAL (GLEIF) |
| initial_registration_date | REAL (GLEIF) |
| last_update_date | REAL (GLEIF) |
| registration_status | REAL (GLEIF) |
| bic | REAL (GLEIF, when present) |
| source_provider | DERIVED (constant, "gleif") |
| ingestion_timestamp | DERIVED (pipeline-generated) |

> **Note (Step 6F real-run finding)**: the GLEIF API request uses `filter[entity.jurisdiction]=GB`. The real initial extraction returned `jurisdiction` values including `GB`, `GB-SCT`, and `GB-NIR`. Any Silver-layer field derived from this must not assume the value is literally `GB`.

All three contracts will be verified field-by-field against real ingestion output once that
ingestion code is published, per the phased roadmap in [20-roadmap.md](20-roadmap.md).
