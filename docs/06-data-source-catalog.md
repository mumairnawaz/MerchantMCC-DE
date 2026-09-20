# 06 — Data Source Catalog

Two status dimensions are used throughout this document and must not be confused:

- **Verification Status** — whether a source's claimed behavior (auth, fields, update
  cadence, incremental mechanism) has actually been tested against the live provider.
  `VERIFIED` means we made real calls and observed the real result. `PROPOSED` means a
  design choice not yet finalized. `PLANNED` means researched but not yet implemented.
- **Ingestion Status** — whether this repository currently contains and runs the ingestion
  code for that source. As of this publication, ingestion code is intentionally **not**
  part of this repository yet (see [19-project-status.md](19-project-status.md)) — that is
  a separate, later phase.

## Summary

| Source | Type | FinPay Domain | Verification Status |
|---|---|---|---|
| OpenStreetMap Overpass | Live API | Merchant Intelligence | VERIFIED |
| Frankfurter | Live API | Reconciliation | VERIFIED |
| GLEIF LEI | Live API | Issuer / Institution / Program-Owner | VERIFIED |
| MCC codes | Reference | Merchant classification | VERIFIED |
| Country reference | Reference | Geographic enrichment | VERIFIED |
| ISO 4217 | Reference | Currency metadata | VERIFIED |
| BIN/IIN issuer | Reference | Card issuer enrichment | VERIFIED |

---

## 1. OpenStreetMap Overpass API

| Field | Value |
|---|---|
| Provider | OpenStreetMap Foundation |
| Source Type | Live API |
| Website | https://www.openstreetmap.org |
| API Endpoint | `https://overpass-api.de/api/interpreter` |
| HTTP Method | POST (`data=<Overpass QL query>`) |
| Authentication | None |
| API Key | Not required |
| Account Required | No |
| Cost | Free |
| Expected Update Frequency | Continuous community edits; FinPay ingests weekly |
| Incremental Mechanism | `newer:"<ISO8601>"` filter on a named result set — **VERIFIED live**: requires a descriptive `User-Agent` (its absence returns HTTP 406) and a generous `[timeout:N]` budget (a short one returns HTTP 504, since `newer` scans attic/history data) |
| Specific Data Retrieved | Shop and select amenity POIs (restaurant, cafe, fast_food, bank, pharmacy) within a small bounding box |
| Important Fields | `type, id, lat, lon, tags.shop, tags.amenity, tags.name, tags.addr:city, tags.addr:postcode, tags.addr:street, tags.phone, tags.website, tags.opening_hours`; with `out meta;`: `timestamp, version, changeset` |
| FinPay Domain | Merchant Intelligence |
| Bronze Destination | `data/bronze/merchant_osm/` (PLANNED location once ingestion is published) |
| Silver Destination | `dim_merchant`, via OSM→MCC crosswalk |
| Gold Usage | Merchant performance, MCC/category analysis marts |
| License | ODbL 1.0 — attribution required, share-alike on the derived database |
| Attribution | © OpenStreetMap contributors |
| Known Limitations | `newer` detects additions/edits, not deletions, within a bounded extract — periodic full re-sync required; not every element has every field (e.g. `name` is sometimes absent) |
| Verification Status | VERIFIED |

## 2. Frankfurter API

| Field | Value |
|---|---|
| Provider | Frankfurter (open-source project, ECB-sourced data) |
| Source Type | Live API |
| Website | https://api.frankfurter.dev |
| API Endpoint | `https://api.frankfurter.dev/v1/latest`, `/v1/{date}`, `/v1/{start}..{end}` |
| HTTP Method | GET |
| Authentication | None |
| API Key | Not required |
| Account Required | No |
| Cost | Free |
| Expected Update Frequency | Once per business day; FinPay ingests daily |
| Incremental Mechanism | `GET /v1/{watermark+1}..{today}` — **VERIFIED live**, returns exactly the missing dates in one call |
| Specific Data Retrieved | Daily EUR-based exchange rates for ~30 currencies |
| Important Fields | `base, date, rates.{currency_code: rate}` |
| FinPay Domain | Reconciliation / FX normalization |
| Bronze Destination | `data/bronze/currency/` (PLANNED location once incremental ingestion is published) |
| Silver Destination | `ref_exchange_rate_daily` |
| Gold Usage | Multi-currency reconciliation marts |
| License | Open-source project; underlying data is public ECB reference data |
| Attribution | Not required, cited for auditability |
| Known Limitations | Daily reference rates, not a trading feed; no rate on weekends/EU holidays — a missing day is expected, not a failure |
| Verification Status | VERIFIED |

## 3. GLEIF LEI API

| Field | Value |
|---|---|
| Provider | Global Legal Entity Identifier Foundation |
| Source Type | Live API |
| Website | https://www.gleif.org |
| API Endpoint | `https://api.gleif.org/api/v1/lei-records` |
| HTTP Method | GET (JSON:API) |
| Authentication | None — **VERIFIED live**, no key or account of any kind |
| API Key | Not required |
| Account Required | No |
| Cost | Free |
| Expected Update Frequency | Continuous; FinPay ingests daily |
| Incremental Mechanism | `filter[registration.lastUpdateDate]=>=<watermark>` with pagination — **VERIFIED live**: filtering for entities updated since 2026-09-19 returned 2,513 real records in a single day |
| Specific Data Retrieved | Legal entity records, scoped to a filtered subset (jurisdiction/status) — never the full 3.4M-record pool |
| Important Fields | `lei, entity.legalName.name, entity.legalAddress.{country,city,region,postalCode,addressLines}, entity.headquartersAddress.{...}, entity.legalForm.id, entity.category, entity.status, entity.jurisdiction, entity.creationDate, registration.initialRegistrationDate, registration.lastUpdateDate, registration.status, registration.nextRenewalDate, bic` — every field confirmed present in a real live record fetched during verification |
| FinPay Domain | Issuer / Institution / Program-Owner intelligence |
| Bronze Destination | `data/bronze/gleif_lei/` (PLANNED — not yet ingested) |
| Silver Destination | `dim_client` |
| Gold Usage | Issuer/program-owner reporting marts |
| License | GLEIF open data terms — free reuse |
| Attribution | GLEIF |
| Known Limitations | Full pool is very large; any ingestion must filter, never bulk-load |
| Verification Status | VERIFIED |

## 4. MCC Reference Data

| Field | Value |
|---|---|
| Provider | github.com/greggles/mcc-codes (community-maintained) |
| Source Type | Reference |
| API Endpoint | `https://raw.githubusercontent.com/greggles/mcc-codes/main/mcc_codes.csv` |
| HTTP Method | GET |
| Authentication / API Key / Account | None |
| Cost | Free |
| Expected Update Frequency | Slow-changing (regulated classification standard) — low-frequency manual refresh |
| Incremental Mechanism | Not applicable — full snapshot replace |
| Important Fields | `mcc, edited_description, combined_description, usda_description, irs_description, irs_reportable` |
| FinPay Domain | Merchant classification |
| Silver Destination | `dim_mcc` |
| License | Open, community-compiled |
| Known Limitations | No formal versioning; community repo could go stale |
| Verification Status | VERIFIED (981 real records confirmed) |

## 5. Country Reference Data

| Field | Value |
|---|---|
| Provider | github.com/mledoze/countries (MIT-licensed) |
| Source Type | Reference |
| API Endpoint | `https://raw.githubusercontent.com/mledoze/countries/master/dist/countries.json` |
| HTTP Method | GET |
| Authentication / API Key / Account | None |
| Cost | Free |
| Expected Update Frequency | Slow-changing — low-frequency manual refresh |
| Important Fields | `cca2, cca3, ccn3, name.common, name.official, region, subregion, capital, latlng, currencies` |
| FinPay Domain | Geographic enrichment |
| Silver Destination | `dim_country` |
| License | MIT |
| Known Limitations | Originally sourced from REST Countries v3.1, which was deprecated for a paid v5 API mid-project — substitution documented as a design decision |
| Verification Status | VERIFIED (250 real records confirmed) |

## 6. ISO 4217 Currency Metadata

| Field | Value |
|---|---|
| Provider | github.com/datasets/currency-codes (Frictionless Data lineage) |
| Source Type | Reference |
| API Endpoint | `https://raw.githubusercontent.com/datasets/currency-codes/main/data/codes-all.csv` |
| HTTP Method | GET |
| Authentication / API Key / Account | None |
| Cost | Free |
| Expected Update Frequency | Slow-changing (ISO revisions only) — low-frequency manual refresh |
| Important Fields | `Entity, Currency, AlphabeticCode, NumericCode, MinorUnit, WithdrawalDate` |
| FinPay Domain | Currency metadata |
| Silver Destination | `dim_currency` |
| License | Public Domain Dedication and License (PDDL) |
| Known Limitations | `AlphabeticCode` repeats once per entity in the raw file (e.g. EUR once per Eurozone country) — Silver deduplication required |
| Verification Status | VERIFIED (449 raw records / 307 distinct currency codes confirmed) |

## 7. BIN/IIN Issuer Reference

| Field | Value |
|---|---|
| Provider | github.com/venelinkochev/bin-list-data (community-maintained) |
| Source Type | Reference |
| API Endpoint | `https://raw.githubusercontent.com/venelinkochev/bin-list-data/master/bin-list-data.csv` |
| HTTP Method | GET |
| Authentication / API Key / Account | None |
| Cost | Free |
| Expected Update Frequency | Slow-changing — low-frequency manual refresh |
| Important Fields | `BIN, Brand, Type, Category, Issuer, IssuerPhone, IssuerUrl, isoCode2, isoCode3, CountryName` |
| FinPay Domain | Card brand / issuer / country enrichment for synthetic tokenized-card records |
| Silver Destination | `dim_card_issuer` |
| License | CC BY 4.0 |
| Attribution | Required: "BIN List Data (venelinkochev/bin-list-data), CC BY 4.0" |
| Known Limitations | ~26.3 MB / 374,788 real rows — confirmed all BINs are exactly 6 digits; 48.2% of rows have a blank `Issuer` field, a genuine gap in this community dataset |
| Verification Status | VERIFIED (full dataset downloaded and analyzed) |

## Sources evaluated and not selected

| Source | Reason |
|---|---|
| REST Countries v5 | Deprecated the free v3.1 API for a paid API requiring an account/key |
| UK Companies House API | Redundant with GLEIF; GLEIF has zero signup friction and broader coverage |
| FRED API, Eurostat API | Real and free, but no FinPay business domain uses macroeconomic indicators |
| Foursquare Places API | Redundant with OSM for the same domain; smaller free quota, requires account/key |
| Commercial coupon/deals APIs | No free, legal, real API exists for merchant offers/rewards data |
