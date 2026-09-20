# 12 — Data Quality

## Two tiers, deliberately kept separate

**Ingestion-level validation** answers only "did we receive something structurally usable
from the source?" — non-empty checks, required-field checks, duplicate-key checks (with an
explicit informational mode for sources where duplication is expected, such as ISO 4217's
per-entity rows), and field-pattern checks (e.g. a verified digit-length pattern for BIN
values). This tier is deliberately minimal and never performs business transformation.

**Silver-layer data quality** (PLANNED) owns business-rule validation, type coercion, and
the cross-source referential checks listed in [11-data-model.md](11-data-model.md).

## A real example, not a hypothetical

During earlier development of this project's data-source strategy, the country reference
API in use at the time (REST Countries v3.1) was found to have been deprecated in favor of
a paid v5 API. Because ingestion-level validation checked for required fields (`cca2`,
`cca3`, `name`) rather than merely checking for HTTP 200, the deprecation's error-envelope
response was correctly flagged as a validation failure instead of silently being treated as
zero valid countries. This is the standard this project holds its data quality design to:
checks that catch real problems, not checks that always pass.

## Planned tooling

Pandera and/or dbt tests for the Silver gate — not yet selected/implemented in this
repository. See [20-roadmap.md](20-roadmap.md).
