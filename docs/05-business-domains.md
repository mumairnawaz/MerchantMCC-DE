# 05 — Business Domains

Presented without ranking — each domain contributes a distinct capability to the platform.

## Merchant Intelligence

Real merchant identity and location (OpenStreetMap) classified by MCC via a curated
crosswalk (OSM has no native MCC concept — see [11-data-model.md](11-data-model.md)).
Feeds merchant performance and category-analysis marts.

## Issuer / Network / Program-Owner Reporting

Real institutional/legal-entity data (GLEIF) standing in for the issuer/program-owner
concept, combined with synthetic program/campaign structures. Demonstrates governed
reporting output, not just internal dashboards.

## Rewards / CLO

Synthetic offers matched against synthetic transactions using real MCC codes as the
eligibility key — mirroring how real card-linked-offer platforms match transactions to
offers via MCC and merchant identity.

## Reconciliation

Real daily FX rates (Frankfurter) normalizing synthetic multi-currency transaction
lifecycles, and comparison logic across synthetic authorization / clearing / settlement
events to detect breaks.

## Data Engineering Operations

The pipeline's own reliability: ingestion-level validation, source metadata capture,
watermark-driven incremental runs, and auditability — the domain that makes the other four
trustworthy.
