# 01 — Project Vision

FinPay is a Data Engineering platform first, and a fintech-domain project second — it exists
to demonstrate real ingestion, validation, transformation, modeling, and delivery
engineering, using a payments/merchant-intelligence domain as the vehicle. Power BI is a
downstream consumption layer, not the point of the project.

## What FinPay is

A pipeline that ingests real data from verified live public APIs, combines it with real
slow-changing reference datasets and internally generated synthetic financial events, and
moves all of it through Bronze → Silver → Gold to two outputs: internal BI and governed
file-based delivery for external stakeholders (issuer, network, program owner).

## What FinPay is not

- Not a production payments system
- Not connected to any real bank, card network, or cardholder data
- Not a Power BI/dashboard-first analytics project
- Not a claim that synthetic data represents real transactions

## Why this domain

The author has ~5 years of fintech/issuer-processing experience and cannot expose
confidential employer systems, schemas, or data. FinPay is built independently, entirely
from public data, open-source tooling, and clearly labeled synthetic data, to demonstrate
transferable Data Engineering capability in a domain that's genuinely relevant to that
background — without touching anything proprietary.
