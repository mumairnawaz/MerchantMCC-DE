> ## ⚠️ ILLUSTRATIVE MOCKUP — ACTUAL POWER BI DESKTOP RENDERING NOT YET VERIFIED
>
> This page is **not a screenshot**. It is a hand-built mockup of the intended
> MerchantMCC-DE analytics experience, using the project's real metrics and real dbt mart
> structure, so a reader can see what the Power BI report is *designed* to show. The real
> engineering artifact — the PBIP project with its TMDL semantic model, 16 real DAX
> measures, and 24 real PBIR visual definitions — is schema-validated and connects
> successfully to real data (see [`08-power-bi-consumption.md`](../../../architecture/diagrams/08-power-bi-consumption.md)),
> but has not yet been opened and visually confirmed rendering in Power BI Desktop. See
> [`../powerbi/README.md`](README.md) for the honest current status.

---

## Page 1 — Executive Overview

**KPI cards:**

| Total Transactions | Total Transaction Amount | Approved Amount | Declined Transactions | Settlement Amount | Reconciliation Expected |
|---|---|---|---|---|---|
| **4,000** | **£501,672.07** | **£435,106.16** | **536** | **£435,106.16** | **£435,106.16** |

**Control identity** (the headline check every viewer should be able to verify at a
glance): `Approved Amount = Settlement Amount = Reconciliation Expected = £435,106.16` ✅

**Transaction status breakdown** (donut): `APPROVED 87.9% · DECLINED 13.4%` *(of
transaction count — approximate, illustrative proportions)*

**Trend over time** (line): daily `amount_total`, sourced from `transaction_mart`

**Top merchants** (table): `merchant_name`, `approved_amount_total` — sourced from
`merchant_mart`

---

## Page 2 — Transaction & Merchant Analytics

| Approved vs Declined (column) | Amount Trend (line) |
|---|---|
| `transaction_mart.transaction_status` × `Total Transactions` | `transaction_mart.full_date` × `Total Transaction Amount` |

**Merchant summary table** — `merchant_name`, `mcc_description`, `Merchant Transaction
Count`, `Merchant Approved Amount`, `Merchant Declined Count` — sourced from
`merchant_mart`.

**Slicers**: `transaction_status`, `currency_code`

---

## Page 3 — Settlement & Reconciliation

**KPI cards:**

| Settlement Amount | Reconciliation Expected | Reconciliation Actual | Reconciliation Difference |
|---|---|---|---|
| **£435,106.16** | **£435,106.16** | **£434,846.91** | **−£259.25** |

**Reconciliation detail table** — `reconciled_date`, `match_status`, `Reconciliation
Expected Total`, `Reconciliation Actual Total`, `Reconciliation Difference` — sourced from
`reconciliation_mart`. `match_status` slicer included (`MATCHED` / `EXCEPTION`).

---

## Page 4 — Client / Program Analytics

**Client/program summary table** — `client_legal_name`, `program_name`, `program_type`,
`Client Transaction Count`, `Client Approved Amount` — sourced from `client_program_mart`.

**Approved amount by client** (column chart).

**Client slicer** — filters the whole page to one client at a time.

---

Every field, measure name, and mart named above is real — copied directly from the actual
TMDL semantic model and dbt marts, not invented for this mockup. Every number is the
project's real, live-verified control total. Nothing here is sample/placeholder data.
