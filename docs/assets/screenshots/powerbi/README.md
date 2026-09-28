# Power BI Evidence

## Illustrative dashboard mockups (not Power BI Desktop screenshots)

![MCC Details Dashboard mockup](01-mcc-details-dashboard.png)
![Transaction Analysis Dashboard mockup](02-transaction-analysis-dashboard.png)

**These two images are illustrative portfolio mockups of the intended MerchantMCC-DE
analytics experience — they are not screenshots of Power BI Desktop, and the specific
figures shown on them (e.g. "1,248,392 transactions", "$18,642,903", "$185.6M") are
illustrative/synthetic values used purely to demonstrate dashboard layout and visual
design. They do not come from and do not match this project's actual verified data.**

The project's real, live-verified control values are:

| Metric | Real value |
|---|---|
| `fact_transactions` row count | 4,000 |
| Approved transaction amount (control total) | £435,106.16 |
| Settlement amount | £435,106.16 |
| Reconciliation expected amount | £435,106.16 |
| Declined transactions | 536 |

For a mockup built from these real, verified figures and the real mart/measure names
used by the actual TMDL semantic model, see [`ILLUSTRATIVE-MOCKUP.md`](ILLUSTRATIVE-MOCKUP.md)
and [`executive-overview-MOCKUP.html`](executive-overview-MOCKUP.html) — both are also
clearly banner-labeled as illustrative, not real screenshots.

## What is independently verified today

- PBIP/PBIR/TMDL structure: schema-validated against Microsoft's own published schemas
  (24/24 visuals, all supporting files).
- 46/46 field references verified against the real semantic model's columns/measures.
- The DuckDB ODBC data connection: independently tested and proven to return correct data.
- **Not verified**: actual on-screen rendering of the real `.pbip` project in Power BI
  Desktop. The two mockup images above illustrate the intended experience but were not
  produced by opening `reports/MerchantMCC_S15C_Executive_Overview.pbip` — they should not
  be read as evidence that it renders successfully.

## Manual screenshot checklist (capture only after opening the real .pbip and confirming rendering)

- [ ] Executive Overview page
- [ ] Transaction & Merchant Analytics page
- [ ] Settlement & Reconciliation page
- [ ] Client / Program Analytics page

When capturing, crop out or avoid:
- the ODBC DSN credential dialog (if it appears — this project's DSN is credential-less,
  but the dialog itself shouldn't be shown)
- any local Windows file path in the title bar
- the "Get Data" connection string editor

Once captured, replace this file's mockup section with the real images and update the
project status line from "Desktop rendering verification pending" to "Desktop rendering
verified."
