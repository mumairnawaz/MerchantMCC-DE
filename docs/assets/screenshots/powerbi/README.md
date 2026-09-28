# Power BI Evidence

**No screenshots are included in this directory.** Per this project's own verification
standard, a Power BI Desktop rendering screenshot is only included once rendering has
actually been confirmed — and that confirmation has not yet happened (the most recent
Power BI Desktop rendering investigation found and fixed a real structural defect in the
PBIR report — a deprecated legacy `card` visual type — but the fix has not yet been
re-opened and visually confirmed in Power BI Desktop by a human). Claiming rendering
success without that evidence would misrepresent the project's actual state.

## What is independently verified today

- PBIP/PBIR/TMDL structure: schema-validated against Microsoft's own published schemas
  (24/24 visuals, all supporting files).
- 46/46 field references verified against the real semantic model's columns/measures.
- The DuckDB ODBC data connection: independently tested and proven to return correct data.
- **Not verified**: actual on-screen rendering in Power BI Desktop.

## Manual screenshot checklist (capture only after opening and confirming rendering)

- [ ] Executive Overview page
- [ ] Transaction & Merchant Analytics page
- [ ] Settlement & Reconciliation page
- [ ] Client / Program Analytics page

When capturing, crop out or avoid:
- the ODBC DSN credential dialog (if it appears — this project's DSN is credential-less,
  but the dialog itself shouldn't be shown)
- any local Windows file path in the title bar you'd rather not publish
- the "Get Data" connection string editor

Once captured, replace this file's Power BI section of the README (and this checklist)
with the real images and update the project status line from "Desktop rendering
verification pending" to "Desktop rendering verified."
