# 03 — Project Objectives

1. Ingest real external data through real, independently verified APIs — never a fabricated
   or assumed source.
2. Keep real, reference, and synthetic data clearly and permanently distinguishable at
   every layer, from Bronze metadata through to any delivered file.
3. Implement genuine incremental ingestion (watermark-driven), not repeated full loads
   dressed up as incremental.
4. Build defensible data quality gates — ones that catch real problems, not decorative
   checks that always pass.
5. Deliver both an internal BI layer and governed, stakeholder-ready CSV/Parquet files with
   control totals and audit metadata.
6. Do all of this at zero cost — no paid APIs, no paid services, no credit-card-gated tiers.
7. Document every non-obvious decision (source substitutions, rejected alternatives, real
   verification failures encountered) so the engineering reasoning is visible, not just the
   final result.
