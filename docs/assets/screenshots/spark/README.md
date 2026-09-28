# Spark Evidence

## Real evidence (captured 2026-09-28, from real output already on disk)

**Bronze → Silver reconciliation — deliberately injected defects, fully accounted for:**
```
1,010 Bronze records (synthetic generator, seeded, real defects deliberately injected:
missing IDs, malformed timestamps, invalid statuses, duplicate transaction IDs)
        ↓
  875 valid   → data/silver_spark/transactions/
  135 rejected → data/rejected_spark/transactions/ (each with a recorded reason)
  ─────────────
  1,010 — fully reconciled, zero unaccounted records
```

**Silver → Gold — a real, independent star schema, confirmed on disk:**
```
data/gold_spark/fact_transactions/   → 875 rows (matches Spark Silver exactly)
data/gold_spark/dim_merchant/
data/gold_spark/dim_date/
```

**Caption**: *"Spark — 1,010 Bronze records reconciled to 875 valid + 135 rejected,
carried through to an independent 875-row Gold fact table."*

This is a separate, independent engineering track (`src/spark/`) — it shares no code or
data with the production Gold pipeline in `src/gold/`. It exists to demonstrate
distributed-processing patterns (PySpark 4.2, window functions, ANSI-mode type coercion),
not to reprocess production volumes that don't need a distributed engine at this scale.

## Manual screenshot checklist

- [ ] A terminal running `spark-submit src/spark/bronze_to_silver.py`, showing the Spark
      job's own console reconciliation output (875/135 split)
- [ ] The Spark UI (if run with it enabled) showing the job's DAG/stages
