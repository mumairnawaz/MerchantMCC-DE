# Client Data Delivery Evidence

## Real evidence (captured 2026-09-28, from the actual `outbox/` on disk)

**Real generated structure** — 4 clients, 5 dataset/client combinations, each with its own
timestamped run and manifest:

```
outbox/
├── CLI-0001/
│   ├── transaction_mart/run_20260927T115137Z_e5540eb3/
│   │   ├── transaction_mart.csv
│   │   ├── transaction_mart.parquet
│   │   └── manifest.json
│   ├── settlement_mart/run_20260927T115137Z_9593d8a3/...
│   └── reconciliation_mart/run_20260927T115137Z_25dc4c79/...
├── CLI-0004/
│   ├── transaction_mart/...
│   └── reconciliation_mart/...
├── CLI-0007/                      (PROGRAM_OWNER — different entitlement)
│   ├── client_program_mart/...    (row-filtered to CLI-0007's own rows only)
│   └── merchant_mart/...
└── CLI-0008/                      (a different PROGRAM_OWNER)
    ├── client_program_mart/...    (row-filtered to CLI-0008's own rows only)
    └── merchant_mart/...
```

**A real manifest.json** (`CLI-0001/transaction_mart/.../manifest.json`):
```json
{
  "run_id": "run_20260927T115137Z_e5540eb3",
  "client_id": "CLI-0001",
  "dataset": "transaction_mart",
  "schema_version": "1.0.0",
  "generated_at_utc": "2026-09-27T11:51:37Z",
  "reporting_period": {"start": "2026-06-23", "end": "2026-09-21"},
  "output_formats": ["csv", "parquet"],
  "record_count": 499,
  "control_total": "435106.1600",
  "control_total_column": "amount_total",
  "dq_status": "PASS",
  "validation_status": "PASSED"
}
```

**Caption**: *"Client Delivery — manifest containing schema version, record count,
reporting period, control total, and validation status."*

**Entitlement filtering, proven**: `CLI-0007` and `CLI-0008` are both entitled to
`client_program_mart`, but each receives only its own rows — CLI-0007 sees 2 program rows
(its own), CLI-0008 sees 1 (its own); neither ever sees the other's data.

**Idempotency, proven**: re-running the same (client, dataset, reporting period, schema
version) combination is recognized and safely skipped — confirmed by rerun testing
producing zero new/duplicate run directories.

## Manual screenshot checklist

- [ ] The `outbox/` tree above, open in a file explorer
- [ ] A `manifest.json` open in an editor (the content above is real and safe to show
      as-is — no credentials, no real customer data)
- [ ] A terminal showing a rerun being recognized as `"status": "skipped_existing"`

`outbox/` is gitignored (generated runtime output, not source) — this directory
documents what it produces without committing the generated files themselves.
