# dbt / Gold Evidence

## Real evidence captured in this directory

[`dbt_build_output.txt`](dbt_build_output.txt) — the actual output of `dbt build`, run
live against this project's real `dbt/` project and `data/gold/gold.duckdb` on
2026-09-28. Final line:

```
Done. PASS=73 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=73
```

73/73: 6 mart table models + 22 staging/intermediate view models + 45 data tests,
including `assert_control_total_identity` — the singular test that re-verifies the
£435,106.16 control total from the marts themselves, independent of Gold's own Python
reconciliation. dbt documentation has been generated (`dbt docs generate`) and is present
under `dbt/target/` (`catalog.json`, `index.html`).

**Caption**: *"dbt — 73/73 models and tests passing, including the control-total identity
test."*

## Manual screenshot checklist

- [ ] `dbt docs generate` output rendered in a browser (the model DAG / lineage graph)
- [ ] The 6 marts listed in `dbt/target/catalog.json` or the generated docs site
- [ ] A DuckDB query against `marts.transaction_mart`/`marts.settlement_mart` showing the
      matching £435,106.16 figures side by side

No credentials appear anywhere in dbt's output — `dbt/profiles.yml` is a local, file-based
DuckDB profile with no connection secrets.
