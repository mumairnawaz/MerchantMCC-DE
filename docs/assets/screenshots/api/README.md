# API Ingestion Evidence

## Real evidence already present in the repository

Every API Bronze run is on disk under `data/bronze/<source>/run_<timestamp>/`, each with a
`metadata.json` recording the source URL, ingestion timestamp, record count, and
validation result — this **is** the evidence, not a screenshot substitute for it. Example,
from a real GLEIF run: `data/bronze/gleif/run_20260921T054417Z/metadata.json`. Per-source
watermark files (proving incremental, not full-reload, ingestion) are at
`data/watermarks/<source>.json`.

These are gitignored (per `.gitignore`'s `data/*` rules) because they are generated
runtime output, not source — the evidence is that the *mechanism* exists and has run
successfully, documented here rather than committed as data.

## Manual screenshot checklist

- [ ] A terminal running `python scripts/run_ingestion.py gleif` (or another source),
      showing the printed `-> <run_dir>` output on success
- [ ] The contents of a real `data/bronze/<source>/run_*/metadata.json` file open in an
      editor
- [ ] A real `data/watermarks/<source>.json` file, showing the watermark advancing between
      two runs
- [ ] Airflow's `merchantmcc_api_pipeline` DAG — a successful run in the Grid view

**Caption suggestions**:
*"API ingestion — GLEIF Bronze metadata showing record count, source URL, and validation
result for a real live run."*
*"API ingestion — watermark file showing the incremental cursor advancing after a
successful run."*

No API in this project requires a key or token (OpenStreetMap Overpass, Frankfurter, and
GLEIF are all verified live to work with no authentication), so there is nothing to redact
in any of this evidence.
