"""Gold layer paths — no environment variables needed (unlike src/oltp/config.py
or src/cdc/consumer_config.py), since Gold has no external service to connect
to: it reads local Parquet and writes a local DuckDB file."""

from pathlib import Path

API_SILVER_ROOT = Path("data/silver")
CDC_SILVER_ROOT = Path("data/silver_cdc")

GOLD_ROOT = Path("data/gold")
DIMENSIONS_ROOT = GOLD_ROOT / "dimensions"
FACTS_ROOT = GOLD_ROOT / "facts"
KEY_REGISTRY_ROOT = GOLD_ROOT / "_key_registry"
DUCKDB_PATH = GOLD_ROOT / "gold.duckdb"

# No quarantine directory for Gold (unlike Bronze/Silver/CDC layers): every
# input row here has already passed Silver-level validation. A Gold DQ
# failure (broken grain, orphan FK, duplicate surrogate key) indicates a
# join/registry bug, not a per-record business data issue to set aside and
# continue past — so Gold validation (src/gold/validation.py) is hard-fail
# (raises), not quarantine-and-continue. Documented, not an oversight.

UNKNOWN_KEY = -1
UNKNOWN_NATURAL_KEY = "UNKNOWN"
