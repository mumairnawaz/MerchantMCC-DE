"""Loads real, already-published Silver Parquet output as reference dimensions
for synthetic generation. Read-only — never writes to data/silver/, never
imports src.ingestion. If a required Silver dataset hasn't been generated yet
(e.g. a fresh checkout before running S3-S6), this raises FileNotFoundError
rather than silently falling back to fabricated reference data.
"""

from pathlib import Path
from typing import Any

from src.silver import common as silver_common

SILVER_ROOT = Path("data/silver")

# The small, deterministic currency universe used by the synthetic programs —
# major real currencies only, not a uniform sample across all 307 real ISO
# codes (most of which would never realistically appear in a UK card program).
# Verified present in real Silver output before use (see docs/22 §9).
PROGRAM_CURRENCIES = ["GBP", "EUR", "USD"]


def load_reference(silver_root: Path = SILVER_ROOT) -> dict[str, list[dict[str, Any]]]:
    """One read of every real Silver dataset the synthetic generator depends on.
    Bundled into one dict so the generator/tests can pass a single object around
    instead of six separate reads, and so tests can substitute a synthetic
    in-memory fixture without touching real data/silver/ files."""
    root = silver_root

    def load(dataset: str) -> list[dict[str, Any]]:
        path = root / dataset / "data.parquet"
        if not path.exists():
            raise FileNotFoundError(
                f"src.synthetic.reference: Silver dataset '{dataset}' not found at {path} — "
                f"run the corresponding Silver phase (S3-S6) before generating synthetic data."
            )
        return silver_common.read_parquet(path)

    return {
        "merchant": load("merchant"),
        "mcc": load("mcc"),
        "country": load("country"),
        "iso_currency": load("iso_currency"),
        "card_issuer": load("card_issuer"),
        "legal_entity": load("legal_entity"),
    }
