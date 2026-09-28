"""Deterministic surrogate-key management (§9/§25 of this phase).

A surrogate key must be STABLE across rebuilds — the same natural key must
always map to the same integer, or every fact table referencing it would
need to be rebuilt in lockstep and idempotency (§25) would break. This is
implemented as a small persisted registry per dimension
(data/gold/_key_registry/<dimension>.json, mapping natural_key -> int),
mirroring this project's established "small JSON state file" pattern
(src/ingestion/watermark.py, src/cdc/checkpoint.py) rather than a new
mechanism. Existing assignments are never renumbered; only genuinely new
natural keys get a new (next-available) integer — exactly how a real
warehouse dimension's identity/sequence column behaves.

Surrogate key `-1` is reserved project-wide for the UNKNOWN member (§10) —
never assigned to a real natural key.
"""

import json
from pathlib import Path

from src.gold.config import KEY_REGISTRY_ROOT, UNKNOWN_KEY, UNKNOWN_NATURAL_KEY


def _registry_path(dimension: str, root: Path = KEY_REGISTRY_ROOT) -> Path:
    return root / f"{dimension}.json"


def load_registry(dimension: str, root: Path = KEY_REGISTRY_ROOT) -> dict[str, int]:
    path = _registry_path(dimension, root)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_registry(dimension: str, registry: dict[str, int], root: Path = KEY_REGISTRY_ROOT) -> Path:
    path = _registry_path(dimension, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(registry, sort_keys=True, indent=2), encoding="utf-8")
    return path


def assign_surrogate_keys(dimension: str, natural_keys: list[str], root: Path = KEY_REGISTRY_ROOT) -> dict[str, int]:
    """Returns the FULL natural_key -> surrogate_key mapping for this
    dimension (existing + any newly assigned), and persists it. Iterates
    `natural_keys` in sorted order so a from-scratch build is itself
    deterministic (not dependent on Parquet row order)."""
    registry = load_registry(dimension, root)
    next_key = (max(registry.values()) + 1) if registry else 1
    for nk in sorted(set(natural_keys)):
        if nk not in registry:
            registry[nk] = next_key
            next_key += 1
    save_registry(dimension, registry, root)
    return registry


def with_unknown_member(registry: dict[str, int]) -> dict[str, int]:
    return {**registry, UNKNOWN_NATURAL_KEY: UNKNOWN_KEY}
