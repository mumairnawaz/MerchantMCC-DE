"""Client Data Delivery — dataset schema/version registry and client
entitlement mapping.

Both are small, static, version-controlled JSON files (matching this
project's existing configs/sources.json convention for src/ingestion) —
deliberately NOT a database table or a real authorization service. See the
Client Data Delivery Audit §3/§4: this is a portfolio-appropriate synthetic
entitlement boundary (a lookup table + a filter), not an enterprise identity
platform.

Client IDs are the REAL synthetic clients already present in Gold's
dim_client / marts.client_program_mart (CLI-0001 .. CLI-0012) — not invented
identifiers. Only a representative subset is entitled here (one ISSUER, one
NETWORK, two PROGRAM_OWNERs) — enough to demonstrate dataset-level gating
and, for the two PROGRAM_OWNERs, row-level filtering of the same dataset by
different clients. Extending coverage to more real clients is a config-only
change (add an entry to configs/client_entitlements.json), not a code change.
"""

import json
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DELIVERY_DATASETS_PATH = PROJECT_ROOT / "configs" / "delivery_datasets.json"
CLIENT_ENTITLEMENTS_PATH = PROJECT_ROOT / "configs" / "client_entitlements.json"


def load_dataset_registry(path: Path = DELIVERY_DATASETS_PATH) -> dict[str, dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_client_entitlements(path: Path = CLIENT_ENTITLEMENTS_PATH) -> dict[str, dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def get_dataset_definition(dataset_name: str, registry: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    registry = registry if registry is not None else load_dataset_registry()
    if dataset_name not in registry:
        from src.delivery.errors import UnknownDatasetError

        raise UnknownDatasetError(f"'{dataset_name}' has no entry in {DELIVERY_DATASETS_PATH}")
    return registry[dataset_name]


def get_client_entitlement(client_id: str, entitlements: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    entitlements = entitlements if entitlements is not None else load_client_entitlements()
    if client_id not in entitlements:
        from src.delivery.errors import UnknownClientError

        raise UnknownClientError(f"'{client_id}' has no entry in {CLIENT_ENTITLEMENTS_PATH}")
    return entitlements[client_id]


def assert_authorized(client_id: str, dataset_name: str, entitlements: dict[str, dict[str, Any]] | None = None) -> None:
    """Raises UnknownClientError / UnauthorizedDatasetError. Returns None (no
    value) when authorized — callers just call this before proceeding."""
    entitlement = get_client_entitlement(client_id, entitlements)
    if dataset_name not in entitlement["allowed_datasets"]:
        from src.delivery.errors import UnauthorizedDatasetError

        raise UnauthorizedDatasetError(
            f"'{client_id}' is not entitled to dataset '{dataset_name}' "
            f"(allowed: {entitlement['allowed_datasets']})"
        )
