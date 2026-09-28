import pytest

from src.delivery import config
from src.delivery.errors import UnauthorizedDatasetError, UnknownClientError, UnknownDatasetError

REGISTRY = {
    "widget_mart": {
        "schema_version": "1.0.0",
        "output_formats": ["csv"],
        "expected_columns": ["widget_id", "count"],
        "business_grain": ["widget_id"],
        "requires_client_filter": False,
        "date_column": None,
        "control_total_column": None,
        "control_total_filter": None,
    }
}
ENTITLEMENTS = {
    "CLI-TEST-A": {"legal_name": "Test Client A", "client_type": "ISSUER", "allowed_datasets": ["widget_mart"]},
    "CLI-TEST-B": {"legal_name": "Test Client B", "client_type": "NETWORK", "allowed_datasets": []},
}


def test_get_dataset_definition_returns_known_dataset():
    assert config.get_dataset_definition("widget_mart", REGISTRY)["schema_version"] == "1.0.0"


def test_get_dataset_definition_raises_for_unknown_dataset():
    with pytest.raises(UnknownDatasetError):
        config.get_dataset_definition("nonexistent_mart", REGISTRY)


def test_get_client_entitlement_returns_known_client():
    assert config.get_client_entitlement("CLI-TEST-A", ENTITLEMENTS)["client_type"] == "ISSUER"


def test_get_client_entitlement_raises_for_unknown_client():
    with pytest.raises(UnknownClientError):
        config.get_client_entitlement("CLI-DOES-NOT-EXIST", ENTITLEMENTS)


def test_assert_authorized_passes_for_allowed_dataset():
    config.assert_authorized("CLI-TEST-A", "widget_mart", ENTITLEMENTS)  # no raise


def test_assert_authorized_raises_for_unauthorized_dataset():
    with pytest.raises(UnauthorizedDatasetError):
        config.assert_authorized("CLI-TEST-B", "widget_mart", ENTITLEMENTS)


def test_real_config_files_load_and_are_internally_consistent():
    """The real, production configs/*.json are static/version-controlled, not
    mutable data — loading and structurally validating them here is safe and
    does not depend on data/gold/gold.duckdb."""
    registry = config.load_dataset_registry()
    entitlements = config.load_client_entitlements()
    assert "transaction_mart" in registry
    assert "client_program_mart" in registry
    for client_id, entitlement in entitlements.items():
        for dataset_name in entitlement["allowed_datasets"]:
            assert dataset_name in registry, f"{client_id} is entitled to unregistered dataset {dataset_name}"
