"""S11 — CDC configuration/parsing/validation tests that do NOT require a
running Kafka/Debezium cluster (per this phase's testing instruction: unit-
testable logic must not depend on live infrastructure). Live end-to-end CDC
verification lives in tests/test_cdc_integration.py, gated on the actual
services being reachable.
"""

from src.cdc.connector import CONNECTOR_NAME, SLOT_NAME, TOPIC_PREFIX, build_connector_config
from src.cdc.publication import PUBLICATION_NAME, PUBLISHED_TABLES
from src.oltp.config import OltpSettings
from src.oltp.schema import SCHEMA_NAME, TABLE_DEPENDENCIES


def test_published_tables_covers_exactly_the_eleven_oltp_tables_no_more_no_less():
    assert set(PUBLISHED_TABLES) == set(TABLE_DEPENDENCIES)
    assert len(PUBLISHED_TABLES) == 11


def test_published_tables_is_sorted_and_deterministic():
    assert PUBLISHED_TABLES == sorted(PUBLISHED_TABLES)


def _fake_settings() -> OltpSettings:
    return OltpSettings(host="testhost", port=5432, dbname="merchantmcc", user="testuser", password="testpass", schema="finpay")


def test_connector_config_never_leaks_password_into_a_non_config_field():
    config = build_connector_config(_fake_settings())
    serialized_top_level = str({k: v for k, v in config.items() if k != "config"})
    assert "testpass" not in serialized_top_level
    assert config["config"]["database.password"] == "testpass"


def test_connector_config_targets_the_existing_finpay_schema_and_database():
    config = build_connector_config(_fake_settings())["config"]
    assert config["database.dbname"] == "merchantmcc"
    assert config["schema.include.list"] == SCHEMA_NAME == "finpay"


def test_connector_config_table_include_list_has_exactly_the_eleven_finpay_tables():
    config = build_connector_config(_fake_settings())["config"]
    tables = config["table.include.list"].split(",")
    assert len(tables) == 11
    assert all(t.startswith("finpay.") for t in tables)
    assert {t.split(".", 1)[1] for t in tables} == set(TABLE_DEPENDENCIES)


def test_connector_config_uses_pgoutput_plugin_no_extra_postgres_extension_needed():
    config = build_connector_config(_fake_settings())["config"]
    assert config["plugin.name"] == "pgoutput"


def test_connector_config_references_the_dedicated_publication_and_does_not_autocreate():
    config = build_connector_config(_fake_settings())["config"]
    assert config["publication.name"] == PUBLICATION_NAME
    assert config["publication.autocreate.mode"] == "disabled"


def test_connector_config_uses_explicit_slot_name():
    config = build_connector_config(_fake_settings())["config"]
    assert config["slot.name"] == SLOT_NAME


def test_connector_config_snapshot_mode_is_initial_given_11_populated_tables():
    config = build_connector_config(_fake_settings())["config"]
    assert config["snapshot.mode"] == "initial"


def test_topic_naming_convention_makes_source_schema_table_explicit():
    # finpay (topic.prefix) . finpay (schema) . <table> — matches the S11
    # brief's own example ("finpay.finpay.transactions").
    config = build_connector_config(_fake_settings())["config"]
    assert config["topic.prefix"] == TOPIC_PREFIX == "finpay"
    expected_topic = f"{TOPIC_PREFIX}.{SCHEMA_NAME}.transactions"
    assert expected_topic == "finpay.finpay.transactions"


def test_connector_name_is_stable_and_deterministic():
    c1 = build_connector_config(_fake_settings())
    c2 = build_connector_config(_fake_settings())
    assert c1["name"] == c2["name"] == CONNECTOR_NAME
