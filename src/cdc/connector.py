"""Debezium PostgreSQL connector configuration + Kafka Connect REST client.

Kafka Connect's REST API is plain HTTP — this uses `requests` (already an
approved project dependency since Bronze ingestion) to register/inspect the
connector. This is NOT a Kafka protocol client and does not read/write Kafka
topics directly; that would reproduce what Kafka Connect/Debezium already do,
which this phase's instructions explicitly say not to do.
"""

from typing import Any

import requests

from src.cdc.publication import PUBLICATION_NAME, PUBLISHED_TABLES
from src.oltp.config import OltpSettings, settings
from src.oltp.schema import SCHEMA_NAME

CONNECT_URL = "http://localhost:8083"
CONNECTOR_NAME = "finpay-postgres-cdc-connector"
TOPIC_PREFIX = "finpay"
SLOT_NAME = "finpay_slot"


def build_connector_config(cfg: OltpSettings = settings) -> dict[str, Any]:
    """The connector's config never contains the password inline as a
    plaintext literal in source — it's read from the same environment-driven
    OltpSettings every other src/oltp module already uses (ultimately from
    .env), and this function's return value is only ever sent directly to
    Kafka Connect's REST API, never logged or written to a file (see
    docs/24 §9 for how to inspect the *registered* config safely)."""
    table_include_list = ",".join(f"{SCHEMA_NAME}.{t}" for t in PUBLISHED_TABLES)
    return {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": cfg.user,
            "database.password": cfg.password,
            "database.dbname": cfg.dbname,
            "topic.prefix": TOPIC_PREFIX,
            "schema.include.list": SCHEMA_NAME,
            "table.include.list": table_include_list,
            "plugin.name": "pgoutput",
            "publication.name": PUBLICATION_NAME,
            "publication.autocreate.mode": "disabled",  # the publication must already exist — see src/cdc/publication.py
            "slot.name": SLOT_NAME,
            "snapshot.mode": "initial",
            "decimal.handling.mode": "string",  # human-readable monetary values in the JSON payload, not base64 — see docs/24 §13
            "topic.creation.enable": "true",
            "topic.creation.default.partitions": "1",
            "topic.creation.default.replication.factor": "1",
        },
    }


def register_connector(cfg: OltpSettings = settings, connect_url: str = CONNECT_URL) -> requests.Response:
    return requests.post(f"{connect_url}/connectors", json=build_connector_config(cfg), timeout=30)


def connector_status(connect_url: str = CONNECT_URL, name: str = CONNECTOR_NAME) -> dict[str, Any]:
    response = requests.get(f"{connect_url}/connectors/{name}/status", timeout=10)
    response.raise_for_status()
    return response.json()


def connect_is_reachable(connect_url: str = CONNECT_URL) -> bool:
    try:
        response = requests.get(f"{connect_url}/connectors", timeout=5)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


def ensure_connector_registered(cfg: OltpSettings = settings, connect_url: str = CONNECT_URL) -> dict[str, Any]:
    """Idempotent: if the connector is already registered, leaves it running
    and returns its current status rather than re-POSTing (which Kafka
    Connect would reject with 409 Conflict anyway)."""
    existing = requests.get(f"{connect_url}/connectors/{CONNECTOR_NAME}", timeout=10)
    if existing.status_code == 200:
        return {"already_registered": True, "status": connector_status(connect_url)}

    response = register_connector(cfg, connect_url)
    response.raise_for_status()
    return {"already_registered": False, "registration_response": response.json()}
