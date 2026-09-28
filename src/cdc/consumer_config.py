"""Kafka consumer configuration for the S12 CDC Bronze consumer.

Deliberately named `consumer_config.py`, not `config.py` — src/cdc/connector.py
already defines Kafka Connect's own bootstrap address (`kafka:9092`, reachable
from *inside* the Docker network by the `connect` container). This is a
different address on purpose: `localhost:29092`, the new host-exposed
"EXTERNAL" listener added in S12 specifically so a Python process running on
the host (this consumer) can reach the broker — see docker-compose.yml's
`kafka` service comment and docs/25 §4.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"


def _load_dotenv(path: Path = ENV_FILE) -> None:
    """Same stdlib-only .env convention as src/oltp/config.py, duplicated
    (not imported) to keep src/cdc independent of src/oltp — same reasoning
    src/oltp itself already applied relative to src/ingestion."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip()


_load_dotenv()

CDC_TOPICS: list[str] = [
    "finpay.finpay.clients",
    "finpay.finpay.programs",
    "finpay.finpay.campaigns",
    "finpay.finpay.offers",
    "finpay.finpay.cardholders",
    "finpay.finpay.card_tokens",
    "finpay.finpay.transactions",
    "finpay.finpay.transaction_events",
    "finpay.finpay.settlements",
    "finpay.finpay.reconciliation",
    "finpay.finpay.reward_events",
]


@dataclass(frozen=True)
class CdcConsumerSettings:
    bootstrap_servers: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    group_id: str = os.environ.get("CDC_CONSUMER_GROUP_ID", "finpay-cdc-bronze-consumer")
    topics: list[str] = field(default_factory=lambda: list(CDC_TOPICS))
    poll_timeout_seconds: float = 5.0
    # How long with no new message before a run() call considers the topic
    # drained and stops — this is a finite BATCH consumer (matching every
    # other src/ingestion/*.py run() in this project: do the available work,
    # return a report, exit), not a perpetual streaming daemon.
    idle_timeout_seconds: float = 10.0


settings = CdcConsumerSettings()
