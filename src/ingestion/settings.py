"""Centralized, environment-driven ingestion settings.

Deliberately stdlib-only. A ~15-line .env loader is used here instead of adding
python-dotenv as a dependency — none of this project's values are secret (the
one real secret, the local Postgres password, is only ever consumed by Docker
Compose's own --env-file flag, never by Python), so a small auditable loader is
enough. Revisit if configuration needs grow past this.

This module holds cross-cutting settings only (timeout, User-Agent, Bronze root,
logging, environment name, optional future API keys). Per-source structural
config (URLs, OSM query, GLEIF pagination/scope) stays in configs/sources.json,
matching the existing convention each ingestion module already reads from.
"""

import logging
import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"


def _load_dotenv(path: Path = ENV_FILE) -> None:
    """Populate os.environ from a .env file without overriding already-set vars."""
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


def _get_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


@dataclass(frozen=True)
class IngestionSettings:
    """Read once at import time. All three live APIs are keyless (verified live) —
    the optional key fields exist only so a future source can be added without a
    code change, never as placeholders for a currently-required credential.
    """

    environment: str = os.environ.get("FINPAY_ENV", "local")
    request_timeout_seconds: int = _get_int("FINPAY_REQUEST_TIMEOUT_SECONDS", 30)
    user_agent: str = os.environ.get(
        "FINPAY_USER_AGENT",
        "FinPay-Merchant-Intelligence/0.1 "
        "(portfolio project; non-commercial; https://github.com/mumairnawaz/MerchantMCC-DE)",
    )
    bronze_root: Path = Path(os.environ.get("FINPAY_BRONZE_ROOT", "data/bronze"))
    log_level: str = os.environ.get("FINPAY_LOG_LEVEL", "INFO")

    # Optional, currently unused — no key is required for OSM, Frankfurter, or GLEIF.
    gleif_api_key: str | None = os.environ.get("GLEIF_API_KEY") or None


settings = IngestionSettings()


def get_logger(name: str) -> logging.Logger:
    """Return a logger configured from `settings.log_level`. Existing modules still
    use print(); this is available for new/updated modules to adopt incrementally,
    not a retrofit of existing ingestion code in this step.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(settings.log_level)
    return logger
