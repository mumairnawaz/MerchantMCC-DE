"""Environment-driven PostgreSQL connection settings.

Follows the same stdlib-only .env convention already established by
src/ingestion/settings.py (a small, auditable loader instead of adding
python-dotenv as a dependency — the loader is duplicated here, not imported
from src.ingestion, to keep src/oltp independent of the ingestion layer, same
as src/silver and src/synthetic already are).

docker/docker-compose.yml and .env already exist in this repository (reused,
not recreated, per this phase's instruction to reuse existing Docker
structure) and define POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_DB/POSTGRES_PORT.
The database is named "merchantmcc" (not "finpay_oltp" as this phase's prompt
suggested as an example) because that infrastructure already existed before
this phase began — see docs/23 §4 for the full discussion of this naming
discrepancy, reported rather than silently resolved either way.
"""

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"


def _load_dotenv(path: Path = ENV_FILE) -> None:
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


@dataclass(frozen=True)
class OltpSettings:
    host: str = os.environ.get("POSTGRES_HOST", "localhost")
    port: int = int(os.environ.get("POSTGRES_PORT", "55432"))
    dbname: str = os.environ.get("POSTGRES_DB", "merchantmcc")
    user: str = os.environ.get("POSTGRES_USER", "merchantmcc")
    password: str = os.environ.get("POSTGRES_PASSWORD", "")
    schema: str = "finpay"

    def conninfo(self) -> str:
        """A psycopg-compatible conninfo string. Never logged/printed with the
        password in place — see src/oltp/database.py's connect() docstring."""
        return f"host={self.host} port={self.port} dbname={self.dbname} user={self.user} password={self.password}"


settings = OltpSettings()
