# 17. Security Boundary

What is a secret, what isn't, and where the line is enforced.

```mermaid
flowchart TB
    subgraph PROTECTED["Local-only — never committed (gitignored)"]
        ENV[(".env<br/>real POSTGRES_*/AIRFLOW_* values")]
        AFPW[("docker/airflow/simple_auth_manager_passwords.json<br/>real Airflow admin password")]
        VENVS[(".venv/ .venv-spark/ .venv-dbt/<br/>local Python environments")]
        CACHE[("__pycache__/ .pytest_cache/ dbt/target/<br/>regenerable local state")]
        GENDATA[("data/*, outbox/*<br/>generated pipeline output")]
    end
    subgraph SOURCE["Source-controlled — safe to publish"]
        ENVEX[(".env.example<br/>variable NAMES only, no values")]
        CODE["src/, dags/, dbt/, tests/, scripts/"]
        CONF["configs/ — dataset registry, client entitlements<br/>(no credentials, no real customer data)"]
        DOCKERCFG["docker-compose.yml<br/>reads secrets via \${VAR} substitution only"]
        DOCS["docs/, architecture/, README.md"]
    end

    ENV -.->|"\${POSTGRES_PASSWORD} etc."| DOCKERCFG
    ENVEX -.->|documents names, never values| ENV
```

**Enforced how**: `.gitignore` excludes `.env`, the Airflow password file, all virtual
environments, and every generated data directory by name. `docker-compose.yml` never
contains a literal credential — every password/secret field is a `${VAR}` substitution
resolved from `.env` at container-start time. No API used by this project (OpenStreetMap
Overpass, Frankfurter, GLEIF) requires a key at all, so there is no API credential to
manage in the first place. A dedicated repository and git-history audit confirmed no
secret has ever been committed.
