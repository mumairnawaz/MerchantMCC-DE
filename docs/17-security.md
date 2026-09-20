# 17 — Security

## Secrets

`.env` (gitignored) holds local secrets — currently only the local PostgreSQL password.
`.env.example` documents required variable names with placeholder values only, never real
credentials. None of the three live APIs (OSM, Frankfurter, GLEIF) require a key, verified
live for all three — so there is currently no API credential to manage at all.

## What `.gitignore` protects

```
.env
.venv/
__pycache__/
.pytest_cache/
logs/
data/
*.log
*.key
*.pem
*credentials*
*secret*
```

## What this first commit does not contain

No `.env` file, no API keys, no tokens, no passwords, no database connection strings, no
downloaded Bronze data, and no confidential company/employer information of any kind.

## Principle

Secret management is demonstrated from the project's very first commit, not retrofitted
once real credentials exist.
