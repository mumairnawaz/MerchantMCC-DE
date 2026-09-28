"""FinPay OLTP schema: DDL + the real foreign-key dependency graph.

TABLE_DEPENDENCIES is the single source of truth for load order — both
init_schema() (via DDL_STATEMENTS, which must already be written in a valid
order since PostgreSQL enforces FK-target-exists-first) and
src/oltp/loader.py derive their table order from load_order() below, which
topologically sorts this graph. Nothing hardcodes "clients before programs
before campaigns..." as a second, possibly-drifting list — see §8 of
docs/23-oltp-postgresql-design.md for the derivation.

Every CHECK constraint here already existed as a Python-level rule enforced by
src/synthetic/validation.py in S9 — none are new/invented financial rules,
they're the same rules now also enforced at the database layer (belt-and-
braces, standard OLTP practice), per this phase's "do not invent financial
rules that aren't supported by the S9 design."

merchant_id/mcc_code/currency_code/country_code on synthetic tables reference
real Silver dimensions living in a completely separate storage layer (Parquet,
not this database) — they are NOT declared as SQL foreign keys here, since
PostgreSQL cannot enforce a constraint against data it doesn't hold. Referential
validity against Silver is verified by src/oltp/validation.py after load,
exactly as it already was by src/synthetic/validation.py in S9.
"""

from typing import Iterable

SCHEMA_NAME = "finpay"

# table_name -> set of tables it has a FK to (within this schema only)
TABLE_DEPENDENCIES: dict[str, set[str]] = {
    "clients": set(),
    "programs": {"clients"},
    "campaigns": {"programs"},
    "offers": {"campaigns"},
    "cardholders": {"programs"},
    "card_tokens": {"cardholders"},
    "transactions": {"card_tokens"},
    "transaction_events": {"transactions"},
    "settlements": {"transactions"},
    "reconciliation": {"settlements"},
    "reward_events": {"transactions", "offers"},
}


def load_order() -> list[str]:
    """Topological sort (Kahn's algorithm) over TABLE_DEPENDENCIES — the real
    dependency order, not a hand-maintained list that could silently drift
    from the actual foreign keys declared above."""
    remaining = dict(TABLE_DEPENDENCIES)
    ordered: list[str] = []
    while remaining:
        ready = sorted(name for name, deps in remaining.items() if deps <= set(ordered))
        if not ready:
            raise RuntimeError(f"src.oltp.schema: circular or unresolved table dependency among {sorted(remaining)}")
        ordered.extend(ready)
        for name in ready:
            del remaining[name]
    return ordered


DDL_STATEMENTS: list[str] = [
    f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};",
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.clients (
        client_id       VARCHAR(10) PRIMARY KEY,
        lei              CHAR(20) NOT NULL UNIQUE,
        legal_name       TEXT NOT NULL,
        client_type      VARCHAR(20) NOT NULL CHECK (client_type IN ('ISSUER','NETWORK','PROGRAM_OWNER')),
        country_code     CHAR(2),
        onboarding_date  DATE NOT NULL,
        status           VARCHAR(10) NOT NULL,
        source_type      VARCHAR(10) NOT NULL DEFAULT 'synthetic' CHECK (source_type = 'synthetic')
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.programs (
        program_id     VARCHAR(10) PRIMARY KEY,
        client_id       VARCHAR(10) NOT NULL REFERENCES {SCHEMA_NAME}.clients(client_id),
        program_name    TEXT NOT NULL,
        program_type    VARCHAR(20) NOT NULL CHECK (program_type IN ('CASHBACK','LOYALTY','CO_BRAND')),
        currency_code   CHAR(3) NOT NULL,
        status          VARCHAR(10) NOT NULL,
        start_date      DATE NOT NULL,
        source_type     VARCHAR(10) NOT NULL DEFAULT 'synthetic' CHECK (source_type = 'synthetic')
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.campaigns (
        campaign_id     VARCHAR(10) PRIMARY KEY,
        program_id       VARCHAR(10) NOT NULL REFERENCES {SCHEMA_NAME}.programs(program_id),
        campaign_name     TEXT NOT NULL,
        start_date         DATE NOT NULL,
        end_date            DATE NOT NULL CHECK (end_date >= start_date),
        status                VARCHAR(10) NOT NULL CHECK (status IN ('ACTIVE','ENDED','SCHEDULED')),
        source_type            VARCHAR(10) NOT NULL DEFAULT 'synthetic' CHECK (source_type = 'synthetic')
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.offers (
        offer_id                 VARCHAR(10) PRIMARY KEY,
        campaign_id                VARCHAR(10) NOT NULL REFERENCES {SCHEMA_NAME}.campaigns(campaign_id),
        eligible_mcc_code            CHAR(4),
        offer_type                     VARCHAR(20) NOT NULL CHECK (offer_type IN ('CASHBACK_PCT','FIXED_AMOUNT')),
        offer_value                     NUMERIC(18,4) NOT NULL CHECK (offer_value > 0),
        min_transaction_amount            NUMERIC(18,2) NOT NULL CHECK (min_transaction_amount > 0),
        currency_code                       CHAR(3) NOT NULL,
        status                                 VARCHAR(10) NOT NULL,
        valid_from                               DATE NOT NULL,
        valid_to                                   DATE NOT NULL CHECK (valid_to >= valid_from),
        source_type                                  VARCHAR(10) NOT NULL DEFAULT 'synthetic' CHECK (source_type = 'synthetic')
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.cardholders (
        cardholder_id          VARCHAR(10) PRIMARY KEY,
        pseudonym                TEXT NOT NULL,
        country_code               CHAR(2) NOT NULL,
        enrolled_program_id          VARCHAR(10) NOT NULL REFERENCES {SCHEMA_NAME}.programs(program_id),
        enrollment_date                DATE NOT NULL,
        source_type                      VARCHAR(10) NOT NULL DEFAULT 'synthetic' CHECK (source_type = 'synthetic')
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.card_tokens (
        token_id         VARCHAR(24) PRIMARY KEY,
        cardholder_id      VARCHAR(10) NOT NULL REFERENCES {SCHEMA_NAME}.cardholders(cardholder_id),
        bin_range             CHAR(6) NOT NULL,
        card_brand              TEXT NOT NULL,
        token_status              VARCHAR(10) NOT NULL CHECK (token_status IN ('ACTIVE','SUSPENDED','EXPIRED')),
        issued_date                 DATE NOT NULL,
        source_type                   VARCHAR(10) NOT NULL DEFAULT 'synthetic' CHECK (source_type = 'synthetic')
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.transactions (
        transaction_id          VARCHAR(12) PRIMARY KEY,
        token_id                  VARCHAR(24) NOT NULL REFERENCES {SCHEMA_NAME}.card_tokens(token_id),
        merchant_id                 TEXT NOT NULL,   -- real silver_merchant.merchant_id (cross-layer, not FK-enforceable here)
        mcc_code                      CHAR(4) NOT NULL, -- real silver_mcc.mcc_code (cross-layer)
        mcc_confidence                  NUMERIC(3,2) NOT NULL CHECK (mcc_confidence BETWEEN 0 AND 1),
        currency_code                     CHAR(3) NOT NULL, -- real silver_iso_currency.currency_code (cross-layer)
        amount                              NUMERIC(18,2) NOT NULL CHECK (amount > 0),
        transaction_timestamp                 TIMESTAMP NOT NULL,
        status                                   VARCHAR(10) NOT NULL CHECK (status IN ('APPROVED','DECLINED')),
        decline_reason                            VARCHAR(30),
        auth_code                                   VARCHAR(6),
        source_type                                   VARCHAR(10) NOT NULL DEFAULT 'synthetic' CHECK (source_type = 'synthetic'),
        CHECK ((status = 'APPROVED' AND decline_reason IS NULL) OR (status = 'DECLINED' AND decline_reason IS NOT NULL))
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.transaction_events (
        event_id           VARCHAR(12) PRIMARY KEY,
        transaction_id        VARCHAR(12) NOT NULL REFERENCES {SCHEMA_NAME}.transactions(transaction_id),
        event_type               VARCHAR(20) NOT NULL CHECK (event_type IN ('AUTHORIZATION','CAPTURE','REVERSAL')),
        event_timestamp             TIMESTAMP NOT NULL,
        event_status                   VARCHAR(20) NOT NULL,
        source_type                       VARCHAR(10) NOT NULL DEFAULT 'synthetic' CHECK (source_type = 'synthetic')
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.settlements (
        settlement_id          VARCHAR(12) PRIMARY KEY,
        transaction_id            VARCHAR(12) NOT NULL UNIQUE REFERENCES {SCHEMA_NAME}.transactions(transaction_id),
        settlement_batch_id          TEXT NOT NULL,
        settlement_date                 DATE NOT NULL,
        settlement_amount                 NUMERIC(18,2) NOT NULL CHECK (settlement_amount > 0),
        settlement_currency                 CHAR(3) NOT NULL,
        fee_amount                             NUMERIC(18,2) NOT NULL CHECK (fee_amount >= 0),
        net_amount                               NUMERIC(18,2) NOT NULL CHECK (net_amount > 0),
        source_type                                 VARCHAR(10) NOT NULL DEFAULT 'synthetic' CHECK (source_type = 'synthetic'),
        CHECK (net_amount = settlement_amount - fee_amount)
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.reconciliation (
        reconciliation_id     VARCHAR(12) PRIMARY KEY,
        settlement_id            VARCHAR(12) NOT NULL UNIQUE REFERENCES {SCHEMA_NAME}.settlements(settlement_id),
        expected_amount             NUMERIC(18,2) NOT NULL,
        actual_amount                 NUMERIC(18,2) NOT NULL,
        variance                        NUMERIC(18,2) NOT NULL,
        match_status                      VARCHAR(10) NOT NULL CHECK (match_status IN ('MATCHED','EXCEPTION')),
        reconciled_date                      DATE NOT NULL,
        source_type                             VARCHAR(10) NOT NULL DEFAULT 'synthetic' CHECK (source_type = 'synthetic'),
        CHECK (variance = actual_amount - expected_amount),
        CHECK ((match_status = 'MATCHED' AND variance = 0) OR (match_status = 'EXCEPTION' AND variance <> 0))
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.reward_events (
        reward_id                VARCHAR(12) PRIMARY KEY,
        transaction_id              VARCHAR(12) NOT NULL REFERENCES {SCHEMA_NAME}.transactions(transaction_id),
        offer_id                       VARCHAR(10) NOT NULL REFERENCES {SCHEMA_NAME}.offers(offer_id),
        qualification_status             VARCHAR(15) NOT NULL CHECK (qualification_status IN ('QUALIFIED','NOT_QUALIFIED')),
        reward_amount                       NUMERIC(18,2) NOT NULL CHECK (reward_amount >= 0),
        reward_currency                        CHAR(3) NOT NULL,
        event_timestamp                          TIMESTAMP NOT NULL,
        source_type                                 VARCHAR(10) NOT NULL DEFAULT 'synthetic' CHECK (source_type = 'synthetic'),
        CHECK ((qualification_status = 'QUALIFIED' AND reward_amount > 0) OR (qualification_status = 'NOT_QUALIFIED' AND reward_amount = 0))
    );
    """,
]

# OLTP lookup indexes only — justified by a realistic operational access
# pattern (see docs/23 §11 for the per-index rationale). settlements.
# transaction_id and reconciliation.settlement_id are already UNIQUE above,
# which Postgres auto-indexes — no separate index needed for those.
INDEX_STATEMENTS: list[str] = [
    f"CREATE INDEX IF NOT EXISTS idx_transactions_token_id ON {SCHEMA_NAME}.transactions(token_id);",
    f"CREATE INDEX IF NOT EXISTS idx_transactions_merchant_id ON {SCHEMA_NAME}.transactions(merchant_id);",
    f"CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON {SCHEMA_NAME}.transactions(transaction_timestamp);",
    f"CREATE INDEX IF NOT EXISTS idx_transaction_events_transaction_id ON {SCHEMA_NAME}.transaction_events(transaction_id);",
    f"CREATE INDEX IF NOT EXISTS idx_reward_events_transaction_id ON {SCHEMA_NAME}.reward_events(transaction_id);",
    f"CREATE INDEX IF NOT EXISTS idx_card_tokens_cardholder_id ON {SCHEMA_NAME}.card_tokens(cardholder_id);",
]


def all_tables() -> list[str]:
    return load_order()
