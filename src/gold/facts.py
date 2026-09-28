"""Gold fact builders. Grain, keys, measures and additivity are documented
per fact (§8/§20-21 of this phase) — this is the authoritative source of
truth for docs/27 §9-13, not duplicated/invented there.

Fact-to-fact references (settlement->transaction_id, reconciliation->
settlement_id, reward->transaction_id, transaction_event->transaction_id):
kept as the SAME natural/business key used in the source OLTP schema (never
resolved to a surrogate transaction_key), because the "one" side is itself
a fact grain, not a dimension — a standard, documented star-schema pattern
(these are degenerate join keys, not conformed-dimension FKs).

country_key on fact_transactions is ALWAYS UNKNOWN_KEY (-1): verified in
this phase that silver_merchant.country_code is 0/521 non-null (docs/21 §14
open decision, unchanged) and CDC transactions carry no other country
signal. This is a real, verified absence — not an invented shortcut.
"""

from datetime import date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa

from src.gold.config import CDC_SILVER_ROOT, UNKNOWN_KEY
from src.gold.dimensions import date_key_of
from src.silver.common import read_parquet


def _cdc_silver(dataset: str) -> list[dict[str, Any]]:
    path = CDC_SILVER_ROOT / dataset / "data.parquet"
    return read_parquet(path) if path.exists() else []


def _read_event_log(dataset: str) -> list[dict[str, Any]]:
    """Event-log CDC Silver datasets are append-only across multiple
    run_<ts>.parquet files (docs/26 §2) — concatenated here, not a single
    data.parquet read."""
    d = CDC_SILVER_ROOT / dataset
    if not d.exists():
        return []
    rows: list[dict[str, Any]] = []
    for f in sorted(d.glob("run_*.parquet")):
        rows.extend(read_parquet(f))
    return rows


def _dk(value: date | datetime | None) -> int | None:
    if value is None:
        return None
    d = value.date() if isinstance(value, datetime) else value
    return date_key_of(d)


# ---- Cross-entity lookup chain: token_id -> cardholder_id -> enrolled_program_id -> client_id
# Built once from CDC Silver current-state natural keys (§ "Current Work" design). ----


def build_lookup_chains() -> dict[str, dict[str, str]]:
    card_tokens = _cdc_silver("silver_cdc_card_token")
    cardholders = _cdc_silver("silver_cdc_cardholder")
    programs = _cdc_silver("silver_cdc_program")

    token_to_cardholder = {r["token_id"]: r["cardholder_id"] for r in card_tokens}
    token_to_bin_range = {r["token_id"]: r["bin_range"] for r in card_tokens}
    cardholder_to_program = {r["cardholder_id"]: r["enrolled_program_id"] for r in cardholders}
    program_to_client = {r["program_id"]: r["client_id"] for r in programs}

    return {
        "token_to_cardholder": token_to_cardholder,
        "token_to_bin_range": token_to_bin_range,
        "cardholder_to_program": cardholder_to_program,
        "program_to_client": program_to_client,
    }


# ---- fact_transactions ----
# Grain: one row per transaction_id (silver_cdc_transaction current state).
# Source: silver_cdc_transaction (CDC Silver) joined to dim_merchant, dim_mcc,
# dim_currency, dim_card_token, dim_cardholder, dim_program, dim_client
# (API+CDC Silver convergence point, §7).
# Measures: amount (additive), mcc_confidence (non-additive, a quality score
# not a monetary fact — kept as a degenerate attribute, not a "measure").
# Degenerate dimensions: transaction_status, decline_reason, auth_code
# (kept directly on the fact — low cardinality, no separate junk dimension,
# a documented choice, not an oversight).

FACT_TRANSACTIONS_SCHEMA = pa.schema(
    [
        ("transaction_id", pa.string()),  # grain / degenerate dimension
        ("date_key", pa.int32()),
        ("merchant_key", pa.int64()),
        ("mcc_key", pa.int64()),
        ("currency_key", pa.int64()),
        ("card_token_key", pa.int64()),
        ("cardholder_key", pa.int64()),
        ("program_key", pa.int64()),
        ("client_key", pa.int64()),
        ("country_key", pa.int64()),  # always UNKNOWN_KEY today — see module docstring
        ("transaction_timestamp", pa.timestamp("us")),
        ("amount", pa.decimal128(18, 4)),
        ("mcc_confidence", pa.decimal128(18, 4)),
        ("transaction_status", pa.string()),
        ("decline_reason", pa.string()),
        ("auth_code", pa.string()),
    ]
)


def build_fact_transactions(
    merchant_registry: dict[str, int],
    mcc_registry: dict[str, int],
    currency_registry: dict[str, int],
    card_token_registry: dict[str, int],
    cardholder_registry: dict[str, int],
    program_registry: dict[str, int],
    client_registry: dict[str, int],
) -> list[dict[str, Any]]:
    source = _cdc_silver("silver_cdc_transaction")
    chains = build_lookup_chains()
    rows = []
    for r in source:
        token_id = r["token_id"]
        cardholder_id = chains["token_to_cardholder"].get(token_id)
        program_id = chains["cardholder_to_program"].get(cardholder_id) if cardholder_id else None
        client_id = chains["program_to_client"].get(program_id) if program_id else None
        rows.append(
            {
                "transaction_id": r["transaction_id"],
                "date_key": _dk(r["transaction_timestamp"]),
                "merchant_key": merchant_registry.get(r["merchant_id"], UNKNOWN_KEY),
                "mcc_key": mcc_registry.get(r["mcc_code"], UNKNOWN_KEY),
                "currency_key": currency_registry.get(r["currency_code"], UNKNOWN_KEY),
                "card_token_key": card_token_registry.get(token_id, UNKNOWN_KEY),
                "cardholder_key": cardholder_registry.get(cardholder_id, UNKNOWN_KEY) if cardholder_id else UNKNOWN_KEY,
                "program_key": program_registry.get(program_id, UNKNOWN_KEY) if program_id else UNKNOWN_KEY,
                "client_key": client_registry.get(client_id, UNKNOWN_KEY) if client_id else UNKNOWN_KEY,
                "country_key": UNKNOWN_KEY,
                "transaction_timestamp": r["transaction_timestamp"],
                "amount": r["amount"],
                "mcc_confidence": r["mcc_confidence"],
                "transaction_status": r["status"],
                "decline_reason": r["decline_reason"],
                "auth_code": r["auth_code"],
            }
        )
    return rows


# ---- fact_transaction_events ----
# Grain: one row per (event_id, kafka_offset) — NOT one row per event_id.
# Verified in this phase against real data: event_id is NOT unique in
# silver_cdc_transaction_event. docs/26's own design note says event_log
# rows are "keyed by their own natural OLTP PK... never updated/deleted in
# practice", but the S10 idempotency-test fixture (tests/test_oltp.py)
# repeatedly inserts+deletes the same hardcoded EVT-9990001 against the real
# live database every pytest session, producing genuine repeated (event_id,
# operation='c') CDC events with the SAME event_id across separate Kafka
# offsets. Assuming event_id uniqueness here would be inventing a grain the
# real source data does not have — so kafka_offset (already the dedup key
# S13's own consumer relies on) is added to make the grain actually unique.
#
# Delete markers (operation='d') are EXCLUDED: a delete of an append-only
# business event is not itself a business event, and (for the pre-REPLICA-
# IDENTITY-FULL rows specifically) carries blank business columns and an
# epoch (1970-01-01) event_timestamp that would corrupt dim_date and every
# degenerate attribute. This is a documented Gold-level filter, not a silent
# drop — reconciliation.py checks
# gold_fact_rows == silver_event_log_rows - delete_marker_rows exactly.
#
# Factless fact pattern: no monetary measure — event_count=1 lets any BI tool
# SUM() to get event counts without a special COUNT(DISTINCT) rule.
# transaction_id is a degenerate join key back to fact_transactions (fact-to-
# fact reference, not a conformed dimension FK — see module docstring).

FACT_TRANSACTION_EVENTS_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("kafka_offset", pa.int64()),
        ("transaction_id", pa.string()),
        ("date_key", pa.int32()),
        ("event_timestamp", pa.timestamp("us")),
        ("event_type", pa.string()),
        ("event_status", pa.string()),
        ("event_count", pa.int32()),
    ]
)


def build_fact_transaction_events() -> list[dict[str, Any]]:
    source = _read_event_log("silver_cdc_transaction_event")
    return [
        {
            "event_id": r["event_id"],
            "kafka_offset": r["kafka_offset"],
            "transaction_id": r["transaction_id"],
            "date_key": _dk(r["event_timestamp"]),
            "event_timestamp": r["event_timestamp"],
            "event_type": r["event_type"],
            "event_status": r["event_status"],
            "event_count": 1,
        }
        for r in source
        if r["operation"] != "d"
    ]


# ---- fact_settlements ----
# Grain: one row per settlement_id. Measures: settlement_amount, fee_amount,
# net_amount — all additive (each settlement is independent; summing across
# any dimension is valid, e.g. total fees per settlement_batch_id, which is
# kept as a degenerate dimension — no separate batch dimension table, since
# it carries no attributes beyond its own id in current source data).

FACT_SETTLEMENTS_SCHEMA = pa.schema(
    [
        ("settlement_id", pa.string()),
        ("transaction_id", pa.string()),
        ("settlement_batch_id", pa.string()),
        ("date_key", pa.int32()),
        ("currency_key", pa.int64()),
        ("settlement_amount", pa.decimal128(18, 4)),
        ("fee_amount", pa.decimal128(18, 4)),
        ("net_amount", pa.decimal128(18, 4)),
    ]
)


def build_fact_settlements(currency_registry: dict[str, int]) -> list[dict[str, Any]]:
    source = _cdc_silver("silver_cdc_settlement")
    return [
        {
            "settlement_id": r["settlement_id"],
            "transaction_id": r["transaction_id"],
            "settlement_batch_id": r["settlement_batch_id"],
            "date_key": _dk(r["settlement_date"]),
            "currency_key": currency_registry.get(r["settlement_currency"], UNKNOWN_KEY),
            "settlement_amount": r["settlement_amount"],
            "fee_amount": r["fee_amount"],
            "net_amount": r["net_amount"],
        }
        for r in source
    ]


# ---- fact_reconciliation ----
# Grain: one row per reconciliation_id. Measures: expected_amount,
# actual_amount are additive; variance is additive as stored (it is just
# actual - expected, a real column) but is semantically a DIFFERENCE, so
# summing it across many rows answers "net over/under" rather than a
# meaningful "total variance" — documented so a BI consumer doesn't
# misread a SUM(variance) as a magnitude-of-error metric.

FACT_RECONCILIATION_SCHEMA = pa.schema(
    [
        ("reconciliation_id", pa.string()),
        ("settlement_id", pa.string()),
        ("date_key", pa.int32()),
        ("match_status", pa.string()),
        ("expected_amount", pa.decimal128(18, 4)),
        ("actual_amount", pa.decimal128(18, 4)),
        ("variance", pa.decimal128(18, 4)),
    ]
)


def build_fact_reconciliation() -> list[dict[str, Any]]:
    source = _cdc_silver("silver_cdc_reconciliation")
    return [
        {
            "reconciliation_id": r["reconciliation_id"],
            "settlement_id": r["settlement_id"],
            "date_key": _dk(r["reconciled_date"]),
            "match_status": r["match_status"],
            "expected_amount": r["expected_amount"],
            "actual_amount": r["actual_amount"],
            "variance": r["variance"],
        }
        for r in source
    ]


# ---- fact_rewards ----
# Grain: one row per (reward_id, kafka_offset) — same real-data justification
# as fact_transaction_events above (verified: reward_id RWD-9990001 repeats
# across separate kafka_offsets from the same S10 idempotency-test fixture).
# Delete markers (operation='d') are EXCLUDED for the same reason as above —
# reconciliation.py checks the exact row-count identity against source minus
# deletes.
# Measure: reward_amount (additive within a given qualification_status;
# NOT_QUALIFIED rows carry a real, verified 0.00 amount in source data — not
# assumed — so a plain SUM(reward_amount) is always correct without needing
# a WHERE/CASE filter).

FACT_REWARDS_SCHEMA = pa.schema(
    [
        ("reward_id", pa.string()),
        ("kafka_offset", pa.int64()),
        ("transaction_id", pa.string()),
        ("offer_key", pa.int64()),
        ("date_key", pa.int32()),
        ("currency_key", pa.int64()),
        ("qualification_status", pa.string()),
        ("reward_amount", pa.decimal128(18, 4)),
    ]
)


def build_fact_rewards(offer_registry: dict[str, int], currency_registry: dict[str, int]) -> list[dict[str, Any]]:
    source = _read_event_log("silver_cdc_reward_event")
    return [
        {
            "reward_id": r["reward_id"],
            "kafka_offset": r["kafka_offset"],
            "transaction_id": r["transaction_id"],
            "offer_key": offer_registry.get(r["offer_id"], UNKNOWN_KEY),
            "date_key": _dk(r["event_timestamp"]),
            "currency_key": currency_registry.get(r["reward_currency"], UNKNOWN_KEY),
            "qualification_status": r["qualification_status"],
            "reward_amount": r["reward_amount"],
        }
        for r in source
        if r["operation"] != "d"
    ]
