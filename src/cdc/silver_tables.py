"""Per-table business column specs for CDC Silver, driven from the real
src/oltp/schema.py DDL (S10) and the real CDC Bronze `after`/`before` JSON
keys (verified in this phase, not assumed — docs/26 §2).

DESIGN NOTE (§4/§26 of this phase — documented, not silent): unlike the
original API-sourced Silver modules (src/silver/mcc.py etc.), which are
genuinely different hand-written modules because each source has a distinct
raw shape and distinct business rules, all 11 CDC tables share exactly ONE
raw shape (the Debezium envelope) and differ ONLY in their column list/types
— which are already fully known and fixed from src/oltp/schema.py's DDL.
A single metadata-driven engine (src/cdc/silver.py) processes every table
uniformly from this table, rather than 11 near-identical hand-written
modules — a deliberate, different-but-justified adaptation to a genuinely
more uniform problem shape, not a departure from "reuse existing patterns"
for its own sake.

CURRENT-STATE vs EVENT-LOG decision (§4/§6, documented): 9 tables are
"current state" master/business entities (upserted, keyed by PK, latest-by-
source.lsn wins) — clients, programs, campaigns, offers, cardholders,
card_tokens, transactions, settlements, reconciliation. 2 tables
(transaction_events, reward_events) are inherently event logs already in
OLTP itself (one immutable row per business event, never updated/deleted in
practice) — modeled as pure append, keyed by their own natural OLTP PK.
A dedicated *typed* "transaction history" dataset (distinct from the
current-state `transaction` table) was considered and explicitly deferred:
CDC Bronze (S12) already permanently retains the complete raw event history
for every table, including transactions — nothing is lost by not duplicating
it into a second typed Silver shape now. A reasonable, well-scoped future
addition, not a silent gap (see docs/26 §18 open decisions).
"""

from dataclasses import dataclass
from typing import Any, Callable

import pyarrow as pa

from src.cdc.silver_types import as_string, debezium_date_to_date, debezium_decimal_string_to_decimal, debezium_micros_to_datetime


@dataclass
class Column:
    name: str
    converter: Callable[[Any], Any]
    pa_type: pa.DataType
    required: bool = False
    positive: bool = False
    valid_values: frozenset[str] | None = None


def _s(name, required=False, valid_values=None):
    return Column(name, as_string, pa.string(), required=required, valid_values=frozenset(valid_values) if valid_values else None)


def _d(name, required=False, positive=False):
    return Column(name, debezium_decimal_string_to_decimal, pa.decimal128(18, 4), required=required, positive=positive)


def _date(name, required=False):
    return Column(name, debezium_date_to_date, pa.date32(), required=required)


def _ts(name, required=False):
    return Column(name, debezium_micros_to_datetime, pa.timestamp("us"), required=required)


@dataclass
class TableSpec:
    bronze_table: str  # matches data/bronze_cdc/<bronze_table>/
    silver_dataset: str
    pk_fields: tuple[str, ...]
    columns: tuple[Column, ...]
    mode: str  # "current_state" | "event_log"


TABLE_SPECS: dict[str, TableSpec] = {
    "clients": TableSpec(
        bronze_table="clients",
        silver_dataset="silver_cdc_client",
        pk_fields=("client_id",),
        mode="current_state",
        columns=(
            _s("client_id", required=True),
            _s("lei", required=True),
            _s("legal_name", required=True),
            _s("client_type", required=True, valid_values={"ISSUER", "NETWORK", "PROGRAM_OWNER"}),
            _s("country_code"),
            _date("onboarding_date", required=True),
            _s("status", required=True),
            _s("source_type", required=True),
        ),
    ),
    "programs": TableSpec(
        bronze_table="programs",
        silver_dataset="silver_cdc_program",
        pk_fields=("program_id",),
        mode="current_state",
        columns=(
            _s("program_id", required=True),
            _s("client_id", required=True),
            _s("program_name", required=True),
            _s("program_type", required=True, valid_values={"CASHBACK", "LOYALTY", "CO_BRAND"}),
            _s("currency_code", required=True),
            _s("status", required=True),
            _date("start_date", required=True),
            _s("source_type", required=True),
        ),
    ),
    "campaigns": TableSpec(
        bronze_table="campaigns",
        silver_dataset="silver_cdc_campaign",
        pk_fields=("campaign_id",),
        mode="current_state",
        columns=(
            _s("campaign_id", required=True),
            _s("program_id", required=True),
            _s("campaign_name", required=True),
            _date("start_date", required=True),
            _date("end_date", required=True),
            _s("status", required=True, valid_values={"ACTIVE", "ENDED", "SCHEDULED"}),
            _s("source_type", required=True),
        ),
    ),
    "offers": TableSpec(
        bronze_table="offers",
        silver_dataset="silver_cdc_offer",
        pk_fields=("offer_id",),
        mode="current_state",
        columns=(
            _s("offer_id", required=True),
            _s("campaign_id", required=True),
            _s("eligible_mcc_code"),
            _s("offer_type", required=True, valid_values={"CASHBACK_PCT", "FIXED_AMOUNT"}),
            _d("offer_value", required=True, positive=True),
            _d("min_transaction_amount", required=True, positive=True),
            _s("currency_code", required=True),
            _s("status", required=True),
            _date("valid_from", required=True),
            _date("valid_to", required=True),
            _s("source_type", required=True),
        ),
    ),
    "cardholders": TableSpec(
        bronze_table="cardholders",
        silver_dataset="silver_cdc_cardholder",
        pk_fields=("cardholder_id",),
        mode="current_state",
        columns=(
            _s("cardholder_id", required=True),
            _s("pseudonym", required=True),
            _s("country_code", required=True),
            _s("enrolled_program_id", required=True),
            _date("enrollment_date", required=True),
            _s("source_type", required=True),
        ),
    ),
    "card_tokens": TableSpec(
        bronze_table="card_tokens",
        silver_dataset="silver_cdc_card_token",
        pk_fields=("token_id",),
        mode="current_state",
        columns=(
            _s("token_id", required=True),
            _s("cardholder_id", required=True),
            _s("bin_range", required=True),
            _s("card_brand", required=True),
            _s("token_status", required=True, valid_values={"ACTIVE", "SUSPENDED", "EXPIRED"}),
            _date("issued_date", required=True),
            _s("source_type", required=True),
        ),
    ),
    "transactions": TableSpec(
        bronze_table="transactions",
        silver_dataset="silver_cdc_transaction",
        pk_fields=("transaction_id",),
        mode="current_state",
        columns=(
            _s("transaction_id", required=True),
            _s("token_id", required=True),
            _s("merchant_id", required=True),
            _s("mcc_code", required=True),
            _d("mcc_confidence", required=True),
            _s("currency_code", required=True),
            _d("amount", required=True, positive=True),
            _ts("transaction_timestamp", required=True),
            _s("status", required=True, valid_values={"APPROVED", "DECLINED"}),
            _s("decline_reason"),
            _s("auth_code"),
            _s("source_type", required=True),
        ),
    ),
    "settlements": TableSpec(
        bronze_table="settlements",
        silver_dataset="silver_cdc_settlement",
        pk_fields=("settlement_id",),
        mode="current_state",
        columns=(
            _s("settlement_id", required=True),
            _s("transaction_id", required=True),
            _s("settlement_batch_id", required=True),
            _date("settlement_date", required=True),
            _d("settlement_amount", required=True, positive=True),
            _s("settlement_currency", required=True),
            _d("fee_amount", required=True),
            _d("net_amount", required=True, positive=True),
            _s("source_type", required=True),
        ),
    ),
    "reconciliation": TableSpec(
        bronze_table="reconciliation",
        silver_dataset="silver_cdc_reconciliation",
        pk_fields=("reconciliation_id",),
        mode="current_state",
        columns=(
            _s("reconciliation_id", required=True),
            _s("settlement_id", required=True),
            _d("expected_amount", required=True),
            _d("actual_amount", required=True),
            _d("variance", required=True),
            _s("match_status", required=True, valid_values={"MATCHED", "EXCEPTION"}),
            _date("reconciled_date", required=True),
            _s("source_type", required=True),
        ),
    ),
    "transaction_events": TableSpec(
        bronze_table="transaction_events",
        silver_dataset="silver_cdc_transaction_event",
        pk_fields=("event_id",),
        mode="event_log",
        columns=(
            _s("event_id", required=True),
            _s("transaction_id", required=True),
            _s("event_type", required=True, valid_values={"AUTHORIZATION", "CAPTURE", "REVERSAL"}),
            _ts("event_timestamp", required=True),
            _s("event_status", required=True),
            _s("source_type", required=True),
        ),
    ),
    "reward_events": TableSpec(
        bronze_table="reward_events",
        silver_dataset="silver_cdc_reward_event",
        pk_fields=("reward_id",),
        mode="event_log",
        columns=(
            _s("reward_id", required=True),
            _s("transaction_id", required=True),
            _s("offer_id", required=True),
            _s("qualification_status", required=True, valid_values={"QUALIFIED", "NOT_QUALIFIED"}),
            _d("reward_amount", required=True),
            _s("reward_currency", required=True),
            _ts("event_timestamp", required=True),
            _s("source_type", required=True),
        ),
    ),
}
