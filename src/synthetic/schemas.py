"""Explicit PyArrow schemas for every synthetic OLTP table — same discipline as
src/silver/fx_rate.py, merchant.py, legal_entity.py: identifiers stay strings,
monetary fields are explicit, no bare type inference."""

import pyarrow as pa

_S = pa.string()

CLIENTS_SCHEMA = pa.schema(
    [("client_id", _S), ("lei", _S), ("legal_name", _S), ("client_type", _S), ("country_code", _S), ("onboarding_date", _S), ("status", _S), ("source_type", _S)]
)

PROGRAMS_SCHEMA = pa.schema(
    [("program_id", _S), ("client_id", _S), ("program_name", _S), ("program_type", _S), ("currency_code", _S), ("status", _S), ("start_date", _S), ("source_type", _S)]
)

CAMPAIGNS_SCHEMA = pa.schema(
    [("campaign_id", _S), ("program_id", _S), ("campaign_name", _S), ("start_date", _S), ("end_date", _S), ("status", _S), ("source_type", _S)]
)

OFFERS_SCHEMA = pa.schema(
    [
        ("offer_id", _S),
        ("campaign_id", _S),
        ("eligible_mcc_code", _S),
        ("offer_type", _S),
        ("offer_value", pa.float64()),
        ("min_transaction_amount", pa.float64()),
        ("currency_code", _S),
        ("status", _S),
        ("valid_from", _S),
        ("valid_to", _S),
        ("source_type", _S),
    ]
)

CARDHOLDERS_SCHEMA = pa.schema(
    [("cardholder_id", _S), ("pseudonym", _S), ("country_code", _S), ("enrolled_program_id", _S), ("enrollment_date", _S), ("source_type", _S)]
)

CARD_TOKENS_SCHEMA = pa.schema(
    [("token_id", _S), ("cardholder_id", _S), ("bin_range", _S), ("card_brand", _S), ("token_status", _S), ("issued_date", _S), ("source_type", _S)]
)

TRANSACTIONS_SCHEMA = pa.schema(
    [
        ("transaction_id", _S),
        ("token_id", _S),
        ("merchant_id", _S),
        ("mcc_code", _S),
        ("mcc_confidence", pa.float64()),
        ("currency_code", _S),
        ("amount", pa.float64()),
        ("transaction_timestamp", _S),
        ("status", _S),
        ("decline_reason", _S),
        ("auth_code", _S),
        ("source_type", _S),
    ]
)

TRANSACTION_EVENTS_SCHEMA = pa.schema(
    [("event_id", _S), ("transaction_id", _S), ("event_type", _S), ("event_timestamp", _S), ("event_status", _S), ("source_type", _S)]
)

SETTLEMENTS_SCHEMA = pa.schema(
    [
        ("settlement_id", _S),
        ("transaction_id", _S),
        ("settlement_batch_id", _S),
        ("settlement_date", _S),
        ("settlement_amount", pa.float64()),
        ("settlement_currency", _S),
        ("fee_amount", pa.float64()),
        ("net_amount", pa.float64()),
        ("source_type", _S),
    ]
)

RECONCILIATION_SCHEMA = pa.schema(
    [
        ("reconciliation_id", _S),
        ("settlement_id", _S),
        ("expected_amount", pa.float64()),
        ("actual_amount", pa.float64()),
        ("variance", pa.float64()),
        ("match_status", _S),
        ("reconciled_date", _S),
        ("source_type", _S),
    ]
)

REWARD_EVENTS_SCHEMA = pa.schema(
    [
        ("reward_id", _S),
        ("transaction_id", _S),
        ("offer_id", _S),
        ("qualification_status", _S),
        ("reward_amount", pa.float64()),
        ("reward_currency", _S),
        ("event_timestamp", _S),
        ("source_type", _S),
    ]
)

SCHEMAS = {
    "clients": CLIENTS_SCHEMA,
    "programs": PROGRAMS_SCHEMA,
    "campaigns": CAMPAIGNS_SCHEMA,
    "offers": OFFERS_SCHEMA,
    "cardholders": CARDHOLDERS_SCHEMA,
    "card_tokens": CARD_TOKENS_SCHEMA,
    "transactions": TRANSACTIONS_SCHEMA,
    "transaction_events": TRANSACTION_EVENTS_SCHEMA,
    "settlements": SETTLEMENTS_SCHEMA,
    "reconciliation": RECONCILIATION_SCHEMA,
    "reward_events": REWARD_EVENTS_SCHEMA,
}
