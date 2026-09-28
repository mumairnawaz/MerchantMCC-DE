"""Gold dimension builders. Every dimension's source column list is taken
directly from the real Silver schemas (docs/21 §3, docs/26 §3-4) — verified
against actual Parquet files in this phase, not assumed.

SCD strategy (§17, per-dimension, documented — not blanket Type 2):
  - dim_merchant: declared SCD Type 2 in the originally-approved architecture
    (docs/11), and this dimension IS built with the Type 2 columns
    (effective_from/effective_to/is_current). But real Bronze OSM data has
    only ever been ingested ONCE (S5) — there is no second snapshot to derive
    real history from. Fabricating history would violate this phase's "do
    not invent" rule. Implemented: every merchant gets exactly one version,
    effective_from = a fixed epoch, effective_to = NULL, is_current = True —
    structurally SCD2-ready, but with only the single real version that
    exists today. Future-ready, not fabricated.
  - Every other dimension: Type 1 (overwrite), matching docs/22's explicit
    architecture decision (dim_client/dim_program/dim_offer = Type 1) and
    this project's simplest-that-fits-the-real-source-cardinality principle
    for the rest (dim_mcc/dim_country/dim_currency/dim_card_issuer/
    dim_legal_entity/dim_campaign/dim_cardholder/dim_card_token — none of
    these have a documented Type 2 requirement anywhere in docs/21-26).

Unknown member (§10): every dimension gets one synthetic UNKNOWN row,
surrogate key -1, so a fact whose lookup misses never disappears — see
src/gold/facts.py.
"""

from datetime import date
from typing import Any

import pyarrow as pa

from src.gold.config import API_SILVER_ROOT, CDC_SILVER_ROOT, UNKNOWN_KEY, UNKNOWN_NATURAL_KEY
from src.gold.keys import assign_surrogate_keys
from src.silver.common import read_parquet

SCD2_EPOCH = date(2026, 1, 1)  # a fixed, documented placeholder "always has been current" start — never invented per-row history


def _api_silver(dataset: str) -> list[dict[str, Any]]:
    path = API_SILVER_ROOT / dataset / "data.parquet"
    return read_parquet(path) if path.exists() else []


def _cdc_silver(dataset: str) -> list[dict[str, Any]]:
    """Reads a "current_state" CDC Silver dataset — a single data.parquet
    (docs/26 §2). Event-log datasets (transaction_events, reward_events) are
    NOT read here — see src/gold/facts.py::_read_event_log, since they are
    append-only across multiple run_<ts>.parquet files, not one data.parquet."""
    path = CDC_SILVER_ROOT / dataset / "data.parquet"
    return read_parquet(path) if path.exists() else []


# ---- dim_date (§18 — real observed range only, never fabricated) ----

DATE_DIM_SCHEMA = pa.schema(
    [
        ("date_key", pa.int32()),
        ("full_date", pa.date32()),
        ("year", pa.int32()),
        ("quarter", pa.int32()),
        ("month", pa.int32()),
        ("month_name", pa.string()),
        ("day", pa.int32()),
        ("day_of_week", pa.int32()),  # 1=Monday .. 7=Sunday (ISO)
        ("day_name", pa.string()),
        ("week_of_year", pa.int32()),
        ("is_weekend", pa.bool_()),
    ]
)

MONTH_NAMES = ["", "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def date_key_of(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def build_dim_date(min_date: date, max_date: date) -> list[dict[str, Any]]:
    rows = []
    d = min_date
    from datetime import timedelta

    while d <= max_date:
        iso = d.isocalendar()
        rows.append(
            {
                "date_key": date_key_of(d),
                "full_date": d,
                "year": d.year,
                "quarter": (d.month - 1) // 3 + 1,
                "month": d.month,
                "month_name": MONTH_NAMES[d.month],
                "day": d.day,
                "day_of_week": iso.weekday,
                "day_name": DAY_NAMES[iso.weekday - 1],
                "week_of_year": iso.week,
                "is_weekend": iso.weekday in (6, 7),
            }
        )
        d += timedelta(days=1)
    return rows


# ---- dim_merchant (API Silver, SCD2-ready single-version) ----

DIM_MERCHANT_SCHEMA = pa.schema(
    [
        ("merchant_key", pa.int64()),
        ("merchant_id", pa.string()),
        ("merchant_name", pa.string()),
        ("raw_shop_tag", pa.string()),
        ("raw_amenity_tag", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("address_city", pa.string()),
        ("address_postcode", pa.string()),
        ("address_street", pa.string()),
        ("phone", pa.string()),
        ("website", pa.string()),
        ("opening_hours", pa.string()),
        ("country_code", pa.string()),  # always NULL today — open Silver decision, docs/21 §14
        ("effective_from", pa.date32()),
        ("effective_to", pa.date32()),
        ("is_current", pa.bool_()),
    ]
)


def build_dim_merchant() -> list[dict[str, Any]]:
    source = _api_silver("merchant")
    registry = assign_surrogate_keys("dim_merchant", [r["merchant_id"] for r in source])
    rows = [
        {
            "merchant_key": registry[r["merchant_id"]],
            "merchant_id": r["merchant_id"],
            "merchant_name": r["merchant_name"],
            "raw_shop_tag": r["raw_shop_tag"],
            "raw_amenity_tag": r["raw_amenity_tag"],
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "address_city": r["address_city"],
            "address_postcode": r["address_postcode"],
            "address_street": r["address_street"],
            "phone": r["phone"],
            "website": r["website"],
            "opening_hours": r["opening_hours"],
            "country_code": r["country_code"],
            "effective_from": SCD2_EPOCH,
            "effective_to": None,
            "is_current": True,
        }
        for r in source
    ]
    rows.append(
        {
            "merchant_key": UNKNOWN_KEY, "merchant_id": UNKNOWN_NATURAL_KEY, "merchant_name": "Unknown Merchant",
            "raw_shop_tag": None, "raw_amenity_tag": None, "latitude": None, "longitude": None,
            "address_city": None, "address_postcode": None, "address_street": None, "phone": None,
            "website": None, "opening_hours": None, "country_code": None,
            "effective_from": SCD2_EPOCH, "effective_to": None, "is_current": True,
        }
    )
    return rows


# ---- dim_mcc (API Silver, Type 1) ----

DIM_MCC_SCHEMA = pa.schema(
    [
        ("mcc_key", pa.int64()),
        ("mcc_code", pa.string()),
        ("description", pa.string()),
        ("description_combined", pa.string()),
        ("description_usda", pa.string()),
        ("description_irs", pa.string()),
        ("irs_reportable", pa.bool_()),
    ]
)


def build_dim_mcc() -> list[dict[str, Any]]:
    source = _api_silver("mcc")
    registry = assign_surrogate_keys("dim_mcc", [r["mcc_code"] for r in source])
    rows = [
        {
            "mcc_key": registry[r["mcc_code"]],
            "mcc_code": r["mcc_code"],
            "description": r["description"],
            "description_combined": r["description_combined"],
            "description_usda": r["description_usda"],
            "description_irs": r["description_irs"],
            "irs_reportable": r["irs_reportable"],
        }
        for r in source
    ]
    rows.append({"mcc_key": UNKNOWN_KEY, "mcc_code": UNKNOWN_NATURAL_KEY, "description": "Unknown MCC", "description_combined": None, "description_usda": None, "description_irs": None, "irs_reportable": None})
    return rows


# ---- dim_country (API Silver, Type 1) ----

DIM_COUNTRY_SCHEMA = pa.schema(
    [
        ("country_key", pa.int64()),
        ("country_code_alpha3", pa.string()),
        ("country_code_alpha2", pa.string()),
        ("country_name", pa.string()),
        ("region", pa.string()),
        ("subregion", pa.string()),
        ("default_currency_code", pa.string()),
    ]
)


def build_dim_country() -> list[dict[str, Any]]:
    source = _api_silver("country")
    registry = assign_surrogate_keys("dim_country", [r["country_code_alpha3"] for r in source])
    rows = [
        {
            "country_key": registry[r["country_code_alpha3"]],
            "country_code_alpha3": r["country_code_alpha3"],
            "country_code_alpha2": r["country_code_alpha2"],
            "country_name": r["country_name"],
            "region": r["region"],
            "subregion": r["subregion"],
            "default_currency_code": r["default_currency_code"],
        }
        for r in source
    ]
    rows.append({"country_key": UNKNOWN_KEY, "country_code_alpha3": UNKNOWN_NATURAL_KEY, "country_code_alpha2": None, "country_name": "Unknown Country", "region": None, "subregion": None, "default_currency_code": None})
    return rows


# ---- dim_currency (API Silver, Type 1) ----

DIM_CURRENCY_SCHEMA = pa.schema(
    [
        ("currency_key", pa.int64()),
        ("currency_code", pa.string()),
        ("currency_name", pa.string()),
        ("minor_unit", pa.int32()),
        ("is_active", pa.bool_()),
    ]
)


def build_dim_currency() -> list[dict[str, Any]]:
    source = _api_silver("iso_currency")
    registry = assign_surrogate_keys("dim_currency", [r["currency_code"] for r in source])
    rows = [
        {
            "currency_key": registry[r["currency_code"]],
            "currency_code": r["currency_code"],
            "currency_name": r["currency_name"],
            "minor_unit": r["minor_unit"],
            "is_active": r["is_active"],
        }
        for r in source
    ]
    rows.append({"currency_key": UNKNOWN_KEY, "currency_code": UNKNOWN_NATURAL_KEY, "currency_name": "Unknown Currency", "minor_unit": None, "is_active": None})
    return rows


# ---- dim_card_issuer (API Silver, Type 1) ----

DIM_CARD_ISSUER_SCHEMA = pa.schema(
    [
        ("card_issuer_key", pa.int64()),
        ("bin_range", pa.string()),
        ("card_brand", pa.string()),
        ("card_type", pa.string()),
        ("card_category", pa.string()),
        ("issuer_name", pa.string()),
        ("issuer_country_alpha2", pa.string()),
        ("issuer_country_name", pa.string()),
    ]
)


def build_dim_card_issuer() -> list[dict[str, Any]]:
    source = _api_silver("card_issuer")
    registry = assign_surrogate_keys("dim_card_issuer", [r["bin_range"] for r in source])
    rows = [
        {
            "card_issuer_key": registry[r["bin_range"]],
            "bin_range": r["bin_range"],
            "card_brand": r["card_brand"],
            "card_type": r["card_type"],
            "card_category": r["card_category"],
            "issuer_name": r["issuer_name"],
            "issuer_country_alpha2": r["issuer_country_alpha2"],
            "issuer_country_name": r["issuer_country_name"],
        }
        for r in source
    ]
    rows.append({"card_issuer_key": UNKNOWN_KEY, "bin_range": UNKNOWN_NATURAL_KEY, "card_brand": "Unknown", "card_type": None, "card_category": None, "issuer_name": None, "issuer_country_alpha2": None, "issuer_country_name": None})
    return rows


# ---- dim_legal_entity (API Silver, Type 1 — entity_jurisdiction preserved exactly, non-negotiable per docs/21 §8) ----

DIM_LEGAL_ENTITY_SCHEMA = pa.schema(
    [
        ("legal_entity_key", pa.int64()),
        ("lei", pa.string()),
        ("legal_name", pa.string()),
        ("legal_address_country", pa.string()),
        ("entity_jurisdiction", pa.string()),
        ("entity_status", pa.string()),
        ("registration_status", pa.string()),
    ]
)


def build_dim_legal_entity() -> list[dict[str, Any]]:
    source = _api_silver("legal_entity")
    registry = assign_surrogate_keys("dim_legal_entity", [r["lei"] for r in source])
    rows = [
        {
            "legal_entity_key": registry[r["lei"]],
            "lei": r["lei"],
            "legal_name": r["legal_name"],
            "legal_address_country": r["legal_address_country"],
            "entity_jurisdiction": r["entity_jurisdiction"],  # never uppercased/split/folded
            "entity_status": r["entity_status"],
            "registration_status": r["registration_status"],
        }
        for r in source
    ]
    rows.append({"legal_entity_key": UNKNOWN_KEY, "lei": UNKNOWN_NATURAL_KEY, "legal_name": "Unknown Legal Entity", "legal_address_country": None, "entity_jurisdiction": None, "entity_status": None, "registration_status": None})
    return rows


# ---- dim_client (CDC Silver, Type 1) ----

DIM_CLIENT_SCHEMA = pa.schema(
    [
        ("client_key", pa.int64()),
        ("client_id", pa.string()),
        ("legal_entity_key", pa.int64()),
        ("lei", pa.string()),
        ("legal_name", pa.string()),
        ("client_type", pa.string()),
        ("country_code", pa.string()),
        ("status", pa.string()),
    ]
)


def build_dim_client(legal_entity_registry: dict[str, int]) -> list[dict[str, Any]]:
    source = _cdc_silver("silver_cdc_client")
    registry = assign_surrogate_keys("dim_client", [r["client_id"] for r in source])
    rows = [
        {
            "client_key": registry[r["client_id"]],
            "client_id": r["client_id"],
            "legal_entity_key": legal_entity_registry.get(r["lei"], UNKNOWN_KEY),
            "lei": r["lei"],
            "legal_name": r["legal_name"],
            "client_type": r["client_type"],
            "country_code": r["country_code"],
            "status": r["status"],
        }
        for r in source
    ]
    rows.append({"client_key": UNKNOWN_KEY, "client_id": UNKNOWN_NATURAL_KEY, "legal_entity_key": UNKNOWN_KEY, "lei": None, "legal_name": "Unknown Client", "client_type": None, "country_code": None, "status": None})
    return rows


# ---- dim_program (CDC Silver, Type 1) ----

DIM_PROGRAM_SCHEMA = pa.schema(
    [
        ("program_key", pa.int64()),
        ("program_id", pa.string()),
        ("client_key", pa.int64()),
        ("program_name", pa.string()),
        ("program_type", pa.string()),
        ("currency_key", pa.int64()),
        ("status", pa.string()),
    ]
)


def build_dim_program(client_registry: dict[str, int], currency_registry: dict[str, int]) -> list[dict[str, Any]]:
    source = _cdc_silver("silver_cdc_program")
    registry = assign_surrogate_keys("dim_program", [r["program_id"] for r in source])
    rows = [
        {
            "program_key": registry[r["program_id"]],
            "program_id": r["program_id"],
            "client_key": client_registry.get(r["client_id"], UNKNOWN_KEY),
            "program_name": r["program_name"],
            "program_type": r["program_type"],
            "currency_key": currency_registry.get(r["currency_code"], UNKNOWN_KEY),
            "status": r["status"],
        }
        for r in source
    ]
    rows.append({"program_key": UNKNOWN_KEY, "program_id": UNKNOWN_NATURAL_KEY, "client_key": UNKNOWN_KEY, "program_name": "Unknown Program", "program_type": None, "currency_key": UNKNOWN_KEY, "status": None})
    return rows


# ---- dim_campaign (CDC Silver, Type 1) ----

DIM_CAMPAIGN_SCHEMA = pa.schema(
    [
        ("campaign_key", pa.int64()),
        ("campaign_id", pa.string()),
        ("program_key", pa.int64()),
        ("campaign_name", pa.string()),
        ("start_date", pa.date32()),
        ("end_date", pa.date32()),
        ("status", pa.string()),
    ]
)


def build_dim_campaign(program_registry: dict[str, int]) -> list[dict[str, Any]]:
    source = _cdc_silver("silver_cdc_campaign")
    registry = assign_surrogate_keys("dim_campaign", [r["campaign_id"] for r in source])
    rows = [
        {
            "campaign_key": registry[r["campaign_id"]],
            "campaign_id": r["campaign_id"],
            "program_key": program_registry.get(r["program_id"], UNKNOWN_KEY),
            "campaign_name": r["campaign_name"],
            "start_date": r["start_date"],
            "end_date": r["end_date"],
            "status": r["status"],
        }
        for r in source
    ]
    rows.append({"campaign_key": UNKNOWN_KEY, "campaign_id": UNKNOWN_NATURAL_KEY, "program_key": UNKNOWN_KEY, "campaign_name": "Unknown Campaign", "start_date": None, "end_date": None, "status": None})
    return rows


# ---- dim_offer (CDC Silver, Type 1; eligible_mcc_code is REAL, docs/11) ----

DIM_OFFER_SCHEMA = pa.schema(
    [
        ("offer_key", pa.int64()),
        ("offer_id", pa.string()),
        ("campaign_key", pa.int64()),
        ("eligible_mcc_key", pa.int64()),
        ("offer_type", pa.string()),
        ("offer_value", pa.decimal128(18, 4)),
        ("min_transaction_amount", pa.decimal128(18, 4)),
        ("currency_key", pa.int64()),
        ("status", pa.string()),
        ("valid_from", pa.date32()),
        ("valid_to", pa.date32()),
    ]
)


def build_dim_offer(campaign_registry: dict[str, int], mcc_registry: dict[str, int], currency_registry: dict[str, int]) -> list[dict[str, Any]]:
    source = _cdc_silver("silver_cdc_offer")
    registry = assign_surrogate_keys("dim_offer", [r["offer_id"] for r in source])
    rows = [
        {
            "offer_key": registry[r["offer_id"]],
            "offer_id": r["offer_id"],
            "campaign_key": campaign_registry.get(r["campaign_id"], UNKNOWN_KEY),
            "eligible_mcc_key": mcc_registry.get(r["eligible_mcc_code"], UNKNOWN_KEY) if r["eligible_mcc_code"] else None,
            "offer_type": r["offer_type"],
            "offer_value": r["offer_value"],
            "min_transaction_amount": r["min_transaction_amount"],
            "currency_key": currency_registry.get(r["currency_code"], UNKNOWN_KEY),
            "status": r["status"],
            "valid_from": r["valid_from"],
            "valid_to": r["valid_to"],
        }
        for r in source
    ]
    rows.append({"offer_key": UNKNOWN_KEY, "offer_id": UNKNOWN_NATURAL_KEY, "campaign_key": UNKNOWN_KEY, "eligible_mcc_key": None, "offer_type": None, "offer_value": None, "min_transaction_amount": None, "currency_key": UNKNOWN_KEY, "status": None, "valid_from": None, "valid_to": None})
    return rows


# ---- dim_cardholder (CDC Silver, Type 1) ----

DIM_CARDHOLDER_SCHEMA = pa.schema(
    [
        ("cardholder_key", pa.int64()),
        ("cardholder_id", pa.string()),
        ("pseudonym", pa.string()),
        ("country_code", pa.string()),
        ("program_key", pa.int64()),
        ("enrollment_date", pa.date32()),
    ]
)


def build_dim_cardholder(program_registry: dict[str, int]) -> list[dict[str, Any]]:
    source = _cdc_silver("silver_cdc_cardholder")
    registry = assign_surrogate_keys("dim_cardholder", [r["cardholder_id"] for r in source])
    rows = [
        {
            "cardholder_key": registry[r["cardholder_id"]],
            "cardholder_id": r["cardholder_id"],
            "pseudonym": r["pseudonym"],
            "country_code": r["country_code"],
            "program_key": program_registry.get(r["enrolled_program_id"], UNKNOWN_KEY),
            "enrollment_date": r["enrollment_date"],
        }
        for r in source
    ]
    rows.append({"cardholder_key": UNKNOWN_KEY, "cardholder_id": UNKNOWN_NATURAL_KEY, "pseudonym": "Unknown Cardholder", "country_code": None, "program_key": UNKNOWN_KEY, "enrollment_date": None})
    return rows


# ---- dim_card_token (CDC Silver, Type 1) ----

DIM_CARD_TOKEN_SCHEMA = pa.schema(
    [
        ("card_token_key", pa.int64()),
        ("token_id", pa.string()),
        ("cardholder_key", pa.int64()),
        ("card_issuer_key", pa.int64()),
        ("card_brand", pa.string()),
        ("token_status", pa.string()),
        ("issued_date", pa.date32()),
    ]
)


def build_dim_card_token(cardholder_registry: dict[str, int], card_issuer_registry: dict[str, int]) -> list[dict[str, Any]]:
    source = _cdc_silver("silver_cdc_card_token")
    registry = assign_surrogate_keys("dim_card_token", [r["token_id"] for r in source])
    rows = [
        {
            "card_token_key": registry[r["token_id"]],
            "token_id": r["token_id"],
            "cardholder_key": cardholder_registry.get(r["cardholder_id"], UNKNOWN_KEY),
            "card_issuer_key": card_issuer_registry.get(r["bin_range"], UNKNOWN_KEY),
            "card_brand": r["card_brand"],
            "token_status": r["token_status"],
            "issued_date": r["issued_date"],
        }
        for r in source
    ]
    rows.append({"card_token_key": UNKNOWN_KEY, "token_id": UNKNOWN_NATURAL_KEY, "cardholder_key": UNKNOWN_KEY, "card_issuer_key": UNKNOWN_KEY, "card_brand": None, "token_status": None, "issued_date": None})
    return rows
