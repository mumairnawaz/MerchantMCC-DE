"""S14 — dimension builders (src/gold/dimensions.py), verified against the
real local Silver/CDC Silver data already produced by S3-S13. No mocks, no
Bronze regeneration — these read the same Parquet the Gold pipeline reads.
"""

from datetime import date

from src.gold.config import UNKNOWN_KEY, UNKNOWN_NATURAL_KEY
from src.gold.dimensions import (
    build_dim_campaign,
    build_dim_card_issuer,
    build_dim_card_token,
    build_dim_cardholder,
    build_dim_client,
    build_dim_country,
    build_dim_currency,
    build_dim_date,
    build_dim_legal_entity,
    build_dim_mcc,
    build_dim_merchant,
    build_dim_offer,
    build_dim_program,
    date_key_of,
)
from src.gold.keys import load_registry
from src.silver.common import read_parquet


# ---- dim_date ----


def test_date_key_of_matches_yyyymmdd_convention():
    assert date_key_of(date(2026, 9, 22)) == 20260922
    assert date_key_of(date(2026, 1, 1)) == 20260101


def test_build_dim_date_covers_every_day_in_range_inclusive():
    rows = build_dim_date(date(2026, 1, 1), date(2026, 1, 5))
    assert len(rows) == 5
    assert {r["full_date"] for r in rows} == {date(2026, 1, d) for d in range(1, 6)}


def test_build_dim_date_weekday_and_weekend_are_correct():
    rows = {r["full_date"]: r for r in build_dim_date(date(2026, 9, 19), date(2026, 9, 21))}
    # 2026-09-19 is a Saturday, 2026-09-21 is a Monday (real calendar fact)
    assert rows[date(2026, 9, 19)]["day_name"] == "Saturday"
    assert rows[date(2026, 9, 19)]["is_weekend"] is True
    assert rows[date(2026, 9, 21)]["day_name"] == "Monday"
    assert rows[date(2026, 9, 21)]["is_weekend"] is False


def test_build_dim_date_quarter_and_month_name():
    rows = build_dim_date(date(2026, 7, 4), date(2026, 7, 4))
    assert rows[0]["quarter"] == 3
    assert rows[0]["month_name"] == "July"


# ---- API Silver-sourced dimensions ----


def test_build_dim_mcc_natural_keys_unique_and_unknown_member_present():
    source_count = len(read_parquet("data/silver/mcc/data.parquet"))
    rows = build_dim_mcc()
    assert len(rows) == source_count + 1
    codes = [r["mcc_code"] for r in rows]
    assert len(codes) == len(set(codes))
    unknown = next(r for r in rows if r["mcc_code"] == UNKNOWN_NATURAL_KEY)
    assert unknown["mcc_key"] == UNKNOWN_KEY


def test_build_dim_country_row_count_matches_source_plus_unknown():
    source_count = len(read_parquet("data/silver/country/data.parquet"))
    rows = build_dim_country()
    assert len(rows) == source_count + 1


def test_build_dim_currency_row_count_matches_source_plus_unknown():
    source_count = len(read_parquet("data/silver/iso_currency/data.parquet"))
    rows = build_dim_currency()
    assert len(rows) == source_count + 1


def test_build_dim_merchant_is_scd2_ready_with_a_single_real_version():
    rows = build_dim_merchant()
    real_rows = [r for r in rows if r["merchant_id"] != UNKNOWN_NATURAL_KEY]
    assert all(r["is_current"] is True for r in real_rows)
    assert all(r["effective_to"] is None for r in real_rows)
    # no fabricated history: exactly one version per merchant_id
    ids = [r["merchant_id"] for r in real_rows]
    assert len(ids) == len(set(ids))


def test_build_dim_legal_entity_preserves_entity_jurisdiction_exactly():
    source = read_parquet("data/silver/legal_entity/data.parquet")
    rows = {r["lei"]: r for r in build_dim_legal_entity()}
    for s in source[:50]:
        assert rows[s["lei"]]["entity_jurisdiction"] == s["entity_jurisdiction"]


def test_build_dim_card_issuer_row_count_matches_source_plus_unknown():
    source_count = len(read_parquet("data/silver/card_issuer/data.parquet"))
    rows = build_dim_card_issuer()
    assert len(rows) == source_count + 1


# ---- CDC Silver-sourced dimensions (dependent chain) ----


def test_build_dim_client_resolves_legal_entity_key_via_real_lei():
    build_dim_legal_entity()
    legal_entity_registry = load_registry("dim_legal_entity")
    rows = build_dim_client(legal_entity_registry)
    source = {r["client_id"]: r for r in read_parquet("data/silver_cdc/silver_cdc_client/data.parquet")}
    for row in rows:
        if row["client_id"] == UNKNOWN_NATURAL_KEY:
            continue
        expected_lei = source[row["client_id"]]["lei"]
        assert row["legal_entity_key"] == legal_entity_registry[expected_lei]


def test_build_dim_program_resolves_client_and_currency_keys():
    build_dim_legal_entity()
    client_rows = build_dim_client(load_registry("dim_legal_entity"))
    client_registry = load_registry("dim_client")
    currency_rows = build_dim_currency()
    currency_registry = load_registry("dim_currency")

    rows = build_dim_program(client_registry, currency_registry)
    source = {r["program_id"]: r for r in read_parquet("data/silver_cdc/silver_cdc_program/data.parquet")}
    for row in rows:
        if row["program_id"] == UNKNOWN_NATURAL_KEY:
            continue
        src = source[row["program_id"]]
        assert row["client_key"] == client_registry[src["client_id"]]
        assert row["currency_key"] == currency_registry[src["currency_code"]]


def _build_full_chain() -> dict[str, dict[str, int]]:
    """Builds the complete dependent-dimension chain once, in dependency
    order, and returns every registry — shared setup for the tests below."""
    build_dim_legal_entity()
    legal_entity_registry = load_registry("dim_legal_entity")
    build_dim_client(legal_entity_registry)
    client_registry = load_registry("dim_client")
    build_dim_currency()
    currency_registry = load_registry("dim_currency")
    build_dim_program(client_registry, currency_registry)
    program_registry = load_registry("dim_program")
    build_dim_campaign(program_registry)
    campaign_registry = load_registry("dim_campaign")
    build_dim_mcc()
    mcc_registry = load_registry("dim_mcc")
    build_dim_cardholder(program_registry)
    cardholder_registry = load_registry("dim_cardholder")
    build_dim_card_issuer()
    card_issuer_registry = load_registry("dim_card_issuer")
    return {
        "legal_entity": legal_entity_registry,
        "client": client_registry,
        "currency": currency_registry,
        "program": program_registry,
        "campaign": campaign_registry,
        "mcc": mcc_registry,
        "cardholder": cardholder_registry,
        "card_issuer": card_issuer_registry,
    }


def test_build_dim_offer_eligible_mcc_key_is_null_when_source_mcc_is_null():
    registries = _build_full_chain()
    rows = build_dim_offer(registries["campaign"], registries["mcc"], registries["currency"])
    source = {r["offer_id"]: r for r in read_parquet("data/silver_cdc/silver_cdc_offer/data.parquet")}
    checked_null = checked_non_null = 0
    for row in rows:
        if row["offer_id"] == UNKNOWN_NATURAL_KEY:
            continue
        src = source[row["offer_id"]]
        if src["eligible_mcc_code"] is None:
            assert row["eligible_mcc_key"] is None
            checked_null += 1
        else:
            assert row["eligible_mcc_key"] == registries["mcc"][src["eligible_mcc_code"]]
            checked_non_null += 1
    assert checked_null > 0 and checked_non_null > 0  # real data has both cases


def test_build_dim_card_token_resolves_cardholder_and_issuer_keys():
    registries = _build_full_chain()
    rows = build_dim_card_token(registries["cardholder"], registries["card_issuer"])
    source = {r["token_id"]: r for r in read_parquet("data/silver_cdc/silver_cdc_card_token/data.parquet")}
    checked = 0
    for row in rows:
        if row["token_id"] == UNKNOWN_NATURAL_KEY:
            continue
        src = source[row["token_id"]]
        assert row["cardholder_key"] == registries["cardholder"][src["cardholder_id"]]
        assert row["card_issuer_key"] == registries["card_issuer"][src["bin_range"]]
        checked += 1
    assert checked > 0
