"""S14 — fact builders (src/gold/facts.py), verified against real CDC
Silver data. Covers grain, FK resolution, the lookup chain, and the
event-log delete-marker exclusion discovered during this phase.
"""

from src.gold.config import UNKNOWN_KEY
from src.gold.dimensions import (
    build_dim_campaign,
    build_dim_card_issuer,
    build_dim_card_token,
    build_dim_cardholder,
    build_dim_client,
    build_dim_currency,
    build_dim_legal_entity,
    build_dim_mcc,
    build_dim_merchant,
    build_dim_offer,
    build_dim_program,
)
from src.gold.facts import (
    build_fact_reconciliation,
    build_fact_rewards,
    build_fact_settlements,
    build_fact_transaction_events,
    build_fact_transactions,
    build_lookup_chains,
)
from src.gold.keys import load_registry
from src.silver.common import read_parquet


def _all_registries() -> dict[str, dict[str, int]]:
    build_dim_legal_entity()
    legal_entity_registry = load_registry("dim_legal_entity")
    build_dim_client(legal_entity_registry)
    client_registry = load_registry("dim_client")
    build_dim_currency()
    currency_registry = load_registry("dim_currency")
    build_dim_program(client_registry, currency_registry)
    program_registry = load_registry("dim_program")
    build_dim_mcc()
    mcc_registry = load_registry("dim_mcc")
    build_dim_cardholder(program_registry)
    cardholder_registry = load_registry("dim_cardholder")
    build_dim_card_issuer()
    card_issuer_registry = load_registry("dim_card_issuer")
    build_dim_card_token(cardholder_registry, card_issuer_registry)
    card_token_registry = load_registry("dim_card_token")
    build_dim_merchant()
    merchant_registry = load_registry("dim_merchant")
    build_dim_campaign(program_registry)
    campaign_registry = load_registry("dim_campaign")
    build_dim_offer(campaign_registry, mcc_registry, currency_registry)
    offer_registry = load_registry("dim_offer")
    return {
        "merchant": merchant_registry,
        "mcc": mcc_registry,
        "currency": currency_registry,
        "card_token": card_token_registry,
        "cardholder": cardholder_registry,
        "program": program_registry,
        "client": client_registry,
        "offer": offer_registry,
    }


def test_lookup_chain_resolves_a_real_token_to_client():
    chains = build_lookup_chains()
    card_tokens = read_parquet("data/silver_cdc/silver_cdc_card_token/data.parquet")
    sample = card_tokens[0]
    cardholder_id = chains["token_to_cardholder"][sample["token_id"]]
    program_id = chains["cardholder_to_program"][cardholder_id]
    client_id = chains["program_to_client"][program_id]
    assert client_id is not None and client_id != ""


def test_fact_transactions_grain_is_one_row_per_transaction_id():
    r = _all_registries()
    rows = build_fact_transactions(r["merchant"], r["mcc"], r["currency"], r["card_token"], r["cardholder"], r["program"], r["client"])
    source_count = len(read_parquet("data/silver_cdc/silver_cdc_transaction/data.parquet"))
    ids = [row["transaction_id"] for row in rows]
    assert len(rows) == source_count
    assert len(ids) == len(set(ids))


def test_fact_transactions_country_key_is_always_unknown():
    r = _all_registries()
    rows = build_fact_transactions(r["merchant"], r["mcc"], r["currency"], r["card_token"], r["cardholder"], r["program"], r["client"])
    assert all(row["country_key"] == UNKNOWN_KEY for row in rows)


def test_fact_transactions_merchant_key_resolves_for_every_real_merchant_id():
    r = _all_registries()
    rows = build_fact_transactions(r["merchant"], r["mcc"], r["currency"], r["card_token"], r["cardholder"], r["program"], r["client"])
    # verified in this phase: 520/520 real merchant_id overlap -> no fact row should fall back to UNKNOWN via merchant
    assert all(row["merchant_key"] != UNKNOWN_KEY for row in rows)


def test_fact_transactions_amount_is_decimal_and_matches_a_real_source_row():
    from decimal import Decimal

    r = _all_registries()
    rows = {row["transaction_id"]: row for row in build_fact_transactions(r["merchant"], r["mcc"], r["currency"], r["card_token"], r["cardholder"], r["program"], r["client"])}
    source = read_parquet("data/silver_cdc/silver_cdc_transaction/data.parquet")
    sample = source[0]
    assert isinstance(rows[sample["transaction_id"]]["amount"], Decimal)
    assert rows[sample["transaction_id"]]["amount"] == sample["amount"]


def test_fact_settlements_grain_and_row_count():
    r = _all_registries()
    rows = build_fact_settlements(r["currency"])
    source_count = len(read_parquet("data/silver_cdc/silver_cdc_settlement/data.parquet"))
    ids = [row["settlement_id"] for row in rows]
    assert len(rows) == source_count
    assert len(ids) == len(set(ids))


def test_fact_reconciliation_grain_and_row_count():
    rows = build_fact_reconciliation()
    source_count = len(read_parquet("data/silver_cdc/silver_cdc_reconciliation/data.parquet"))
    ids = [row["reconciliation_id"] for row in rows]
    assert len(rows) == source_count
    assert len(ids) == len(set(ids))


def test_fact_transaction_events_excludes_delete_markers():
    rows = build_fact_transaction_events()
    from pathlib import Path

    source_dir = Path("data/silver_cdc/silver_cdc_transaction_event")
    total = 0
    deletes = 0
    for f in sorted(source_dir.glob("run_*.parquet")):
        for rec in read_parquet(f):
            total += 1
            if rec["operation"] == "d":
                deletes += 1
    assert len(rows) == total - deletes
    assert all(row["event_type"] for row in rows)  # no blank delete-marker content leaked through


def test_fact_transaction_events_grain_is_event_id_plus_kafka_offset():
    rows = build_fact_transaction_events()
    keys = [(row["event_id"], row["kafka_offset"]) for row in rows]
    assert len(keys) == len(set(keys))


def test_fact_rewards_excludes_delete_markers_and_uses_unknown_member_for_unresolved_offers():
    r = _all_registries()
    rows = build_fact_rewards(r["offer"], r["currency"])
    from pathlib import Path

    source_dir = Path("data/silver_cdc/silver_cdc_reward_event")
    total = 0
    deletes = 0
    for f in sorted(source_dir.glob("run_*.parquet")):
        for rec in read_parquet(f):
            total += 1
            if rec["operation"] == "d":
                deletes += 1
    assert len(rows) == total - deletes

    # Real data (verified in this phase): the S10 idempotency-test fixture's
    # RWD-9990001 rows reference offer_id "OFR-9001", which was never a real
    # dim_offer row — the unknown-member convention (§10) must catch this,
    # not silently drop the reward row or raise.
    unresolved = [row for row in rows if row["offer_key"] == UNKNOWN_KEY]
    resolved = [row for row in rows if row["offer_key"] != UNKNOWN_KEY]
    assert len(unresolved) > 0
    assert all(row["reward_id"] == "RWD-9990001" for row in unresolved)
    assert len(resolved) > 0  # the overwhelming majority of real rewards do resolve


def test_fact_rewards_not_qualified_rows_carry_a_real_zero_amount():
    from decimal import Decimal

    r = _all_registries()
    rows = build_fact_rewards(r["offer"], r["currency"])
    not_qualified = [row for row in rows if row["qualification_status"] == "NOT_QUALIFIED"]
    assert len(not_qualified) > 0
    assert all(row["reward_amount"] == Decimal("0.0000") for row in not_qualified)
