"""Synthetic OLTP entity generators (Phase S9).

All generation rules, counts, and distributions are documented here and in
docs/22-synthetic-business-data-design.md §9-11 — nothing here is arbitrary or
undocumented. Every function takes an explicit `random.Random` instance (never
the global `random` module) so the whole pipeline is reproducible end to end
from a single seed (src.synthetic.common.SEED).

Grain and scope decisions (see docs/22 for full rationale):
  - clients: EVERY client maps 1:1 to a real, distinct silver_legal_entity LEI
    (docs/11-data-model.md already specifies "dim_client = REAL (GLEIF legal
    entities)"). client_type/onboarding_date/status/client_id are synthetic;
    legal_name/country_code are copied verbatim from the real LEI record.
  - offer.eligible_mcc_code, when set, is a real silver_mcc code (docs/11:
    "dim_offer = SYNTHETIC, except its eligible_mcc_code FK value, which is REAL").
  - transactions.merchant_id is a real silver_merchant.merchant_id; every one of
    the 521 real merchants is eligible.
  - transactions.mcc_code is assigned via src.synthetic.mcc_crosswalk — a
    synthetic, low-confidence per-merchant assignment, NOT the same as (and
    does not modify) silver_merchant.mcc_code, which stays NULL.
  - transactions.currency_code follows the cardholder's enrolled program's
    currency — no cross-currency/FX-converted transactions in this v1 (kept
    out of scope; see docs/22 §17 deferred items).
  - offer_type is limited to CASHBACK_PCT and FIXED_AMOUNT (both monetary) —
    POINTS_MULTIPLIER is deferred (documented, not silently dropped).
  - Settlement always covers the FULL transaction amount (no partial capture/
    partial settlement modeled in v1).
  - A reversal never un-settles a transaction in v1 — it is an event-level
    signal only; settlement/reconciliation totals are unaffected by reversals.
"""

import random
from datetime import date, datetime, timedelta
from typing import Any

from src.synthetic import mcc_crosswalk
from src.synthetic.common import REFERENCE_DATE, SOURCE_TYPE, days_before_reference

N_CLIENTS_ISSUER = 3
N_CLIENTS_NETWORK = 3
N_CLIENTS_PROGRAM_OWNER = 6
N_PROGRAMS = 8
N_CAMPAIGNS = 12
N_OFFERS = 20
N_CARDHOLDERS = 200
TOKEN_SECOND_TOKEN_PROBABILITY = 0.3
N_TRANSACTIONS = 4000
TRANSACTION_WINDOW_DAYS = 90
APPROVAL_RATE = 0.88
REVERSAL_RATE = 0.03  # of approved+captured transactions old enough not to reverse into the future
SETTLEMENT_FEE_RATE = 0.015  # fixed 1.5% — deliberately fixed, not randomized, for a clean control total
RECONCILIATION_EXCEPTION_RATE = 0.03

DECLINE_REASONS = ["INSUFFICIENT_FUNDS", "FRAUD_SUSPECTED", "CARD_EXPIRED", "DO_NOT_HONOR"]
PROGRAM_CURRENCIES_WEIGHTED = ["GBP", "GBP", "GBP", "EUR", "EUR", "USD"]  # 50% / 33% / 17%


def _seq(prefix: str, n: int, width: int) -> list[str]:
    return [f"{prefix}-{i:0{width}d}" for i in range(1, n + 1)]


def _random_amount(rng: random.Random, low: float = 2.0, high: float = 250.0) -> float:
    return round(rng.uniform(low, high), 2)


def _random_datetime_in_window(rng: random.Random, start: date, end: date) -> datetime:
    span_days = (end - start).days
    offset_days = rng.randint(0, max(span_days, 0))
    d = start + timedelta(days=offset_days)
    return datetime(d.year, d.month, d.day, rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59))


# ---- 1. clients ----


def generate_clients(rng: random.Random, legal_entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    total = N_CLIENTS_ISSUER + N_CLIENTS_NETWORK + N_CLIENTS_PROGRAM_OWNER
    pool = sorted(legal_entities, key=lambda r: r["lei"])  # deterministic order before sampling
    chosen = rng.sample(pool, total)
    types = ["ISSUER"] * N_CLIENTS_ISSUER + ["NETWORK"] * N_CLIENTS_NETWORK + ["PROGRAM_OWNER"] * N_CLIENTS_PROGRAM_OWNER

    clients = []
    for i, (entity, client_type) in enumerate(zip(chosen, types), start=1):
        onboarding_offset = rng.randint(30, 1500)
        clients.append(
            {
                "client_id": f"CLI-{i:04d}",
                "lei": entity["lei"],
                "legal_name": entity["legal_name"],
                "client_type": client_type,
                "country_code": entity.get("legal_address_country"),
                "onboarding_date": days_before_reference(onboarding_offset).isoformat(),
                "status": "ACTIVE",
                "source_type": SOURCE_TYPE,
            }
        )
    return clients


# ---- 2. programs ----


def generate_programs(rng: random.Random, clients: list[dict[str, Any]]) -> list[dict[str, Any]]:
    program_owners = [c for c in clients if c["client_type"] == "PROGRAM_OWNER"]
    program_types = ["CASHBACK", "LOYALTY", "CO_BRAND"]
    programs = []
    for i in range(1, N_PROGRAMS + 1):
        owner = program_owners[(i - 1) % len(program_owners)]
        programs.append(
            {
                "program_id": f"PRG-{i:04d}",
                "client_id": owner["client_id"],
                "program_name": f"{owner['legal_name'][:20]} {program_types[(i - 1) % len(program_types)].title()} Program {i}",
                "program_type": program_types[(i - 1) % len(program_types)],
                "currency_code": rng.choice(PROGRAM_CURRENCIES_WEIGHTED),
                "status": "ACTIVE",
                "start_date": days_before_reference(rng.randint(200, 1200)).isoformat(),
                "source_type": SOURCE_TYPE,
            }
        )
    return programs


# ---- 3. campaigns ----


def generate_campaigns(rng: random.Random, programs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    campaigns = []
    for i in range(1, N_CAMPAIGNS + 1):
        program = programs[(i - 1) % len(programs)]
        if i <= 8:  # straddles REFERENCE_DATE -> ACTIVE, and overlaps the transaction window
            start = days_before_reference(rng.randint(60, 120))
            end = REFERENCE_DATE + timedelta(days=rng.randint(30, 90))
        elif i <= 10:  # fully in the past -> ENDED
            start = days_before_reference(rng.randint(400, 500))
            end = days_before_reference(rng.randint(100, 200))
        else:  # fully in the future -> SCHEDULED
            start = REFERENCE_DATE + timedelta(days=rng.randint(10, 30))
            end = REFERENCE_DATE + timedelta(days=rng.randint(60, 150))

        if end < REFERENCE_DATE:
            status = "ENDED"
        elif start > REFERENCE_DATE:
            status = "SCHEDULED"
        else:
            status = "ACTIVE"

        campaigns.append(
            {
                "campaign_id": f"CMP-{i:04d}",
                "program_id": program["program_id"],
                "campaign_name": f"Campaign {i} — {program['program_type'].title()}",
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "status": status,
                "source_type": SOURCE_TYPE,
            }
        )
    return campaigns


# ---- 4. offers ----


def generate_offers(rng: random.Random, campaigns: list[dict[str, Any]], programs: list[dict[str, Any]], mcc_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    program_by_id = {p["program_id"]: p for p in programs}
    mcc_codes = sorted(r["mcc_code"] for r in mcc_rows)
    offer_types = ["CASHBACK_PCT", "FIXED_AMOUNT"]

    offers = []
    for i in range(1, N_OFFERS + 1):
        campaign = campaigns[(i - 1) % len(campaigns)]
        program = program_by_id[campaign["program_id"]]
        campaign_start = date.fromisoformat(campaign["start_date"])
        campaign_end = date.fromisoformat(campaign["end_date"])
        valid_from = campaign_start + timedelta(days=rng.randint(0, 10))
        valid_to = campaign_end - timedelta(days=rng.randint(0, 10))
        if valid_to <= valid_from:
            valid_to = valid_from + timedelta(days=30)

        offer_type = offer_types[(i - 1) % len(offer_types)]
        offer_value = round(rng.uniform(0.01, 0.10), 4) if offer_type == "CASHBACK_PCT" else round(rng.uniform(1.0, 10.0), 2)
        eligible_mcc = rng.choice(mcc_codes) if rng.random() < 0.6 else None

        offers.append(
            {
                "offer_id": f"OFR-{i:04d}",
                "campaign_id": campaign["campaign_id"],
                "eligible_mcc_code": eligible_mcc,
                "offer_type": offer_type,
                "offer_value": offer_value,
                "min_transaction_amount": _random_amount(rng, 5.0, 50.0),
                "currency_code": program["currency_code"],
                "status": campaign["status"],
                "valid_from": valid_from.isoformat(),
                "valid_to": valid_to.isoformat(),
                "source_type": SOURCE_TYPE,
            }
        )
    return offers


# ---- 5. cardholders ----


def generate_cardholders(rng: random.Random, programs: list[dict[str, Any]], countries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    country_codes = sorted({r["country_code_alpha2"] for r in countries if r["country_code_alpha2"]})
    non_gb = [c for c in country_codes if c != "GB"]

    cardholders = []
    for i in range(1, N_CARDHOLDERS + 1):
        program = programs[(i - 1) % len(programs)]
        country_code = "GB" if rng.random() < 0.7 else rng.choice(non_gb)
        cardholders.append(
            {
                "cardholder_id": f"CH-{i:06d}",
                "pseudonym": f"Cardholder-{i:06d}",  # deliberately not a realistic fake name — see docs/22 §3
                "country_code": country_code,
                "enrolled_program_id": program["program_id"],
                "enrollment_date": days_before_reference(rng.randint(30, 700)).isoformat(),
                "source_type": SOURCE_TYPE,
            }
        )
    return cardholders


# ---- 6. card tokens ----


def generate_card_tokens(rng: random.Random, cardholders: list[dict[str, Any]], card_issuers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bin_rows = card_issuers
    tokens = []
    seq = 1
    for cardholder in cardholders:
        n_tokens = 2 if rng.random() < TOKEN_SECOND_TOKEN_PROBABILITY else 1
        for _ in range(n_tokens):
            bin_row = rng.choice(bin_rows)
            token_hex = "".join(rng.choice("0123456789abcdef") for _ in range(16))
            tokens.append(
                {
                    "token_id": f"TKN-{token_hex}",
                    "cardholder_id": cardholder["cardholder_id"],
                    "bin_range": bin_row["bin_range"],
                    "card_brand": bin_row["card_brand"],
                    "token_status": rng.choices(["ACTIVE", "SUSPENDED", "EXPIRED"], weights=[90, 5, 5])[0],
                    "issued_date": cardholder["enrollment_date"],
                    "source_type": SOURCE_TYPE,
                }
            )
            seq += 1
    return tokens


# ---- 7. transactions ----


def _assign_merchant_mcc(merchants: list[dict[str, Any]]) -> dict[str, tuple[str, float, str]]:
    return {m["merchant_id"]: mcc_crosswalk.assign_mcc(m) for m in merchants}


def generate_transactions(
    rng: random.Random,
    card_tokens: list[dict[str, Any]],
    cardholders: list[dict[str, Any]],
    programs: list[dict[str, Any]],
    merchants: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, tuple[str, float, str]]]:
    cardholder_by_id = {c["cardholder_id"]: c for c in cardholders}
    program_by_id = {p["program_id"]: p for p in programs}
    merchant_mcc = _assign_merchant_mcc(merchants)

    window_start = days_before_reference(TRANSACTION_WINDOW_DAYS)
    transactions = []
    for i in range(1, N_TRANSACTIONS + 1):
        token = rng.choice(card_tokens)
        cardholder = cardholder_by_id[token["cardholder_id"]]
        program = program_by_id[cardholder["enrolled_program_id"]]
        merchant = rng.choice(merchants)
        ts = _random_datetime_in_window(rng, window_start, REFERENCE_DATE)
        approved = rng.random() < APPROVAL_RATE
        mcc_code, mcc_confidence, _ = merchant_mcc[merchant["merchant_id"]]

        transactions.append(
            {
                "transaction_id": f"TXN-{i:07d}",
                "token_id": token["token_id"],
                "merchant_id": merchant["merchant_id"],
                "mcc_code": mcc_code,
                "mcc_confidence": mcc_confidence,
                "currency_code": program["currency_code"],
                "amount": _random_amount(rng),
                "transaction_timestamp": ts.isoformat(),
                "status": "APPROVED" if approved else "DECLINED",
                "decline_reason": None if approved else rng.choice(DECLINE_REASONS),
                "auth_code": "".join(rng.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(6)) if approved else None,
                "source_type": SOURCE_TYPE,
            }
        )
    return transactions, merchant_mcc


# ---- 8. transaction events ----


def generate_transaction_events(rng: random.Random, transactions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events = []
    seq = 1
    reversal_eligible_cutoff = days_before_reference(14)  # never let a reversal timestamp exceed REFERENCE_DATE

    for txn in transactions:
        ts = datetime.fromisoformat(txn["transaction_timestamp"])
        events.append(
            {
                "event_id": f"EVT-{seq:07d}",
                "transaction_id": txn["transaction_id"],
                "event_type": "AUTHORIZATION",
                "event_timestamp": ts.isoformat(),
                "event_status": txn["status"],
                "source_type": SOURCE_TYPE,
            }
        )
        seq += 1

        if txn["status"] == "APPROVED":
            capture_ts = ts + timedelta(minutes=rng.randint(1, 120))
            events.append(
                {
                    "event_id": f"EVT-{seq:07d}",
                    "transaction_id": txn["transaction_id"],
                    "event_type": "CAPTURE",
                    "event_timestamp": capture_ts.isoformat(),
                    "event_status": "CAPTURED",
                    "source_type": SOURCE_TYPE,
                }
            )
            seq += 1

            if ts.date() <= reversal_eligible_cutoff and rng.random() < REVERSAL_RATE:
                reversal_ts = capture_ts + timedelta(days=rng.randint(1, 13))
                events.append(
                    {
                        "event_id": f"EVT-{seq:07d}",
                        "transaction_id": txn["transaction_id"],
                        "event_type": "REVERSAL",
                        "event_timestamp": reversal_ts.isoformat(),
                        "event_status": "REVERSED",
                        "source_type": SOURCE_TYPE,
                    }
                )
                seq += 1

    return events


# ---- 9. settlements ----


def generate_settlements(rng: random.Random, transactions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    settlements = []
    seq = 1
    for txn in transactions:
        if txn["status"] != "APPROVED":
            continue
        txn_date = datetime.fromisoformat(txn["transaction_timestamp"]).date()
        settlement_date = txn_date + timedelta(days=rng.choice([1, 2]))
        settlement_amount = txn["amount"]
        fee_amount = round(settlement_amount * SETTLEMENT_FEE_RATE, 2)
        settlements.append(
            {
                "settlement_id": f"STL-{seq:07d}",
                "transaction_id": txn["transaction_id"],
                "settlement_batch_id": f"BATCH-{settlement_date.isoformat()}",
                "settlement_date": settlement_date.isoformat(),
                "settlement_amount": settlement_amount,
                "settlement_currency": txn["currency_code"],
                "fee_amount": fee_amount,
                "net_amount": round(settlement_amount - fee_amount, 2),
                "source_type": SOURCE_TYPE,
            }
        )
        seq += 1
    return settlements


# ---- 10. reconciliation ----


def generate_reconciliation(rng: random.Random, settlements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    exception_deltas = [-10.00, -5.00, -1.23, 2.50, 7.77]
    records = []
    for seq, settlement in enumerate(settlements, start=1):
        expected = settlement["settlement_amount"]
        is_exception = rng.random() < RECONCILIATION_EXCEPTION_RATE
        actual = round(expected + rng.choice(exception_deltas), 2) if is_exception else expected
        variance = round(actual - expected, 2)
        settlement_date = date.fromisoformat(settlement["settlement_date"])
        records.append(
            {
                "reconciliation_id": f"REC-{seq:07d}",
                "settlement_id": settlement["settlement_id"],
                "expected_amount": expected,
                "actual_amount": actual,
                "variance": variance,
                "match_status": "EXCEPTION" if is_exception else "MATCHED",
                "reconciled_date": (settlement_date + timedelta(days=rng.choice([0, 1, 2]))).isoformat(),
                "source_type": SOURCE_TYPE,
            }
        )
    return records


# ---- 11. rewards / CLO events ----


def generate_reward_events(
    transactions: list[dict[str, Any]],
    offers: list[dict[str, Any]],
    campaigns: list[dict[str, Any]],
    cardholders: list[dict[str, Any]],
    card_tokens: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """One row per (transaction, matched offer) — only for APPROVED transactions
    that actually match an active, in-scope offer. No match -> no row (see
    module docstring for the "eligible = has a row" interpretation)."""
    campaign_program = {c["campaign_id"]: c["program_id"] for c in campaigns}
    offers_by_program: dict[str, list[dict[str, Any]]] = {}
    for offer in offers:
        program_id = campaign_program[offer["campaign_id"]]
        offers_by_program.setdefault(program_id, []).append(offer)
    for program_id in offers_by_program:
        offers_by_program[program_id].sort(key=lambda o: o["offer_id"])  # deterministic "first match wins"

    token_by_id = {t["token_id"]: t for t in card_tokens}
    cardholder_by_id = {c["cardholder_id"]: c for c in cardholders}

    events = []
    seq = 1
    for txn in transactions:
        if txn["status"] != "APPROVED":
            continue
        token = token_by_id[txn["token_id"]]
        cardholder = cardholder_by_id[token["cardholder_id"]]
        candidate_offers = offers_by_program.get(cardholder["enrolled_program_id"], [])

        txn_date = date.fromisoformat(txn["transaction_timestamp"][:10])
        matched_offer = None
        for offer in candidate_offers:
            if not (date.fromisoformat(offer["valid_from"]) <= txn_date <= date.fromisoformat(offer["valid_to"])):
                continue
            if offer["eligible_mcc_code"] is not None and offer["eligible_mcc_code"] != txn["mcc_code"]:
                continue
            matched_offer = offer
            break

        if matched_offer is None:
            continue

        qualified = txn["amount"] >= matched_offer["min_transaction_amount"]
        if qualified:
            if matched_offer["offer_type"] == "CASHBACK_PCT":
                reward_amount = round(txn["amount"] * matched_offer["offer_value"], 2)
            else:  # FIXED_AMOUNT
                reward_amount = matched_offer["offer_value"]
        else:
            reward_amount = 0.0

        events.append(
            {
                "reward_id": f"RWD-{seq:07d}",
                "transaction_id": txn["transaction_id"],
                "offer_id": matched_offer["offer_id"],
                "qualification_status": "QUALIFIED" if qualified else "NOT_QUALIFIED",
                "reward_amount": reward_amount,
                "reward_currency": matched_offer["currency_code"],
                "event_timestamp": txn["transaction_timestamp"],
                "source_type": SOURCE_TYPE,
            }
        )
        seq += 1

    return events
