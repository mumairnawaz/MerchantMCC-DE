"""Data-quality validation and control totals for the generated synthetic OLTP
dataset (S9). The generator is expected to produce clean data by default — this
module PROVES that, raising RuntimeError on any violation, rather than merely
reporting it. Negative-test fixtures for the individual checks live in
tests/test_synthetic_validation.py, not in the primary generated dataset.

Reuses src.silver.common.reconcile_counts for the pure count-reconciliation
checks (transactions -> events, transactions -> settlements) — it's already
dataset-agnostic and exactly fits this need; no reason to reinvent it.
"""

from datetime import date, datetime
from typing import Any

from src.silver.common import reconcile_counts


class SyntheticDataQualityError(RuntimeError):
    pass


def _fail(message: str) -> None:
    raise SyntheticDataQualityError(f"synthetic DQ violation: {message}")


def _check_unique(records: list[dict[str, Any]], key_field: str, table_name: str) -> None:
    keys = [r[key_field] for r in records]
    if len(keys) != len(set(keys)):
        dupes = {k for k in keys if keys.count(k) > 1}
        _fail(f"{table_name}.{key_field} has duplicate values: {sorted(dupes)[:5]}")


def _check_fk(records: list[dict[str, Any]], fk_field: str, valid_values: set, table_name: str, *, allow_null: bool = False) -> None:
    for r in records:
        value = r.get(fk_field)
        if value is None:
            if allow_null:
                continue
            _fail(f"{table_name}.{fk_field} is NULL on {r}")
        if value not in valid_values:
            _fail(f"{table_name}.{fk_field}={value!r} not found in the referenced dimension")


def _check_positive(records: list[dict[str, Any]], field: str, table_name: str) -> None:
    for r in records:
        if not (r[field] > 0):
            _fail(f"{table_name}.{field}={r[field]!r} is not positive (record: {r})")


def _check_source_type(records: list[dict[str, Any]], table_name: str) -> None:
    for r in records:
        if r.get("source_type") != "synthetic":
            _fail(f"{table_name} row missing/incorrect source_type tag: {r}")


def validate_all(tables: dict[str, list[dict[str, Any]]], reference: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    clients = tables["clients"]
    programs = tables["programs"]
    campaigns = tables["campaigns"]
    offers = tables["offers"]
    cardholders = tables["cardholders"]
    card_tokens = tables["card_tokens"]
    transactions = tables["transactions"]
    transaction_events = tables["transaction_events"]
    settlements = tables["settlements"]
    reconciliation = tables["reconciliation"]
    reward_events = tables["reward_events"]

    real_leis = {r["lei"] for r in reference["legal_entity"]}
    real_mcc_codes = {r["mcc_code"] for r in reference["mcc"]}
    real_currency_codes = {r["currency_code"] for r in reference["iso_currency"]}
    real_country_alpha2 = {r["country_code_alpha2"] for r in reference["country"]}
    real_merchant_ids = {r["merchant_id"] for r in reference["merchant"]}
    real_bin_ranges = {r["bin_range"] for r in reference["card_issuer"]}

    # ---- synthetic tagging ----
    for name, records in tables.items():
        _check_source_type(records, name)

    # ---- PK uniqueness ----
    _check_unique(clients, "client_id", "clients")
    _check_unique(programs, "program_id", "programs")
    _check_unique(campaigns, "campaign_id", "campaigns")
    _check_unique(offers, "offer_id", "offers")
    _check_unique(cardholders, "cardholder_id", "cardholders")
    _check_unique(card_tokens, "token_id", "card_tokens")
    _check_unique(transactions, "transaction_id", "transactions")
    _check_unique(transaction_events, "event_id", "transaction_events")
    _check_unique(settlements, "settlement_id", "settlements")
    _check_unique(settlements, "transaction_id", "settlements")  # exactly one settlement per transaction
    _check_unique(reconciliation, "reconciliation_id", "reconciliation")
    _check_unique(reconciliation, "settlement_id", "reconciliation")  # exactly one reconciliation per settlement
    _check_unique(reward_events, "reward_id", "reward_events")

    # ---- FK / reference validity ----
    _check_fk(clients, "lei", real_leis, "clients")
    program_owner_ids = {c["client_id"] for c in clients if c["client_type"] == "PROGRAM_OWNER"}
    _check_fk(programs, "client_id", program_owner_ids, "programs")
    _check_fk(programs, "currency_code", real_currency_codes, "programs")
    program_ids = {p["program_id"] for p in programs}
    _check_fk(campaigns, "program_id", program_ids, "campaigns")
    campaign_ids = {c["campaign_id"] for c in campaigns}
    _check_fk(offers, "campaign_id", campaign_ids, "offers")
    _check_fk(offers, "eligible_mcc_code", real_mcc_codes, "offers", allow_null=True)
    _check_fk(offers, "currency_code", real_currency_codes, "offers")
    _check_fk(cardholders, "enrolled_program_id", program_ids, "cardholders")
    _check_fk(cardholders, "country_code", real_country_alpha2, "cardholders")
    cardholder_ids = {c["cardholder_id"] for c in cardholders}
    _check_fk(card_tokens, "cardholder_id", cardholder_ids, "card_tokens")
    _check_fk(card_tokens, "bin_range", real_bin_ranges, "card_tokens")
    token_ids = {t["token_id"] for t in card_tokens}
    _check_fk(transactions, "token_id", token_ids, "transactions")
    _check_fk(transactions, "merchant_id", real_merchant_ids, "transactions")
    _check_fk(transactions, "mcc_code", real_mcc_codes, "transactions")
    _check_fk(transactions, "currency_code", real_currency_codes, "transactions")
    transaction_ids = {t["transaction_id"] for t in transactions}
    _check_fk(transaction_events, "transaction_id", transaction_ids, "transaction_events")
    _check_fk(settlements, "transaction_id", transaction_ids, "settlements")
    settlement_ids = {s["settlement_id"] for s in settlements}
    _check_fk(reconciliation, "settlement_id", settlement_ids, "reconciliation")
    _check_fk(reward_events, "transaction_id", transaction_ids, "reward_events")
    offer_ids = {o["offer_id"] for o in offers}
    _check_fk(reward_events, "offer_id", offer_ids, "reward_events")

    # ---- positive/valid monetary values ----
    _check_positive(transactions, "amount", "transactions")
    _check_positive(offers, "min_transaction_amount", "offers")
    _check_positive(offers, "offer_value", "offers")
    _check_positive(settlements, "settlement_amount", "settlements")
    _check_positive(settlements, "net_amount", "settlements")
    for r in reward_events:
        if r["qualification_status"] == "QUALIFIED" and not (r["reward_amount"] > 0):
            _fail(f"reward_events: QUALIFIED row has non-positive reward_amount: {r}")
        if r["qualification_status"] == "NOT_QUALIFIED" and r["reward_amount"] != 0:
            _fail(f"reward_events: NOT_QUALIFIED row has nonzero reward_amount: {r}")

    # ---- transaction timestamp window ----
    window_start = date.fromisoformat(min(t["transaction_timestamp"][:10] for t in transactions))
    for t in transactions:
        ts = datetime.fromisoformat(t["transaction_timestamp"])
        if not (window_start <= ts.date()):
            _fail(f"transactions.transaction_timestamp={t['transaction_timestamp']} before the generation window")

    # ---- lifecycle ordering ----
    events_by_txn: dict[str, list[dict[str, Any]]] = {}
    for e in transaction_events:
        events_by_txn.setdefault(e["transaction_id"], []).append(e)

    for txn in transactions:
        evs = sorted(events_by_txn.get(txn["transaction_id"], []), key=lambda e: e["event_timestamp"])
        types = [e["event_type"] for e in evs]
        if types.count("AUTHORIZATION") != 1 or types[0] != "AUTHORIZATION":
            _fail(f"transaction {txn['transaction_id']} does not start with exactly one AUTHORIZATION event: {types}")
        if txn["status"] == "APPROVED":
            if types.count("CAPTURE") != 1:
                _fail(f"approved transaction {txn['transaction_id']} does not have exactly one CAPTURE event: {types}")
        else:
            if "CAPTURE" in types:
                _fail(f"declined transaction {txn['transaction_id']} has a CAPTURE event: {types}")
            if "REVERSAL" in types:
                _fail(f"declined transaction {txn['transaction_id']} has a REVERSAL event: {types}")
        if "REVERSAL" in types and "CAPTURE" not in types:
            _fail(f"transaction {txn['transaction_id']} has a REVERSAL without a prior CAPTURE: {types}")
        for a, b in zip(evs, evs[1:]):
            if b["event_timestamp"] < a["event_timestamp"]:
                _fail(f"transaction {txn['transaction_id']} events out of order: {types}")

    # ---- transaction_events composition (every event is one of the three known
    # types; catches a stray/malformed event_type that the per-row checks above
    # might not) ----
    auth_events = sum(1 for e in transaction_events if e["event_type"] == "AUTHORIZATION")
    capture_events = sum(1 for e in transaction_events if e["event_type"] == "CAPTURE")
    reversal_events = sum(1 for e in transaction_events if e["event_type"] == "REVERSAL")
    reconcile_counts(
        len(transaction_events),
        {"authorization": auth_events, "capture": capture_events, "reversal": reversal_events},
        label="synthetic transaction_events composition",
    )

    # ---- settlement consistency ----
    approved_count = sum(1 for t in transactions if t["status"] == "APPROVED")
    if auth_events != len(transactions):
        _fail(f"authorization event count {auth_events} != transaction count {len(transactions)}")
    if capture_events != approved_count:
        _fail(f"capture event count {capture_events} != approved transaction count {approved_count}")
    if len(settlements) != approved_count:
        _fail(f"settlement count {len(settlements)} != approved transaction count {approved_count}")

    txn_by_id = {t["transaction_id"]: t for t in transactions}
    for s in settlements:
        txn = txn_by_id[s["transaction_id"]]
        if s["settlement_amount"] != txn["amount"]:
            _fail(f"settlement {s['settlement_id']} amount {s['settlement_amount']} != transaction amount {txn['amount']}")
        if round(s["settlement_amount"] - s["fee_amount"], 2) != s["net_amount"]:
            _fail(f"settlement {s['settlement_id']}: net_amount does not equal settlement_amount - fee_amount")
        if date.fromisoformat(s["settlement_date"]) < datetime.fromisoformat(txn["transaction_timestamp"]).date():
            _fail(f"settlement {s['settlement_id']} settles before its own transaction")

    # ---- reconciliation arithmetic ----
    reconcile_counts(len(settlements), {"reconciled": len(reconciliation)}, label="synthetic reconciliation coverage")
    settlement_by_id = {s["settlement_id"]: s for s in settlements}
    for r in reconciliation:
        settlement = settlement_by_id[r["settlement_id"]]
        if r["expected_amount"] != settlement["settlement_amount"]:
            _fail(f"reconciliation {r['reconciliation_id']}: expected_amount != settlement.settlement_amount")
        if round(r["actual_amount"] - r["expected_amount"], 2) != r["variance"]:
            _fail(f"reconciliation {r['reconciliation_id']}: variance != actual_amount - expected_amount")
        if r["match_status"] == "MATCHED" and r["variance"] != 0:
            _fail(f"reconciliation {r['reconciliation_id']}: MATCHED but variance != 0")
        if r["match_status"] == "EXCEPTION" and r["variance"] == 0:
            _fail(f"reconciliation {r['reconciliation_id']}: EXCEPTION but variance == 0")
        if date.fromisoformat(r["reconciled_date"]) < date.fromisoformat(settlement["settlement_date"]):
            _fail(f"reconciliation {r['reconciliation_id']} reconciles before its own settlement")

    # ---- control totals ----
    gross_transaction_amount = round(sum(t["amount"] for t in transactions if t["status"] == "APPROVED"), 2)
    gross_settlement_amount = round(sum(s["settlement_amount"] for s in settlements), 2)
    if gross_transaction_amount != gross_settlement_amount:
        _fail(f"control total mismatch: gross approved transaction amount {gross_transaction_amount} != gross settlement amount {gross_settlement_amount}")

    expected_reconciliation_total = round(sum(r["expected_amount"] for r in reconciliation), 2)
    if expected_reconciliation_total != gross_settlement_amount:
        _fail("control total mismatch: reconciliation expected_amount total != gross settlement amount")

    return {
        "row_counts": {name: len(records) for name, records in tables.items()},
        "gross_approved_transaction_amount": gross_transaction_amount,
        "gross_settlement_amount": gross_settlement_amount,
        "reconciliation_expected_total": expected_reconciliation_total,
        "passed": True,
    }
