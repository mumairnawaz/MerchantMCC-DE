"""Orchestrates synthetic OLTP generation end to end: load real Silver
reference dimensions -> generate every table in dependency order -> validate
-> write Parquet. Deterministic given (seed, Silver snapshot) — see
src/synthetic/common.py for the reproducibility discipline.
"""

import random
from pathlib import Path
from typing import Any

from src.synthetic import generators, reference, schemas
from src.synthetic.common import SEED, SYNTHETIC_ROOT
from src.synthetic.validation import validate_all


def generate_all_tables(ref: dict[str, list[dict[str, Any]]], seed: int = SEED) -> dict[str, list[dict[str, Any]]]:
    """Pure generation — no filesystem writes, no validation. Split out from
    run() so tests can generate in-memory against a tiny synthetic reference
    fixture without touching data/silver/ or data/synthetic_oltp/."""
    rng = random.Random(seed)

    clients = generators.generate_clients(rng, ref["legal_entity"])
    programs = generators.generate_programs(rng, clients)
    campaigns = generators.generate_campaigns(rng, programs)
    offers = generators.generate_offers(rng, campaigns, programs, ref["mcc"])
    cardholders = generators.generate_cardholders(rng, programs, ref["country"])
    card_tokens = generators.generate_card_tokens(rng, cardholders, ref["card_issuer"])
    transactions, _merchant_mcc = generators.generate_transactions(rng, card_tokens, cardholders, programs, ref["merchant"])
    transaction_events = generators.generate_transaction_events(rng, transactions)
    settlements = generators.generate_settlements(rng, transactions)
    reconciliation = generators.generate_reconciliation(rng, settlements)
    reward_events = generators.generate_reward_events(transactions, offers, campaigns, cardholders, card_tokens)

    return {
        "clients": clients,
        "programs": programs,
        "campaigns": campaigns,
        "offers": offers,
        "cardholders": cardholders,
        "card_tokens": card_tokens,
        "transactions": transactions,
        "transaction_events": transaction_events,
        "settlements": settlements,
        "reconciliation": reconciliation,
        "reward_events": reward_events,
    }


def run(
    *,
    silver_root: Path | None = None,
    output_root: Path = SYNTHETIC_ROOT,
    seed: int = SEED,
    write_output: bool = True,
) -> dict[str, Any]:
    ref = reference.load_reference(silver_root) if silver_root is not None else reference.load_reference()
    tables = generate_all_tables(ref, seed=seed)

    validation_result = validate_all(tables, ref)

    written_paths = {}
    if write_output:
        from src.synthetic.common import write_table

        for name, records in tables.items():
            written_paths[name] = write_table(records, name, schemas.SCHEMAS[name], root=output_root)

    return {
        "tables": tables,
        "row_counts": validation_result["row_counts"],
        "gross_approved_transaction_amount": validation_result["gross_approved_transaction_amount"],
        "gross_settlement_amount": validation_result["gross_settlement_amount"],
        "reconciliation_expected_total": validation_result["reconciliation_expected_total"],
        "written_paths": written_paths,
        "seed": seed,
    }
