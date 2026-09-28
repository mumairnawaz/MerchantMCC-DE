"""Gold pipeline orchestrator (§ suggested structure). Full-rebuild strategy
(§ "Rebuild strategy" — matches the S3 "simple overwrite" precedent already
established for mcc/country/iso_currency/card_issuer): every run recomputes
every dimension and fact fresh from current Silver/CDC Silver state.
Idempotency (§25) is guaranteed by src.gold.keys' persisted, monotonic,
never-renumbered surrogate-key registry — NOT by incremental merge logic.

Order of operations, and why:
  1. Independent dimensions first (no FK dependency on another Gold dim).
  2. Dependent dimensions next, each consuming the natural_key->surrogate_key
     registry of whatever it depends on (client->legal_entity,
     program->client+currency, campaign->program, offer->campaign+mcc+
     currency, cardholder->program, card_token->cardholder+card_issuer).
  3. Facts, which consume the now-complete set of dimension registries.
  4. dim_date LAST — sized from the real date range actually present across
     every fact's date-bearing column (never a fabricated calendar; see
     dimensions.py module docstring and this phase's §18).
  5. Validation (surrogate/natural key uniqueness, fact grain, FK integrity).
  6. Reconciliation (row-count identities + the control-total identity).
  7. Materialize every dimension/fact as Parquet, then load into DuckDB
     (schema "main" of data/gold/gold.duckdb).

Mart ownership (S15): this module used to also create merchant_mart/
transaction_mart as raw SQL views (S14) — that responsibility now belongs to
the dbt project at dbt/ (models/marts/*.sql), which reads these same "main"
schema tables as dbt sources and writes its own staging/intermediate/marts
schemas into the same gold.duckdb file. This module no longer creates any
view — see docs/28-dbt-transformation-layer-design.md.
"""

from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

from src.gold import dimensions as dim
from src.gold import facts as fct
from src.gold import reconciliation as recon
from src.gold import validation as dq
from src.gold.config import CDC_SILVER_ROOT, DIMENSIONS_ROOT, DUCKDB_PATH, FACTS_ROOT
from src.gold.keys import load_registry
from src.silver.common import read_parquet, write_parquet


def _as_date(value: date | datetime) -> date:
    return value.date() if isinstance(value, datetime) else value


def _cdc_silver_count(dataset: str) -> int:
    path = CDC_SILVER_ROOT / dataset / "data.parquet"
    return len(read_parquet(path)) if path.exists() else 0


def _event_log_rows_and_delete_count(dataset: str) -> tuple[int, int]:
    d = CDC_SILVER_ROOT / dataset
    if not d.exists():
        return 0, 0
    total = 0
    deletes = 0
    for f in sorted(d.glob("run_*.parquet")):
        rows = read_parquet(f)
        total += len(rows)
        deletes += sum(1 for r in rows if r["operation"] == "d")
    return total, deletes


def _event_log_dates(dataset: str) -> list[date]:
    """Real event_timestamp dates for an event-log dataset, excluding delete
    markers (which carry an epoch 1970-01-01 placeholder — see facts.py)."""
    d = CDC_SILVER_ROOT / dataset
    if not d.exists():
        return []
    dates: list[date] = []
    for f in sorted(d.glob("run_*.parquet")):
        for r in read_parquet(f):
            if r["operation"] != "d":
                dates.append(_as_date(r["event_timestamp"]))
    return dates


def run() -> dict[str, Any]:
    # ---- 1. Independent dimensions ----
    dim_mcc = dim.build_dim_mcc()
    mcc_registry = load_registry("dim_mcc")

    dim_country = dim.build_dim_country()

    dim_currency = dim.build_dim_currency()
    currency_registry = load_registry("dim_currency")

    dim_card_issuer = dim.build_dim_card_issuer()
    card_issuer_registry = load_registry("dim_card_issuer")

    dim_legal_entity = dim.build_dim_legal_entity()
    legal_entity_registry = load_registry("dim_legal_entity")

    dim_merchant = dim.build_dim_merchant()
    merchant_registry = load_registry("dim_merchant")

    # ---- 2. Dependent dimensions ----
    dim_client = dim.build_dim_client(legal_entity_registry)
    client_registry = load_registry("dim_client")

    dim_program = dim.build_dim_program(client_registry, currency_registry)
    program_registry = load_registry("dim_program")

    dim_campaign = dim.build_dim_campaign(program_registry)
    campaign_registry = load_registry("dim_campaign")

    dim_offer = dim.build_dim_offer(campaign_registry, mcc_registry, currency_registry)
    offer_registry = load_registry("dim_offer")

    dim_cardholder = dim.build_dim_cardholder(program_registry)
    cardholder_registry = load_registry("dim_cardholder")

    dim_card_token = dim.build_dim_card_token(cardholder_registry, card_issuer_registry)
    card_token_registry = load_registry("dim_card_token")

    # ---- 3. Facts ----
    fact_transactions = fct.build_fact_transactions(
        merchant_registry, mcc_registry, currency_registry, card_token_registry, cardholder_registry, program_registry, client_registry
    )
    fact_transaction_events = fct.build_fact_transaction_events()
    fact_settlements = fct.build_fact_settlements(currency_registry)
    fact_reconciliation = fct.build_fact_reconciliation()
    fact_rewards = fct.build_fact_rewards(offer_registry, currency_registry)

    # ---- 4. dim_date — sized from real dates actually present in the facts ----
    settlement_source = read_parquet(CDC_SILVER_ROOT / "silver_cdc_settlement" / "data.parquet")
    reconciliation_source = read_parquet(CDC_SILVER_ROOT / "silver_cdc_reconciliation" / "data.parquet")
    reward_event_dates = _event_log_dates("silver_cdc_reward_event")

    all_real_dates: list[date] = (
        [_as_date(r["transaction_timestamp"]) for r in fact_transactions]
        + [_as_date(r["event_timestamp"]) for r in fact_transaction_events]
        + reward_event_dates
        + [r["settlement_date"] for r in settlement_source]
        + [r["reconciled_date"] for r in reconciliation_source]
    )
    dim_date_rows = dim.build_dim_date(min(all_real_dates), max(all_real_dates))
    valid_date_keys = {r["date_key"] for r in dim_date_rows}

    # ---- 5. Validation ----
    dq.validate_dimensions(
        {
            "dim_mcc": (dim_mcc, "mcc_key", "mcc_code"),
            "dim_country": (dim_country, "country_key", "country_code_alpha3"),
            "dim_currency": (dim_currency, "currency_key", "currency_code"),
            "dim_card_issuer": (dim_card_issuer, "card_issuer_key", "bin_range"),
            "dim_legal_entity": (dim_legal_entity, "legal_entity_key", "lei"),
            "dim_merchant": (dim_merchant, "merchant_key", "merchant_id"),
            "dim_client": (dim_client, "client_key", "client_id"),
            "dim_program": (dim_program, "program_key", "program_id"),
            "dim_campaign": (dim_campaign, "campaign_key", "campaign_id"),
            "dim_offer": (dim_offer, "offer_key", "offer_id"),
            "dim_cardholder": (dim_cardholder, "cardholder_key", "cardholder_id"),
            "dim_card_token": (dim_card_token, "card_token_key", "token_id"),
            "dim_date": (dim_date_rows, "date_key", "date_key"),
        }
    )

    fk_checks = [
        ("fact_transactions", "merchant_key", "dim_merchant", {r["merchant_key"] for r in dim_merchant}),
        ("fact_transactions", "mcc_key", "dim_mcc", {r["mcc_key"] for r in dim_mcc}),
        ("fact_transactions", "currency_key", "dim_currency", {r["currency_key"] for r in dim_currency}),
        ("fact_transactions", "card_token_key", "dim_card_token", {r["card_token_key"] for r in dim_card_token}),
        ("fact_transactions", "cardholder_key", "dim_cardholder", {r["cardholder_key"] for r in dim_cardholder}),
        ("fact_transactions", "program_key", "dim_program", {r["program_key"] for r in dim_program}),
        ("fact_transactions", "client_key", "dim_client", {r["client_key"] for r in dim_client}),
        ("fact_transactions", "date_key", "dim_date", valid_date_keys),
        ("fact_settlements", "currency_key", "dim_currency", {r["currency_key"] for r in dim_currency}),
        ("fact_settlements", "date_key", "dim_date", valid_date_keys),
        ("fact_reconciliation", "date_key", "dim_date", valid_date_keys),
        ("fact_rewards", "offer_key", "dim_offer", {r["offer_key"] for r in dim_offer}),
        ("fact_rewards", "currency_key", "dim_currency", {r["currency_key"] for r in dim_currency}),
        ("fact_rewards", "date_key", "dim_date", valid_date_keys),
        ("fact_transaction_events", "date_key", "dim_date", valid_date_keys),
    ]
    dq.validate_facts(
        {
            "fact_transactions": (fact_transactions, ("transaction_id",)),
            "fact_transaction_events": (fact_transaction_events, ("event_id", "kafka_offset")),
            "fact_settlements": (fact_settlements, ("settlement_id",)),
            "fact_reconciliation": (fact_reconciliation, ("reconciliation_id",)),
            "fact_rewards": (fact_rewards, ("reward_id", "kafka_offset")),
        },
        fk_checks,
    )

    # ---- 6. Reconciliation ----
    txn_events_total, txn_events_deletes = _event_log_rows_and_delete_count("silver_cdc_transaction_event")
    reward_total, reward_deletes = _event_log_rows_and_delete_count("silver_cdc_reward_event")

    reconciliation_results = {
        "fact_transactions": recon.reconcile_fact_transactions(_cdc_silver_count("silver_cdc_transaction"), fact_transactions),
        "fact_settlements": recon.reconcile_fact_settlements(_cdc_silver_count("silver_cdc_settlement"), fact_settlements),
        "fact_reconciliation": recon.reconcile_fact_reconciliation(_cdc_silver_count("silver_cdc_reconciliation"), fact_reconciliation),
        "fact_transaction_events": recon.reconcile_event_log_fact(txn_events_total, fact_transaction_events, txn_events_deletes, label="fact_transaction_events"),
        "fact_rewards": recon.reconcile_event_log_fact(reward_total, fact_rewards, reward_deletes, label="fact_rewards"),
    }
    control_totals = recon.compute_control_totals(fact_transactions, fact_settlements, fact_reconciliation)
    recon.verify_control_total_identity(control_totals)

    # ---- 7. Materialize: Parquet + DuckDB ----
    dimension_outputs = {
        "dim_date": (dim_date_rows, dim.DATE_DIM_SCHEMA),
        "dim_merchant": (dim_merchant, dim.DIM_MERCHANT_SCHEMA),
        "dim_mcc": (dim_mcc, dim.DIM_MCC_SCHEMA),
        "dim_country": (dim_country, dim.DIM_COUNTRY_SCHEMA),
        "dim_currency": (dim_currency, dim.DIM_CURRENCY_SCHEMA),
        "dim_card_issuer": (dim_card_issuer, dim.DIM_CARD_ISSUER_SCHEMA),
        "dim_legal_entity": (dim_legal_entity, dim.DIM_LEGAL_ENTITY_SCHEMA),
        "dim_client": (dim_client, dim.DIM_CLIENT_SCHEMA),
        "dim_program": (dim_program, dim.DIM_PROGRAM_SCHEMA),
        "dim_campaign": (dim_campaign, dim.DIM_CAMPAIGN_SCHEMA),
        "dim_offer": (dim_offer, dim.DIM_OFFER_SCHEMA),
        "dim_cardholder": (dim_cardholder, dim.DIM_CARDHOLDER_SCHEMA),
        "dim_card_token": (dim_card_token, dim.DIM_CARD_TOKEN_SCHEMA),
    }
    fact_outputs = {
        "fact_transactions": (fact_transactions, fct.FACT_TRANSACTIONS_SCHEMA),
        "fact_transaction_events": (fact_transaction_events, fct.FACT_TRANSACTION_EVENTS_SCHEMA),
        "fact_settlements": (fact_settlements, fct.FACT_SETTLEMENTS_SCHEMA),
        "fact_reconciliation": (fact_reconciliation, fct.FACT_RECONCILIATION_SCHEMA),
        "fact_rewards": (fact_rewards, fct.FACT_REWARDS_SCHEMA),
    }

    written_paths: dict[str, Path] = {}
    for name, (rows, schema) in dimension_outputs.items():
        written_paths[name] = write_parquet(rows, DIMENSIONS_ROOT / f"{name}.parquet", schema=schema)
    for name, (rows, schema) in fact_outputs.items():
        written_paths[name] = write_parquet(rows, FACTS_ROOT / f"{name}.parquet", schema=schema)

    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH))
    try:
        for name in list(dimension_outputs) + list(fact_outputs):
            path = written_paths[name].as_posix()
            con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM read_parquet('{path}')")
    finally:
        con.close()

    return {
        "dimension_row_counts": {name: len(rows) for name, (rows, _) in dimension_outputs.items()},
        "fact_row_counts": {name: len(rows) for name, (rows, _) in fact_outputs.items()},
        "reconciliation": reconciliation_results,
        "control_totals": {k: str(v) for k, v in control_totals.items()},
        "written_paths": {k: str(v) for k, v in written_paths.items()},
        "duckdb_path": str(DUCKDB_PATH),
    }
