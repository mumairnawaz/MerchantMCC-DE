"""Run one or all Phase 3 batch reference-data ingestion jobs."""

import argparse
import sys

from src.ingestion import card_issuer, country, currency, gleif, iso_currency, mcc, merchant_osm

JOBS = {
    "mcc": mcc.run,
    "country": country.run,
    "currency": currency.run,
    "merchant_osm": merchant_osm.run,
    "iso_currency": iso_currency.run,
    "card_issuer": card_issuer.run,
    "gleif": gleif.run,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MerchantMCC DE batch reference-data ingestion")
    parser.add_argument("source", nargs="?", choices=[*JOBS.keys(), "all"], default="all")
    args = parser.parse_args()

    targets = JOBS.keys() if args.source == "all" else [args.source]
    for name in targets:
        print(f"Running ingestion: {name}")
        run_dir = JOBS[name]()
        print(f"  -> {run_dir}")


if __name__ == "__main__":
    sys.exit(main())
